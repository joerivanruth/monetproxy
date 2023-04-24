mod formatter;

use anyhow::{anyhow, Result as AResult};
use argsplitter::{ArgError, ArgSplitter};
use formatter::{dump_binary, dump_text, Formatter, TextFormatter};
use itertools::Itertools;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::thread;

const USAGE: &str = "\
Usage:  monetproxy LISTEN_ADDR DEST_ADDR
        (ADDR is PORT or HOST:PORT)
";

const BLOCKSIZE: usize = 8190;

fn main() -> ExitCode {
    argsplitter::main_support::report_errors(USAGE, mymain())
}

fn mymain() -> AResult<()> {
    let mut args = ArgSplitter::from_env();
    while let Some(flag) = args.flag()? {
        match flag {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            _ => Err(ArgError::unknown_flag(flag))?,
        }
    }
    let listen_addrs = parse_addr(&args.stashed("LISTEN_ADDR")?)?;
    let dest_addrs = parse_addr(&args.stashed("DEST_ADDR")?)?;
    args.no_more_stashed()?;

    if dest_addrs.is_empty() {
        return Err(anyhow!("DEST_ADDR does not resolve to any addresses"));
    }
    eprintln!("Forwarding to first of {}", dest_addrs.iter().format(", "));

    let formatter = TextFormatter::new(io::stdout());
    let formatter = Arc::new(Mutex::new(formatter));

    for addr in listen_addrs {
        let da = dest_addrs.clone();
        let f = Arc::clone(&formatter);
        spawn_anyhow(addr, move || listen(addr, f, da));
    }

    loop {
        thread::park()
    }
}

fn parse_addr(s: &str) -> AResult<Vec<SocketAddr>> {
    if let Ok(x) = s.to_socket_addrs() {
        return Ok(x.collect_vec());
    }

    if let Ok(n) = s.parse::<u16>() {
        return Ok(("localhost", n).to_socket_addrs()?.collect_vec());
    }

    Err(anyhow!("invalid address: {s}"))
}

fn listen(
    addr: SocketAddr,
    formatter: Arc<Mutex<TextFormatter>>,
    dest_addrs: Vec<SocketAddr>,
) -> AResult<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("Listening on {addr}");
    loop {
        let (conn, remote) = listener.accept()?;
        eprintln!("Accepted connection from {remote} on {addr}");
        let da = dest_addrs.clone();
        let f = Arc::clone(&formatter);
        spawn_anyhow(remote, move || handle(conn, f, &da));
    }
}

fn handle(
    client: std::net::TcpStream,
    formatter: Arc<Mutex<TextFormatter>>,
    dest_addrs: &[SocketAddr],
) -> AResult<()> {
    let cur = thread::current();
    let name = cur.name().unwrap_or("<unnamed>");
    let (server, addr) = connect(dest_addrs)?;
    eprintln!("Connected {name} to {addr}");

    let client_inspector = Inspector::new("CLIENT", Arc::clone(&formatter));
    let to_client = client.try_clone()?;
    let from_client = client;

    let server_inspector = Inspector::new("SERVER", Arc::clone(&formatter));
    let to_server = server.try_clone()?;
    let from_server = server;

    spawn_anyhow(format!("downstream-{name}"), || {
        pump(server_inspector, from_server, to_client)
    });
    spawn_anyhow(format!("upstream-{name}"), || {
        pump(client_inspector, from_client, to_server)
    });

    Ok(())
}

fn connect(dest_addrs: &[SocketAddr]) -> AResult<(TcpStream, SocketAddr)> {
    let mut addrs = dest_addrs.iter().copied();

    let first = addrs.next().expect("dest_addrs cannot be empty");
    let first_error = match TcpStream::connect(first) {
        Ok(conn) => return Ok((conn, first)),
        Err(e) => e,
    };

    for a in addrs {
        if let Ok(conn) = TcpStream::connect(a) {
            return Ok((conn, a));
        }
    }

    Err(first_error.into())
}

fn pump(
    mut inspector: Inspector<TextFormatter>,
    mut r: TcpStream,
    mut w: TcpStream,
) -> AResult<()> {
    let mut buffer = [0u8; BLOCKSIZE];

    loop {
        let nread = match r.read(&mut buffer) {
            Err(e) => {
                let result = inspector.on_error("reading from the server", &e);
                let _ = w.shutdown(Shutdown::Write);
                return Ok(result?);
            }
            Ok(0) => {
                inspector.on_close()?;
                let _ = w.shutdown(Shutdown::Write);
                return Ok(());
            }
            Ok(n) => n,
        };

        inspector.on_data(&buffer[..nread])?;

        if let Err(e) = w.write_all(&buffer[0..nread]) {
            inspector.on_error("writing to the client", &e)?;
            let _ = r.shutdown(Shutdown::Read);
            return Err(e.into());
        }
    }
}

fn spawn_anyhow<N: fmt::Display>(
    name: N,
    f: impl FnOnce() -> AResult<()> + Send + 'static,
) -> thread::JoinHandle<()> {
    let closure = || {
        if let Err(e) = f() {
            let cur = thread::current();
            let name = cur.name().unwrap_or("<unnamed>");
            println!("Thread '{name}' failed:");
            println!("{e:#}");
        }
    };
    thread::Builder::new()
        .name(name.to_string())
        .spawn(closure)
        .unwrap()
}

struct Inspector<F> {
    name: String,
    formatter: Arc<Mutex<F>>,
    buffer: Vec<u8>,
    goal: usize,
    last_block: bool,
}

impl<F: Formatter> Inspector<F> {
    fn new(name: impl fmt::Display, formatter: Arc<Mutex<F>>) -> Inspector<F> {
        let name = name.to_string();
        Inspector {
            name,
            formatter,
            buffer: vec![],
            goal: 2,
            last_block: false,
        }
    }

    fn on_data(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            assert!(self.buffer.len() < self.goal);
            let to_read = self.goal - self.buffer.len();
            let n = data.len().min(to_read);
            self.buffer.extend_from_slice(&data[..n]);
            data = &data[n..];

            if self.buffer.len() == self.goal {
                self.goal_reached()?;
                assert_ne!(self.buffer.len(), self.goal);
            }
        }

        Ok(())
    }

    fn goal_reached(&mut self) -> io::Result<()> {
        assert!(self.goal >= 2);
        if self.goal == 2 {
            let header: [u8; 2] = self.buffer[0..2].try_into().unwrap();
            let header = u16::from_le_bytes(header);
            let last = (header & 1) != 0;
            let size = (header / 2) as usize;
            self.goal += size;
            self.last_block = last;
            if size > 0 {
                return Ok(());
            }
        }

        self.on_message(&self.buffer[2..])?;
        self.buffer.clear();
        self.goal = 2;
        self.last_block = false;

        Ok(())
    }

    #[allow(unused_assignments)] // bug in rust-analyzer
    fn on_close(&mut self) -> io::Result<()> {
        let n = self.buffer.len();

        let mut buf = None;
        let msg = if n == 0 {
            "closed the connection"
        } else if n == 1 {
            "eof on incomplete header: 1/2"
        } else {
            let have = n - 2;
            let size = self.goal - 2;
            buf = Some(format!("eof on incomplete body: {have}/{size}"));
            buf.as_ref().unwrap()
        };

        let mut f = self.formatter.lock().unwrap();
        f.message(&self.name, msg)
    }

    fn on_error(&mut self, kind: &str, err: &io::Error) -> io::Result<()> {
        let msg = format!("encountered an error {kind}: {err}");
        let mut f = self.formatter.lock().unwrap();
        f.message(&self.name, &msg)
    }

    fn on_message(&self, data: &[u8]) -> io::Result<()> {
        let n = data.len();
        let is_text = if let Ok(text) = from_utf8(data) {
            text.chars()
                .filter(|&c| c.is_control() && c != '\n' && c != '\t')
                .count()
                == 0
        } else {
            false
        };

        let msg = if is_text {
            if data.is_empty() || data.ends_with(b"\n") {
                format!("text, {n} bytes")
            } else {
                format!("text, {n} bytes, no trailing newline!")
            }
        } else {
            format!("binary, {n} bytes")
        };

        let mut f = self.formatter.lock().unwrap();
        f.start_block(&self.name, &msg)?;
        if is_text {
            dump_text(&mut *f, data)?;
        } else {
            dump_binary(&mut *f, data)?;
        }
        f.end_block()?;

        Ok(())
    }
}
