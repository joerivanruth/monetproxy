use itertools::Itertools;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::{fmt, io};

use crate::formatter::{Formatter, Side};

pub const BLOCKSIZE: usize = 8190;

pub trait Inspector: Send {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()>;
    fn on_close(&mut self) -> io::Result<()>;
    fn on_error(&mut self, kind: &str, err: &io::Error) -> io::Result<()>;
}

pub fn parse_addr(s: &str) -> io::Result<Vec<SocketAddr>> {
    if let Ok(x) = s.to_socket_addrs() {
        return Ok(x.collect_vec());
    }

    if let Ok(n) = s.parse::<u16>() {
        return Ok(("localhost", n).to_socket_addrs()?.collect_vec());
    }

    let kind = io::ErrorKind::NotFound;
    let err = io::Error::new(kind, format!("invalid address: {s}"));
    Err(err)
}

pub fn spawn_listener<O, I, F>(
    addr: SocketAddr,
    dest_addrs: Vec<SocketAddr>,
    formatter: Arc<Mutex<O>>,
    make_inspector: F,
) -> JoinHandle<()>
where
    O: Formatter + Send + 'static,
    I: Inspector + Send + 'static,
    F: FnMut(Side, Arc<Mutex<O>>) -> I + Send + Sync + 'static,
{
    spawn_worker(addr, move || {
        listen(addr, formatter, make_inspector, dest_addrs)
    })
}

fn listen<O, I, F>(
    addr: SocketAddr,
    formatter: Arc<Mutex<O>>,
    mut make_inspector: F,
    dest_addrs: Vec<SocketAddr>,
) -> io::Result<()>
where
    O: Formatter,
    I: Inspector + Send + 'static,
    F: FnMut(Side, Arc<Mutex<O>>) -> I + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr)?;
    eprintln!("Listening on {addr}");
    loop {
        let (conn, remote) = listener.accept()?;

        let da = dest_addrs.clone();
        let (server, addr) = connect(&da)?;
        formatter.lock().unwrap().connected(&addr, &remote)?;

        let inspect_client = make_inspector(Side::Client, Arc::clone(&formatter));
        let to_client = conn.try_clone()?;
        let from_client = conn;

        let inspect_server = make_inspector(Side::Server, Arc::clone(&formatter));
        let to_server = server.try_clone()?;
        let from_server = server;

        spawn_worker(format!("downstream-{remote}"), || {
            pump(inspect_server, from_server, to_client)
        });
        spawn_worker(format!("upstream-{remote}"), || {
            pump(inspect_client, from_client, to_server)
        });
    }
}

fn connect(dest_addrs: &[SocketAddr]) -> io::Result<(TcpStream, SocketAddr)> {
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

    Err(first_error)
}

fn pump(mut inspector: impl Inspector, mut r: TcpStream, mut w: TcpStream) -> io::Result<()> {
    let mut buffer = [0u8; BLOCKSIZE];

    loop {
        let nread = match r.read(&mut buffer) {
            Err(e) => {
                let result = inspector.on_error("reading from the server", &e);
                let _ = w.shutdown(Shutdown::Write);
                return result;
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
            return Err(e);
        }
    }
}

fn spawn_worker<N: fmt::Display>(
    name: N,
    f: impl FnOnce() -> io::Result<()> + Send + 'static,
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
