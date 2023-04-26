use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::{fmt, io};

use crate::formatter::{Formatter, Side};
use crate::network::{Address, Incoming, Outgoing};

pub const BLOCKSIZE: usize = 8190;

pub trait Observer: Send {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()>;
    fn on_close(&mut self) -> io::Result<()>;
    fn on_error(&mut self, while_writing: bool, err: &io::Error) -> io::Result<()>;
    fn on_unix0(&mut self, data: &[u8], message: Option<&str>) -> io::Result<()>;
}

pub fn spawn_listener<O, I, F>(
    addr: Address,
    forward_to: Address,
    formatter: Arc<Mutex<O>>,
    make_inspector: F,
) -> JoinHandle<()>
where
    O: Formatter + Send + 'static,
    I: Observer + Send + 'static,
    F: FnMut(Side, Arc<Mutex<O>>) -> I + Send + Sync + 'static,
{
    spawn_worker(addr.to_string(), move || {
        listen(addr, formatter, make_inspector, forward_to)
    })
}

fn listen<O, I, F>(
    addr: Address,
    formatter: Arc<Mutex<O>>,
    mut make_inspector: F,
    forward_to: Address,
) -> io::Result<()>
where
    O: Formatter,
    I: Observer + Send + 'static,
    F: FnMut(Side, Arc<Mutex<O>>) -> I + Send + Sync + 'static,
{
    let mut accepter = addr.listen()?;
    eprintln!("Listening on {addr}");
    loop {
        let (mut from_client, to_client, client_address) = accepter()?;

        let (from_server, mut to_server, server_address) = connect(&forward_to)?;
        formatter
            .lock()
            .unwrap()
            .connected(&addr, &server_address)?;

        let mut inspect_client = make_inspector(Side::Client, Arc::clone(&formatter));
        let inspect_server = make_inspector(Side::Server, Arc::clone(&formatter));

        spawn_worker(format!("downstream-{client_address}"), || {
            pump(inspect_server, from_server, to_client)
        });
        spawn_worker(format!("upstream-{client_address}"), move || {
            adjust_unix(&mut inspect_client, &mut from_client, &mut to_server)?;
            pump(inspect_client, from_client, to_server)
        });
    }
}

fn connect(addr: &Address) -> io::Result<(Incoming, Outgoing, Address)> {
    if let Some(Address::Unix(path)) = addr.to_unix() {
        if let Ok(tuple) = connect_unix(path) {
            return Ok(tuple);
        }
    }

    if let Some(Address::Inet(a)) = addr.to_inet() {
        return connect_inet(a);
    }

    let kind: io::ErrorKind = io::ErrorKind::ConnectionRefused;
    Err(io::Error::new(kind, format!("can't connect to {addr}")))
}

fn connect_inet<A: ToSocketAddrs>(addr: A) -> io::Result<(Incoming, Outgoing, Address)> {
    let conn1 = TcpStream::connect(addr)?;
    let conn2 = conn1.try_clone()?;
    let peer = conn1.peer_addr()?;
    Ok((
        Incoming::Inet(conn1),
        Outgoing::Inet(conn2),
        Address::Inet(peer.to_string()),
    ))
}

fn connect_unix(p: impl AsRef<Path>) -> io::Result<(Incoming, Outgoing, Address)> {
    let p = p.as_ref();
    let conn1 = UnixStream::connect(p)?;
    let conn2 = conn1.try_clone()?;
    let peer = Address::Unix(p.into());

    Ok((Incoming::Unix(conn1), Outgoing::Unix(conn2), peer))
}

fn adjust_unix(observer: &mut dyn Observer, r: &mut Incoming, w: &mut Outgoing) -> io::Result<()> {
    remove_unix0(r)?;
    match (&r, &w) {
        (Incoming::Inet(_), Outgoing::Inet(_)) => {}
        (Incoming::Inet(_), Outgoing::Unix(_)) => observer.on_unix0(
            b"",
            Some("proxy inserting leading '0' to adjust inet->unix"),
        )?,
        (Incoming::Unix(_), Outgoing::Inet(_)) => observer.on_unix0(
            b"0",
            Some("proxy eliminated leading '0' to adjust unix->inet"),
        )?,
        (Incoming::Unix(_), Outgoing::Unix(_)) => observer.on_unix0(b"0", None)?,
    }
    insert_unix0(w)
}

fn remove_unix0(r: &mut Incoming) -> io::Result<()> {
    let Incoming::Unix(ref mut r) = r else { return Ok(()) };

    let mut buffer = [0u8];
    r.read_exact(&mut buffer)?;
    if buffer[0] == b'0' {
        Ok(())
    } else {
        let kind = io::ErrorKind::InvalidData;
        let msg = format!(
            "expected first character from client unix domain socket to be 0x30 ('0'), got 0x{x:02x}",
            x = buffer[0]
        );
        Err(io::Error::new(kind, msg))
    }
}

fn insert_unix0(w: &mut Outgoing) -> io::Result<()> {
    if let Outgoing::Unix(ref mut w) = w {
        w.write_all(b"0")?;
    }
    Ok(())
}

fn pump(mut inspector: impl Observer, mut r: Incoming, mut w: Outgoing) -> io::Result<()> {
    let mut buffer = [0u8; BLOCKSIZE];

    loop {
        let nread = match r.read(&mut buffer) {
            Err(e) => {
                let result = inspector.on_error(false, &e);
                let _ = w.shutdown();
                return result;
            }
            Ok(0) => {
                inspector.on_close()?;
                let _ = w.shutdown();
                return Ok(());
            }
            Ok(n) => n,
        };

        inspector.on_data(&buffer[..nread])?;

        if let Err(e) = w.write_all(&buffer[0..nread]) {
            inspector.on_error(true, &e)?;
            let _ = r.shutdown();
            return Ok(());
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
            println!("[{k:?}] {e:#}", k = e.kind());
        }
    };
    thread::Builder::new()
        .name(name.to_string())
        .spawn(closure)
        .unwrap()
}
