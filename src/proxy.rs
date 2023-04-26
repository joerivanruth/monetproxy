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
    fn on_error(&mut self, kind: &str, err: &io::Error) -> io::Result<()>;
    fn on_adjustment(&mut self, message: &str) -> io::Result<()>;
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

        use Address::*;
        let mut to_insert = &b""[..];
        let mut to_remove = &b""[..];
        match (&client_address, &server_address) {
            (Inet(_), Unix(_)) => {
                to_insert = &b"0"[..];
            }
            (Unix(_), Inet(_)) => {
                to_remove = &b"0"[..];
            }
            _ => {}
        };

        let mut inspect_client = make_inspector(Side::Client, Arc::clone(&formatter));
        let inspect_server = make_inspector(Side::Server, Arc::clone(&formatter));

        spawn_worker(format!("downstream-{client_address}"), || {
            pump(inspect_server, from_server, to_client)
        });
        spawn_worker(format!("upstream-{client_address}"), move || {
            if !to_remove.is_empty() {
                assert_eq!(to_remove, &[b'0']);
                let mut buffer = [0u8; 1];
                from_client.read_exact(&mut buffer)?;
                if buffer[0] == b'0' {
                    inspect_client.on_adjustment("adjust unix->inet socket: remove leading '0'")?;
                } else {
                    let msg = format!(
                        "expected first byte on unix socket to be '0', got {c:?}",
                        c = buffer[0]
                    );
                    let kind = io::ErrorKind::InvalidData;
                    return Err(io::Error::new(kind, msg));
                }
            }
            if !to_insert.is_empty() {
                inspect_client.on_adjustment("adjust inet->unix socket: insert '0'")?;
                to_server.write_all(to_insert)?;
                to_server.flush()?;
            }
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

fn pump(mut inspector: impl Observer, mut r: Incoming, mut w: Outgoing) -> io::Result<()> {
    let mut buffer = [0u8; BLOCKSIZE];

    loop {
        let nread = match r.read(&mut buffer) {
            Err(e) => {
                let result = inspector.on_error("reading from the server", &e);
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
            inspector.on_error("writing to the client", &e)?;
            let _ = r.shutdown();
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
            println!("[{k:?}] {e:#}", k = e.kind());
        }
    };
    thread::Builder::new()
        .name(name.to_string())
        .spawn(closure)
        .unwrap()
}
