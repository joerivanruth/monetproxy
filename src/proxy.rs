use std::borrow::Cow;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{self, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::{fmt, fs, io};

use crate::formatter::{Formatter, Side};

pub const BLOCKSIZE: usize = 8190;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    Inet(String),
    Unix(PathBuf),
    PortOnly(u16),
}

type Accepter = dyn FnMut() -> io::Result<(Incoming, Outgoing, Address)>;

impl Address {
    pub fn parse(s: impl AsRef<OsStr>) -> io::Result<Address> {
        let s = s.as_ref();

        let lossy = OsStr::to_string_lossy(s);
        if lossy.contains(['/', path::MAIN_SEPARATOR]) {
            return Ok(Address::Unix(s.into()));
        }

        if let Cow::Borrowed(t) = lossy {
            // It was valid UTF-8

            if t.to_socket_addrs().is_ok() {
                return Ok(Address::Inet(t.to_string()));
            }

            if let Ok(n) = t.parse() {
                return Ok(Address::PortOnly(n));
            }
        }

        let kind: io::ErrorKind = io::ErrorKind::NotFound;
        Err(io::Error::new(kind, format!("invalid address: {s:?}")))
    }

    pub fn to_inet(&self) -> Option<Address> {
        match self {
            Address::Inet(_) => Some(self.clone()),
            Address::Unix(_) => None,
            Address::PortOnly(n) => Some(Address::Inet(format!("localhost:{n}"))),
        }
    }

    pub fn to_unix(&self) -> Option<Address> {
        match self {
            Address::Inet(_) => None,
            Address::Unix(_) => Some(self.clone()),
            Address::PortOnly(n) => Some(Address::Unix(format!("/tmp/.s.monetdb.{n}").into())),
        }
    }

    pub fn expand(&self) -> impl Iterator<Item = Address> {
        self.to_unix().into_iter().chain(self.to_inet())
    }

    fn listen(&self) -> io::Result<Box<Accepter>> {
        match self {
            Address::PortOnly(_) => panic!("cannot invoke listen() on PortOnly"),
            Address::Inet(a) => {
                let listener = TcpListener::bind(a)?;
                Ok(Box::new(move || {
                    let (conn, client) = listener.accept()?;
                    let from_client = Incoming::Inet(conn.try_clone()?);
                    let to_client = Outgoing::Inet(conn);
                    let client = Address::Inet(client.to_string());
                    Ok((from_client, to_client, client))
                }))
            }
            Address::Unix(p) => {
                let listener = match UnixListener::bind(p) {
                    Ok(l) => l,
                    Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                        fs::remove_file(p)?;
                        UnixListener::bind(p)?
                    }
                    Err(other) => return Err(other),
                };
                let p = p.clone();
                Ok(Box::new(move || {
                    let (conn, _) = listener.accept()?;
                    let from_client = Incoming::Unix(conn.try_clone()?);
                    let to_client = Outgoing::Unix(conn);
                    let client = Address::Unix(p.clone());
                    Ok((from_client, to_client, client))
                }))
            }
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::Inet(a) => a.fmt(f),
            Address::Unix(a) => a.display().fmt(f),
            Address::PortOnly(a) => a.fmt(f),
        }
    }
}

enum Incoming {
    Inet(TcpStream),
    Unix(UnixStream),
}

impl Read for Incoming {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Incoming::Inet(conn) => conn.read(buf),
            Incoming::Unix(conn) => conn.read(buf),
        }
    }
}

impl Incoming {
    fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Incoming::Inet(conn) => conn.shutdown(Shutdown::Read),
            Incoming::Unix(conn) => conn.shutdown(Shutdown::Read),
        }
    }
}

enum Outgoing {
    Inet(TcpStream),
    Unix(UnixStream),
}

impl Write for Outgoing {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Outgoing::Inet(conn) => conn.write(buf),
            Outgoing::Unix(conn) => conn.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Outgoing::Inet(conn) => conn.flush(),
            Outgoing::Unix(conn) => conn.flush(),
        }
    }
}

impl Outgoing {
    fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Outgoing::Inet(conn) => conn.shutdown(Shutdown::Write),
            Outgoing::Unix(conn) => conn.shutdown(Shutdown::Write),
        }
    }
}

pub trait Inspector: Send {
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
    I: Inspector + Send + 'static,
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
    I: Inspector + Send + 'static,
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

fn pump(mut inspector: impl Inspector, mut r: Incoming, mut w: Outgoing) -> io::Result<()> {
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
