use std::borrow::Cow;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{self, PathBuf};
use std::{fmt, fs, io};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    Inet(String),
    Unix(PathBuf),
    PortOnly(u16),
}

pub type Accepter = dyn FnMut() -> io::Result<(Incoming, Outgoing, Address)>;

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

    pub fn listen(&self) -> io::Result<Box<Accepter>> {
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

pub enum Incoming {
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
    pub fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Incoming::Inet(conn) => conn.shutdown(Shutdown::Read),
            Incoming::Unix(conn) => conn.shutdown(Shutdown::Read),
        }
    }
}

pub enum Outgoing {
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
    pub fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Outgoing::Inet(conn) => conn.shutdown(Shutdown::Write),
            Outgoing::Unix(conn) => conn.shutdown(Shutdown::Write),
        }
    }
}
