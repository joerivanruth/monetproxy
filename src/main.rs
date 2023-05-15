mod formatter;
mod network;
mod observers;
mod proxy;

use anyhow::Result as AResult;
use argsplitter::{ArgError, ArgSplitter};
use formatter::TextFormatter;
use network::Address;
use std::io;
use std::net::ToSocketAddrs;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;

use observers::{BlockObserver, MessageObserver, RawObserver};
use proxy::spawn_listener;

const VERSION: &str = env!("CARGO_PKG_VERSION");

const USAGE: &str = "\
Usage:  monetproxy [OPTION..] LISTEN_ADDR DEST_ADDR
        (ADDR is PORT or HOST:PORT or ../PATH/TO/SOCKET)
Options:
    -h --help       Show help
    -r --raw        Dump raw bytes
    -b --blocks     Dump blocks
    -m --messages   Dump messages (default)
    -B --binary     Force binary dump
    -v --version    Show version information
";

fn main() -> ExitCode {
    argsplitter::main_support::report_errors(USAGE, mymain())
}

#[derive(Debug, PartialEq, Eq)]
enum Observe {
    Raw,
    Blocks,
    Messages,
}

fn mymain() -> AResult<()> {
    let mut args = ArgSplitter::from_env();
    let mut observe = Observe::Messages;
    let mut force_binary = false;
    while let Some(flag) = args.flag()? {
        match flag {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "-r" | "--raw" => observe = Observe::Raw,
            "-b" | "--blocks" => observe = Observe::Blocks,
            "-m" | "--messages" => observe = Observe::Messages,
            "-B" | "--binary" => force_binary = true,
            "-v" | "--version" => {
                println!("Monetproxy {VERSION}");
                return Ok(());
            }
            _ => Err(ArgError::unknown_flag(flag))?,
        }
    }
    let listen_addr = Address::parse(&args.stashed("LISTEN_ADDR")?)?;
    let forward_addr = Address::parse(&args.stashed("DEST_ADDR")?)?;
    args.no_more_stashed()?;

    let mut formatter = TextFormatter::new(io::stdout());
    formatter.set_force_binary(force_binary);

    let formatter = Arc::new(Mutex::new(formatter));

    for addr in expand_listen_address(&listen_addr)? {
        let fw = forward_addr.clone();
        let cloned = Arc::clone(&formatter);
        match observe {
            Observe::Raw => spawn_listener(addr, fw, cloned, RawObserver::new),
            Observe::Blocks => spawn_listener(addr, fw, cloned, BlockObserver::new),
            Observe::Messages => spawn_listener(addr, fw, cloned, MessageObserver::new),
        };
    }

    loop {
        thread::park()
    }
}

fn expand_listen_address(listen_address: &Address) -> io::Result<Vec<Address>> {
    let mut answers = vec![];

    for addr in listen_address.expand() {
        if let Address::Inet(inet) = addr {
            for a in inet.to_socket_addrs()? {
                answers.push(Address::Inet(a.to_string()))
            }
        } else {
            answers.push(addr)
        }
    }

    Ok(answers)
}
