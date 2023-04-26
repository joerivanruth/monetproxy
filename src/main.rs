mod formatter;
mod inspect;
mod proxy;

use anyhow::Result as AResult;
use argsplitter::{ArgError, ArgSplitter};
use formatter::TextFormatter;
use std::io;
use std::net::ToSocketAddrs;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::inspect::MessageInspector;
use crate::proxy::{spawn_listener, Address};

const USAGE: &str = "\
Usage:  monetproxy LISTEN_ADDR DEST_ADDR
        (ADDR is PORT or HOST:PORT or ../PATH/TO/SOCKET)
";

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
    let listen_addr = Address::parse(&args.stashed("LISTEN_ADDR")?)?;
    let forward_addr = Address::parse(&args.stashed("DEST_ADDR")?)?;
    args.no_more_stashed()?;

    let formatter = TextFormatter::new(io::stdout());
    let formatter = Arc::new(Mutex::new(formatter));

    for addr in expand_listen_address(&listen_addr)? {
        let cloned = Arc::clone(&formatter);
        spawn_listener(addr, forward_addr.clone(), cloned, MessageInspector::new);
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
