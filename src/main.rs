mod formatter;
mod inspect;
mod proxy;

use anyhow::{anyhow, Result as AResult};
use argsplitter::{ArgError, ArgSplitter};
use formatter::TextFormatter;
use itertools::Itertools;
use std::io;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::inspect::MessageInspector;
use crate::proxy::{parse_addr, spawn_listener};

const USAGE: &str = "\
Usage:  monetproxy LISTEN_ADDR DEST_ADDR
        (ADDR is PORT or HOST:PORT)
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
        let cloned = Arc::clone(&formatter);
        spawn_listener(addr, da, cloned, MessageInspector::new);
    }

    loop {
        thread::park()
    }
}
