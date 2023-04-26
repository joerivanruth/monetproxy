use std::io;

use std::str::from_utf8;
use std::sync::{Arc, Mutex};

use crate::formatter::Side;
use crate::formatter::{dump_binary, dump_text, Formatter};
use crate::proxy::Observer;

struct Blocks {
    buffer: Vec<u8>,
    goal: usize,
    last_block: bool,
}

impl Blocks {
    fn new() -> Blocks {
        Blocks {
            buffer: Vec::with_capacity(8192),
            goal: 2,
            last_block: false,
        }
    }

    fn process(&mut self,mut data: &[u8],callback: &mut dyn FnMut(&[u8], bool) -> io::Result<()>
    ) -> io::Result<()> {
        while !data.is_empty() {
            // Move some data into the buffer, there must be some room
            assert!(self.buffer.len() < self.goal);
            let to_read = self.goal - self.buffer.len();
            let n = data.len().min(to_read);
            let (append, rest) = data.split_at(n);
            self.buffer.extend_from_slice(append);
            data = rest;

            if self.buffer.len() < self.goal {
                continue;
            }

            // We've reached the goal.
            // Three situations to consider
            // 1. we've just completed reading the header of a nonempty block
            // 2. we've just completed reading the headerr of an empty block
            // 3. we've just completed reading the data of a block
            assert!(self.goal >= 2);
            if self.goal == 2 {
                // we've read the header, process it
                let header: [u8; 2] = self.buffer[..2].try_into().unwrap();
                let header = u16::from_le_bytes(header);
                self.goal += (header / 2) as usize;
                self.last_block = (header & 1) != 0;

                if self.goal > 2 {
                    // situation 1.
                    continue;
                }
                // situation 2. fall through to situation 3.
            }
            // situation 2 or 3
            let result = callback(&self.buffer[2..], self.last_block);
            self.buffer.clear();
            self.goal = 2;
            result?;
        }
        Ok(())
    }

    fn describe_eof(&mut self) -> Result<&'static str, &'static str> {
        let n = self.buffer.len();
        assert_ne!(n, self.goal);
        match n {
            0 => Ok("closed its side of the connection"),
            1 => Err("eof on incomplete block header"),
            _ => Err("eof on incomplete block body")
        }
    }
}

pub struct MessageObserver<F> {
    formatter: Arc<Mutex<F>>,
    side: Side,
    blocks: Blocks,
    message: Vec<u8>,
}

impl<F: Formatter> MessageObserver<F> {
    pub fn new(side: Side, formatter: Arc<Mutex<F>>) -> MessageObserver<F> {
        MessageObserver {
            formatter,
            side,
            blocks: Blocks::new(),
            message: Vec::new(),
        }
    }
}

fn print_message(f: &mut dyn Formatter, side: Side, data: &[u8]) -> io::Result<()> {
    let text = is_printable_text(data);

    let n = data.len();
    let msg = if text.is_some() {
        if data.is_empty() || data.ends_with(b"\n") {
            format!("text, {n} bytes")
        } else {
            format!("text, {n} bytes, no trailing newline!")
        }
    } else {
        format!("binary, {n} bytes")
    };

    f.start_block(side, &msg)?;
    if let Some(t) = text {
        dump_text(f, t)?;
    } else {
        dump_binary(f, data)?;
    }
    f.end_block()?;

    Ok(())
}

impl<F: Formatter + Send> Observer for MessageObserver<F> {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.blocks.process(data, &mut |block, is_last| {
            self.message.extend_from_slice(block);
            if is_last {
                let mut f = self.formatter.lock().unwrap();
                let result = print_message(&mut *f, self.side, &self.message);
                self.message.clear();
                result
            } else {
                Ok(())
            }
        })?;

        Ok(())
    }

    fn on_close(&mut self) -> io::Result<()> {
        let mut f = self.formatter.lock().unwrap();
        match self.blocks.describe_eof() {
            Ok(msg) => f.message(self.side, msg),
            Err(msg) => f.message(self.side, msg),
        }
    }

    fn on_error(&mut self, kind: &str, err: &io::Error) -> io::Result<()> {
        let msg = format!("encountered an error {kind}: {err}");
        let mut f = self.formatter.lock().unwrap();
        f.message(self.side, &msg)
    }

    fn on_unix0(&mut self, _data: &[u8], _message: &str) -> io::Result<()> {
        // ignore
        Ok(())
    }
}

fn is_printable_text(data: &[u8]) -> Option<&str> {
    if let Ok(text) = from_utf8(data) {
        let scary = text
            .chars()
            .find(|&c| c.is_control() && c != '\n' && c != '\t');
        if scary.is_some() {
            None
        } else {
            Some(text)
        }
    } else {
        None
    }
}