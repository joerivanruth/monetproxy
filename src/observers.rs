use std::io;

use std::sync::{Arc, Mutex};

use crate::formatter::Formatter;
use crate::formatter::{print_message, Side};
use crate::proxy::Observer;

const CLOSE_MESSAGE: &str = "closed its side of the connection";

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

    fn process(
        &mut self,
        mut data: &[u8],
        callback: &mut dyn FnMut(&[u8], bool) -> io::Result<()>,
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

    fn describe_eof(&mut self) -> &'static str {
        let n = self.buffer.len();
        assert_ne!(n, self.goal);
        match n {
            0 => CLOSE_MESSAGE,
            1 => "eof on incomplete block header",
            _ => "eof on incomplete block body",
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

impl<F: Formatter + Send> Observer for MessageObserver<F> {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.blocks.process(data, &mut |block, is_last| {
            self.message.extend_from_slice(block);
            if is_last {
                let mut f = self.formatter.lock().unwrap();
                let result = print_message(&mut *f, self.side, &self.message, &[]);
                self.message.clear();
                result
            } else {
                Ok(())
            }
        })?;

        Ok(())
    }

    fn on_close(&mut self) -> io::Result<()> {
        let message = self.blocks.describe_eof();
        self.formatter.lock().unwrap().message(self.side, message)
    }

    fn on_error(&mut self, while_writing: bool, err: &io::Error) -> io::Result<()> {
        let action = describe_error(self.side, while_writing);
        let msg = format!("{action}: {err}");
        self.formatter.lock().unwrap().message(self.side, &msg)
    }

    fn on_unix0(&mut self, _data: &[u8], _message: Option<&str>) -> io::Result<()> {
        // ignore
        Ok(())
    }
}

pub struct RawObserver<F> {
    formatter: Arc<Mutex<F>>,
    side: Side,
}

impl<F: Formatter + Send> RawObserver<F> {
    pub fn new(side: Side, formatter: Arc<Mutex<F>>) -> RawObserver<F> {
        RawObserver { formatter, side }
    }
}

impl<F: Formatter + Send> Observer for RawObserver<F> {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()> {
        let mut f = self.formatter.lock().unwrap();
        print_message(&mut *f, self.side, data, &[])
    }

    fn on_close(&mut self) -> io::Result<()> {
        self.formatter.lock().unwrap().message(self.side, CLOSE_MESSAGE)
    }

    fn on_error(&mut self, while_writing: bool, err: &io::Error) -> io::Result<()> {
        let action = describe_error(self.side, while_writing);
        let msg = format!("{action}: {err}");
        self.formatter.lock().unwrap().message(self.side, &msg)
    }

    fn on_unix0(&mut self, data: &[u8], message: Option<&str>) -> io::Result<()> {
        self.on_data(data)?;
        if let Some(m) = message {
            self.formatter.lock().unwrap().message(self.side, m)?
        }
        Ok(())
    }
}

pub struct BlockObserver<F> {
    formatter: Arc<Mutex<F>>,
    side: Side,
    blocks: Blocks,
}

impl<F: Formatter + Send> BlockObserver<F> {
    pub fn new(side: Side, formatter: Arc<Mutex<F>>) -> BlockObserver<F> {
        BlockObserver {
            formatter,
            side,
            blocks: Blocks::new(),
        }
    }
}

impl<F: Formatter + Send> Observer for BlockObserver<F> {
    fn on_data(&mut self, data: &[u8]) -> io::Result<()> {
        let mut f = self.formatter.lock().unwrap();
        self.blocks.process(data, &mut |block, is_last| {
            let remarks = [if is_last {
                "ends the message"
            } else {
                "does not end the message"
            }];
            print_message(&mut *f, self.side, block, &remarks)
        })
    }

    fn on_close(&mut self) -> io::Result<()> {
        let message = self.blocks.describe_eof();
        self.formatter.lock().unwrap().message(self.side, message)
    }

    fn on_error(&mut self, while_writing: bool, err: &io::Error) -> io::Result<()> {
        let action = describe_error(self.side, while_writing);
        let msg = format!("{action}: {err}");
        self.formatter.lock().unwrap().message(self.side, &msg)
    }

    fn on_unix0(&mut self, _data: &[u8], message: Option<&str>) -> io::Result<()> {
        if let Some(m) = message {
            self.formatter.lock().unwrap().message(self.side, m)?
        }
        Ok(())
    }
}


fn describe_error(side: Side, while_writing: bool) -> &'static str {
    match (side, while_writing) {
        (Side::Client, true) => "encountered an error writing to server",
        (Side::Server, true) => "encountered an error writing to client",
        (_, false) => "could not be read",
    }
}