use std::io;

use std::str::from_utf8;
use std::sync::{Arc, Mutex};

use crate::formatter::Side;
use crate::formatter::{dump_binary, dump_text, Formatter};
use crate::proxy::Inspector;

pub struct MessageInspector<F> {
    side: Side,
    formatter: Arc<Mutex<F>>,
    buffer: Vec<u8>,
    goal: usize,
    last_block: bool,
}

impl<F: Formatter> MessageInspector<F> {
    pub fn new(side: Side, formatter: Arc<Mutex<F>>) -> MessageInspector<F> {
        MessageInspector {
            side,
            formatter,
            buffer: vec![],
            goal: 2,
            last_block: false,
        }
    }

    fn goal_reached(&mut self) -> io::Result<()> {
        assert!(self.goal >= 2);
        if self.goal == 2 {
            let header: [u8; 2] = self.buffer[0..2].try_into().unwrap();
            let header = u16::from_le_bytes(header);
            let last = (header & 1) != 0;
            let size = (header / 2) as usize;
            self.goal += size;
            self.last_block = last;
            if size > 0 {
                return Ok(());
            }
        }

        self.on_message(&self.buffer[2..])?;
        self.buffer.clear();
        self.goal = 2;
        self.last_block = false;

        Ok(())
    }

    fn on_message(&self, data: &[u8]) -> io::Result<()> {
        let n = data.len();
        let text = if let Ok(text) = from_utf8(data) {
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
        };

        let msg = if text.is_some() {
            if data.is_empty() || data.ends_with(b"\n") {
                format!("text, {n} bytes")
            } else {
                format!("text, {n} bytes, no trailing newline!")
            }
        } else {
            format!("binary, {n} bytes")
        };

        let mut f = self.formatter.lock().unwrap();
        f.start_block(self.side, &msg)?;
        if let Some(t) = text {
            dump_text(&mut *f, t)?;
        } else {
            dump_binary(&mut *f, data)?;
        }
        f.end_block()?;

        Ok(())
    }
}

impl<F: Formatter + Send> Inspector for MessageInspector<F> {
    fn on_data(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            assert!(self.buffer.len() < self.goal);
            let to_read = self.goal - self.buffer.len();
            let n = data.len().min(to_read);
            self.buffer.extend_from_slice(&data[..n]);
            data = &data[n..];

            if self.buffer.len() == self.goal {
                self.goal_reached()?;
                assert_ne!(self.buffer.len(), self.goal);
            }
        }

        Ok(())
    }

    fn on_close(&mut self) -> io::Result<()> {
        let n = self.buffer.len();

        let msg = if n == 0 {
            "closed its side of the connection"
        } else if n == 1 {
            "eof on incomplete header: 1/2"
        } else {
            "eof on incomplete body"
        };

        let mut f = self.formatter.lock().unwrap();
        f.message(self.side, msg)
    }

    fn on_error(&mut self, kind: &str, err: &io::Error) -> io::Result<()> {
        let msg = format!("encountered an error {kind}: {err}");
        let mut f = self.formatter.lock().unwrap();
        f.message(self.side, &msg)
    }

    fn on_adjustment(&mut self, _message: &str) -> io::Result<()> {
        // ignore
        Ok(())
    }
}
