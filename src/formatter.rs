use box_drawing::light as boxchars;
use std::{
    fmt,
    io::{self, BufWriter, Write},
    str::from_utf8,
};

#[derive(Debug, Clone, Copy)]
pub enum Side {
    Client,
    Server,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Side::Client => "CLIENT",
            Side::Server => "SERVER",
        };
        f.write_str(s)
    }
}

pub trait Formatter: io::Write {
    fn connected(&mut self, local: &dyn fmt::Display, remote: &dyn fmt::Display) -> io::Result<()>;
    fn message(&mut self, side: Side, message: &str) -> io::Result<()>;
    fn start_block(&mut self, side: Side, message: &str) -> io::Result<()>;
    fn end_block(&mut self) -> io::Result<()>;
    fn force_binary(&self) -> bool;
}

pub struct TextFormatter {
    out: BufWriter<Box<dyn Write + Send>>,
    force_binary: bool,
    in_block: bool,
    at_start: bool,
}

impl TextFormatter {
    pub fn new(w: impl Write + Send + 'static) -> TextFormatter {
        let w: Box<dyn Write + Send> = Box::new(w);
        let out = BufWriter::new(w);
        TextFormatter {
            out,
            force_binary: false,
            in_block: false,
            at_start: true,
        }
    }

    fn go_to_start(&mut self) -> io::Result<()> {
        assert!(self.in_block);
        if !self.at_start {
            self.out.write_all(b"\n")?;
        }
        self.at_start = true;
        Ok(())
    }

    pub fn set_force_binary(&mut self, b: bool) {
        self.force_binary = b;
    }
}

impl io::Write for TextFormatter {
    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        assert!(self.in_block);
        for line in buf.split_inclusive(|b| *b == b'\n') {
            assert!(!line.is_empty());
            if self.at_start {
                self.out.write_all(boxchars::VERTICAL.as_bytes())?;
                self.at_start = false;
            }
            self.out.write_all(line)?;
            self.at_start = line.ends_with(b"\n");
        }
        Ok(buf.len())
    }
}

impl Formatter for TextFormatter {
    fn connected(
        &mut self,
        client: &dyn fmt::Display,
        server: &dyn fmt::Display,
    ) -> io::Result<()> {
        writeln!(self.out, "• PROXY {client} to {server}")
    }

    fn message(&mut self, side: Side, message: &str) -> io::Result<()> {
        assert!(!self.in_block);
        assert!(self.at_start);
        writeln!(self.out, "• {side} {message}")?;
        self.flush()
    }

    fn start_block(&mut self, side: Side, message: &str) -> io::Result<()> {
        assert!(!self.in_block);
        assert!(self.at_start);
        write!(self.out, "{} {side}", boxchars::DOWN_RIGHT)?;
        if !message.is_empty() {
            write!(self.out, " {message}")?;
        }
        writeln!(self.out)?;
        self.in_block = true;
        Ok(())
    }

    fn end_block(&mut self) -> io::Result<()> {
        assert!(self.in_block);
        self.go_to_start()?;
        self.out.write_all(boxchars::UP_RIGHT.as_bytes())?;
        self.out.write_all(b"\n")?;
        self.flush()?;
        self.in_block = false;
        Ok(())
    }

    fn force_binary(&self) -> bool {
        self.force_binary
    }
}

pub fn dump_text(f: &mut dyn Formatter, text: &str) -> io::Result<()> {
    for c in text.chars() {
        match c {
            '\n' => writeln!(f, "↵")?,
            '\t' => write!(f, "→")?,
            _ => write!(f, "{c}")?,
        }
    }
    Ok(())
}

pub fn dump_binary(f: &mut dyn Formatter, data: &[u8]) -> io::Result<()> {
    let n = 16;
    for line in data.chunks(n) {
        dump_line(f, line, n)?;
    }
    Ok(())
}

fn dump_line(f: &mut dyn Formatter, data: &[u8], n: usize) -> io::Result<()> {
    for i in 0..n {
        if i < data.len() {
            let b = data[i];
            write!(f, "{:02x} ", b)?;
        } else {
            // write!(f, "·· ")?;
            write!(f, "__ ")?;
        }
        if i % 4 == 3 {
            f.write_all(b" ")?;
        }
        if i % 8 == 7 {
            f.write_all(b" ")?;
        }
    }

    write!(f, "  ")?;
    for &b in data {
        let disp = match char::from_u32(b as u32) {
            Some(c) if c.is_ascii() && !c.is_ascii_control() => c,
            Some('\n') => '↵',
            Some('\t') => '→',
            Some('\u{0000}') => '░',
            _ => '▒',
        };
        write!(f, "{disp}")?;
    }

    writeln!(f)
}

pub fn print_message(
    f: &mut dyn Formatter,
    side: Side,
    data: &[u8],
    remarks: &[&str],
) -> io::Result<()> {
    let text = if f.force_binary() {
        None
    } else {
        is_printable_text(data)
    };

    let n = data.len();
    let mut msg = if text.is_some() {
        if data.is_empty() || data.ends_with(b"\n") {
            format!("text, {n} bytes")
        } else {
            format!("text, {n} bytes, no trailing newline")
        }
    } else {
        format!("binary, {n} bytes")
    };
    for r in remarks {
        msg.push_str(", ");
        msg.push_str(r);
    }

    f.start_block(side, &msg)?;
    if let Some(t) = text {
        dump_text(f, t)?;
    } else {
        dump_binary(f, data)?;
    }
    f.end_block()?;

    Ok(())
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
