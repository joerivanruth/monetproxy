use box_drawing::light as boxchars;
use std::io::{self, BufWriter, Write};

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum Style {
    Other,
    Whitespace,
    AlphaNum,
}

pub trait Formatter: io::Write {
    fn message(&mut self, sender: &str, message: &str) -> io::Result<()>;
    fn start_block(&mut self, sender: &str, message: &str) -> io::Result<()>;
    fn end_block(&mut self) -> io::Result<()>;
    fn set_style(&mut self, style: Style) -> io::Result<()>;
}

pub struct TextFormatter {
    out: BufWriter<Box<dyn Write + Send>>,
    in_block: bool,
    at_start: bool,
}

impl TextFormatter {
    pub fn new(w: impl Write + Send + 'static) -> TextFormatter {
        let w: Box<dyn Write + Send> = Box::new(w);
        let out = BufWriter::new(w);
        TextFormatter {
            out,
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
}

impl io::Write for TextFormatter {
    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        assert!(self.in_block);
        let x = buf.split_inclusive(|b| *b == b'\n');
        for line in x {
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
    fn message(&mut self, sender: &str, message: &str) -> io::Result<()> {
        assert!(!self.in_block);
        assert!(self.at_start);
        writeln!(self.out, " {sender} {message}")?;
        self.flush()
    }

    fn start_block(&mut self, sender: &str, message: &str) -> io::Result<()> {
        assert!(!self.in_block);
        assert!(self.at_start);
        write!(self.out, "{} MESSAGE {sender}", boxchars::DOWN_RIGHT)?;
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

    fn set_style(&mut self, style: Style) -> io::Result<()> {
        let _ = style;
        Ok(())
    }
}

pub fn dump_text<F: Formatter>(f: &mut F, data: &[u8]) -> io::Result<()> {
    for &b in data {
        match char::from_u32(b as u32) {
            Some(c) if !c.is_ascii_control() => write!(f, "{c}")?,
            Some('\n') => {
                writeln!(f, "↵")?;
            }
            Some('\t') => write!(f, "→")?,
            _ => write!(f, "«{b:02X}»")?,
        }
    }
    Ok(())
}

pub fn dump_binary<F: Formatter>(f: &mut F, data: &[u8]) -> io::Result<()> {
    let n = 16;
    for line in data.chunks(n) {
        dump_line(f, line, n)?;
    }
    Ok(())
}

fn dump_line<F: Formatter>(f: &mut F, data: &[u8], n: usize) -> io::Result<()> {
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
            Some(c) if !c.is_ascii_control() => c,
            Some('\n') => '↵',
            Some('\t') => '→',
            _ => '░',
        };
        write!(f, "{disp}")?;
    }

    writeln!(f)
}
