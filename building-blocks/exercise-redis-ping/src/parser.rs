use std::io::{BufRead, BufReader, ErrorKind, Read};

use super::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Value>),
}

pub struct Parser<R> {
    reader: BufReader<R>,
}

impl<R: Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    fn peek(&mut self) -> Result<u8> {
        self.buffer().map(|buf| buf[0])
    }

    fn tag(&mut self, bytes: &[u8]) -> Result<()> {
        let mut buf = vec![0; bytes.len()];
        self.reader.read_exact(&mut buf)?;
        if buf == bytes {
            Ok(())
        } else {
            Err(Error::SyntaxError(format!(
                "Expected {:x?}, Found {:x?}",
                bytes, buf
            )))
        }
    }

    fn crlf(&mut self) -> Result<()> {
        self.tag(b"\r\n")
    }

    fn buffer(&mut self) -> Result<&[u8]> {
        let buf = if self.reader.buffer().is_empty() {
            self.reader.fill_buf()?
        } else {
            self.reader.buffer()
        };

        if buf.is_empty() {
            Err(Error::IO(std::io::Error::from(ErrorKind::UnexpectedEof)))
        } else {
            Ok(buf)
        }
    }

    fn take_while<F>(&mut self, mut f: F) -> Result<Vec<u8>>
    where
        F: FnMut(&u8) -> bool,
    {
        let mut vec = vec![];
        loop {
            let buf = self.buffer()?;
            let amt = buf.iter().position(|b| !f(b)).unwrap_or(buf.len());
            let (prefix, suffix) = buf.split_at(amt);
            vec.extend_from_slice(prefix);
            let consumed = suffix.is_empty();

            self.reader.consume(amt);
            if !consumed {
                return Ok(vec);
            }
        }
    }

    fn unsigned(&mut self) -> Result<i64> {
        let digits = self.take_while(u8::is_ascii_digit)?;
        digits
            .into_iter()
            .fold(Some(0i64), |int, b| {
                let int = int?.checked_mul(10)?;
                int.checked_add(i64::from(b - b'0'))
            })
            .ok_or(Error::Overflow)
    }

    fn signed(&mut self) -> Result<i64> {
        let buf = self.buffer()?;

        let neg = buf.starts_with(b"-");
        if neg {
            self.reader.consume(1);
        }

        let int = self.unsigned()?;

        if neg {
            int.checked_neg().ok_or(Error::Overflow)
        } else {
            Ok(int)
        }
    }

    fn parse(&mut self) -> Result<Value> {
        match self.peek()? {
            b'+' => self.simple_string().map(Value::SimpleString),
            b':' => self.integer().map(Value::Integer),
            b'$' => match self.bulk_string() {
                Ok(Some(str)) => Ok(Value::BulkString(str)),
                Ok(None) => Ok(Value::Null),
                Err(e) => Err(e),
            },
            b'*' => match self.array() {
                Ok(Some(arr)) => Ok(Value::Array(arr)),
                Ok(None) => Ok(Value::Null),
                Err(e) => Err(e),
            },
            _ => Err(Error::Unsupported),
        }
    }

    pub fn simple_string(&mut self) -> Result<Vec<u8>> {
        self.tag(b"+")?;
        let str = self.take_while(|&b| b != b'\r' && b != b'\n')?;
        self.crlf()?;
        Ok(str)
    }

    pub fn integer(&mut self) -> Result<i64> {
        self.tag(b":")?;
        let int = self.signed()?;
        self.crlf()?;
        Ok(int)
    }

    pub fn bulk_string(&mut self) -> Result<Option<Vec<u8>>> {
        self.tag(b"$")?;
        let len = self.signed()?;
        self.crlf()?;

        if len < 0 {
            Ok(None)
        } else {
            let mut str = vec![0; len as usize];
            self.reader.read_exact(&mut str)?;
            self.crlf()?;
            Ok(Some(str))
        }
    }

    pub fn array(&mut self) -> Result<Option<Vec<Value>>> {
        self.tag(b"*")?;
        let len = self.signed()?;
        self.crlf()?;
        if len < 0 {
            return Ok(None);
        }

        let mut arr = Vec::with_capacity(len as usize);
        for _ in 0..len {
            arr.push(self.parse()?);
        }

        Ok(Some(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_integer() -> Result<()> {
        let mut parser = Parser::new(b":-11313\r\n".as_ref());
        assert_eq!(parser.integer()?, -11313);
        Ok(())
    }

    #[test]
    fn parse_simple_string() -> Result<()> {
        let mut parser = Parser::new(b"+This is a test\r\n".as_ref());
        assert_eq!(parser.simple_string()?, b"This is a test");
        Ok(())
    }

    #[test]
    fn parse_bulk_string() -> Result<()> {
        let mut parser = Parser::new(b"$10\r\nHelloWorld\r\n".as_ref());
        assert_eq!(parser.bulk_string()?, Some(b"HelloWorld".to_vec()));
        Ok(())
    }

    #[test]
    fn parse_null_string() -> Result<()> {
        let mut parser = Parser::new(b"$-1\r\n".as_ref());
        assert_eq!(parser.bulk_string()?, None);
        Ok(())
    }

    #[test]
    fn parse_array() -> Result<()> {
        let mut parser = Parser::new(b"*3\r\n:1\r\n:2\r\n:3\r\n".as_ref());
        assert_eq!(
            parser.array()?,
            Some(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])
        );
        Ok(())
    }

    #[test]
    fn parse_null_array() -> Result<()> {
        let mut parser = Parser::new(b"*-1\r\n".as_ref());
        assert_eq!(parser.array()?, None);
        Ok(())
    }
}
