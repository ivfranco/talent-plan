#![allow(dead_code)]

mod parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Unsupported type")]
    Unsupported,

    #[error("Syntax error, expected {0}")]
    SyntaxError(String),

    #[error("Integer overflow")]
    Overflow,
}

type Result<T> = std::result::Result<T, Error>;

use std::net::{Ipv4Addr, ToSocketAddrs};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

use parser::{Parser, Value};

struct PingClient {
    conn: TcpStream,
}

impl PingClient {
    fn connect<A: ToSocketAddrs>(server: A) -> Result<Self> {
        let conn = TcpStream::connect(server)?;
        Ok(Self { conn })
    }

    fn ping_and_check(&self, msg: Option<&[u8]>) -> Result<()> {
        let mut conn = &self.conn;
        let len = if msg.is_some() { 2 } else { 1 };

        // start of array
        write!(conn, "*{}\r\n", len)?;
        // bulk string "PING"
        write!(conn, "${}\r\nPING", b"PING".len())?;
        if let Some(msg) = msg {
            // bulk string msg
            write!(conn, "${}\r\n", msg.len())?;
            conn.write_all(msg)?;
        }
        // end of array
        conn.write_all(b"\r\n")?;
        conn.flush()?;

        let str = Parser::new(conn).simple_string()?;
        let correct = if let Some(msg) = msg {
            str == msg
        } else {
            &str == b"PONG"
        };

        if correct {
            Ok(())
        } else {
            Err(Error::SyntaxError(format!("{:?}", str)))
        }
    }
}

struct PongServer;

impl PongServer {
    fn listen(port: u16) -> Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
        for stream in listener.incoming() {
            Self::handle(stream?)?;
        }
        Ok(())
    }

    fn handle(mut stream: TcpStream) -> Result<()> {
        let cmd = Parser::new(&mut stream)
            .array()?
            .ok_or_else(|| Error::SyntaxError("Null array command".to_string()))?;

        let ping = cmd
            .get(0)
            .ok_or_else(|| Error::SyntaxError("No command".to_string()))?;

        if ping != &Value::BulkString(b"PING".to_vec()) {
            return Err(Error::Unsupported);
        }

        stream.write_all(b"+")?;
        if let Some(Value::BulkString(msg)) = cmd.get(1) {
            stream.write_all(msg)?;
        } else {
            stream.write_all(b"PONG")?;
        }
        stream.write_all(b"\r\n")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    const REDIS_PORT: u16 = 6379;

    #[test]
    fn local_ping_pong() -> Result<()> {
        thread::spawn(move || {
            PongServer::listen(REDIS_PORT).unwrap();
        });

        let client = PingClient::connect((Ipv4Addr::LOCALHOST, REDIS_PORT))?;
        client.ping_and_check(None)
    }
}
