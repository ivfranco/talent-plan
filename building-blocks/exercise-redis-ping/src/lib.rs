#![allow(dead_code)]

mod parser;
use serde::{Deserialize, Serialize};
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

    #[error("Serde error: {0}")]
    Serde(String),

    #[error("CR/LF in simple string")]
    SimpleCRLF,

    #[error("Simple string is not encoded as utf8")]
    NonUtf8,
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Serde(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Serde(msg.to_string())
    }
}

type Result<T> = std::result::Result<T, Error>;

use std::net::{TcpListener, TcpStream};
use std::{
    net::{Ipv4Addr, ToSocketAddrs},
    str::from_utf8,
};

use parser::{Parser, Serializer, Value};

struct PingClient {
    conn: TcpStream,
}

impl PingClient {
    fn connect<A: ToSocketAddrs>(server: A) -> Result<Self> {
        let conn = TcpStream::connect(server)?;
        Ok(Self { conn })
    }

    fn ping_and_check(&self, msg: Option<&[u8]>) -> Result<()> {
        let conn = &self.conn;

        let mut cmd = vec![Value::BulkString(b"PING".to_vec())];
        if let Some(msg) = msg {
            cmd.push(Value::BulkString(msg.to_vec()));
        }

        Value::Array(cmd).serialize(&mut Serializer::from_writer(conn))?;

        let str = String::deserialize(&mut Parser::new(conn))?;
        let correct = if let Some(msg) = msg {
            str.as_bytes() == msg
        } else {
            &str == "PONG"
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
        let cmd = Vec::<Value>::deserialize(&mut Parser::new(&mut stream))?;

        let ping = cmd
            .get(0)
            .ok_or_else(|| Error::SyntaxError("No command".to_string()))?;

        if ping != &Value::BulkString(b"PING".to_vec()) {
            return Err(Error::Unsupported);
        }

        let response = if let Some(Value::BulkString(msg)) = cmd.get(1) {
            Value::SimpleString(
                from_utf8(msg)
                    .map(|s| s.to_string())
                    .map_err(|_| Error::NonUtf8)?,
            )
        } else {
            Value::SimpleString("PONG".to_string())
        };

        response.serialize(&mut Serializer::from_writer(stream))?;

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
