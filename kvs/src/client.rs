use std::net::{SocketAddr, TcpStream};

use crate::{
    server::{Error, Response},
    Command,
};

/// A persistent key-value store client.
pub struct Client {
    conn: TcpStream,
}

impl Client {
    fn connect(addr: SocketAddr) -> Result<Self, Error> {
        let conn = TcpStream::connect(addr)?;
        Ok(Self { conn })
    }

    fn get(&self, key: String) -> Result<Option<String>, Error> {
        unimplemented!()
    }

    fn request(&self, command: &Command) -> Result<Response, Error> {
        serde_json::to_writer(&self.conn, command)?;
        let res = serde_json::from_reader(&self.conn)?;
        Ok(res)
    }
}
