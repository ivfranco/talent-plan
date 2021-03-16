use std::{
    io::Write,
    net::{SocketAddr, TcpStream},
};

use crate::{
    server::{Error, Response},
    Command,
};

/// A persistent key-value store client.
pub struct KvsClient {
    conn: TcpStream,
}

impl KvsClient {
    /// Connect to a server at the given address.
    pub fn connect(addr: SocketAddr) -> Result<Self, Error> {
        let conn = TcpStream::connect(addr)?;
        Ok(Self { conn })
    }

    /// Get the string value of a given string key.
    pub fn get(&self, key: String) -> Result<Option<String>, Error> {
        let command = Command::Get(key);
        match self.request(&command)? {
            r @ Response::Ok => Err(Error::UnexpectedResponse(command, r)),
            Response::Value(value) => Ok(value),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    /// Set the value of a string key to a string.
    pub fn set(&self, key: String, value: String) -> Result<(), Error> {
        let command = Command::Set(key, value);
        match self.request(&command)? {
            Response::Ok => Ok(()),
            r @ Response::Value(..) => Err(Error::UnexpectedResponse(command, r)),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    /// Remove a given key.
    pub fn remove(&self, key: String) -> Result<(), Error> {
        let command = Command::Remove(key);
        match self.request(&command)? {
            Response::Ok => Ok(()),
            r @ Response::Value(..) => Err(Error::UnexpectedResponse(command, r)),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    fn request(&self, command: &Command) -> Result<Response, Error> {
        serde_json::to_writer(&self.conn, command)?;
        (&self.conn).flush()?;
        let res = serde_json::from_reader(&self.conn)?;
        Ok(res)
    }
}
