use crate::{
    protocol::{read_tagged, write_tagged, Error, Response},
    Command,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
};

/// A persistent key-value store client.
pub struct KvsClient {
    conn: TcpStream,
}

impl KvsClient {
    /// Connect to a server at the given address.
    pub async fn connect<S: ToSocketAddrs>(addr: S) -> Result<Self, Error> {
        let conn = TcpStream::connect(addr).await?;
        Ok(Self { conn })
    }

    /// Get the string value of a given string key.
    pub async fn get(&mut self, key: String) -> Result<Option<String>, Error> {
        let command = Command::Get(key);
        match self.request(&command).await? {
            r @ Response::Ok => Err(Error::UnexpectedResponse(command, r)),
            Response::Value(value) => Ok(value),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    /// Set the value of a string key to a string.
    pub async fn set(&mut self, key: String, value: String) -> Result<(), Error> {
        let command = Command::Set(key, value);
        match self.request(&command).await? {
            Response::Ok => Ok(()),
            r @ Response::Value(..) => Err(Error::UnexpectedResponse(command, r)),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    /// Remove a given key.
    pub async fn remove(&mut self, key: String) -> Result<(), Error> {
        let command = Command::Remove(key);
        match self.request(&command).await? {
            Response::Ok => Ok(()),
            r @ Response::Value(..) => Err(Error::UnexpectedResponse(command, r)),
            Response::Err(e) => Err(Error::ServerError(e)),
        }
    }

    async fn request(&mut self, command: &Command) -> Result<Response, Error> {
        write_tagged(&mut self.conn, command).await?;
        self.conn.flush().await?;
        let response = read_tagged(&mut self.conn).await?;
        Ok(response)
    }
}
