use crate::{
    sled_engine::{SledKvsEngine, SLED_STORE_DIR},
    Command, Error as KvsError, KvStore, KvsEngine, STORE_NAME,
};
use serde::{Deserialize, Serialize};
use std::{
    io::{Error as IOError, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    str::FromStr,
};
use thiserror::Error;

/// This type represents all possible errors that can occur when operating the
/// key-value store server and client.
#[derive(Debug, Error)]
pub enum Error {
    /// Unknown engine flavor.
    #[error("Unknown engine flavor")]
    UnknownEngine,

    /// The persisting engine and the required engine doesn't match.
    #[error("Engine mismatch")]
    EngineMismatch,

    /// Error returned by the key-value store engine.
    #[error("{0}")]
    Kvs(#[from] KvsError),

    /// Error returned by the network stack.
    #[error("{0}")]
    Net(#[from] std::io::Error),

    /// Error in the server-client protocol.
    #[error("{0}")]
    Protocol(#[from] serde_json::Error),
}

impl Error {
    fn should_halt(&self) -> bool {
        match self {
            Error::Kvs(e) => e.should_halt(),
            _ => false,
        }
    }
}

/// Flavor of engines.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Flavor {
    /// The built-in Kvs engine.
    Kvs,
    /// The [Sled](sled) embedded database.
    Sled,
}

impl FromStr for Flavor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Flavor::Kvs),
            "sled" => Ok(Flavor::Sled),
            _ => Err(Error::UnknownEngine),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Response {
    Ok,
    Value(Option<String>),
    Err(String),
}

/// A persistent key-value store server.
pub struct KvsServer {
    engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    /// Start the server with the engine flavor.
    pub fn open(flavor: Option<Flavor>) -> Result<Self, Error> {
        let cwd = std::env::current_dir()?;

        let flavor = match (flavor, persisting_flavor(&cwd)) {
            (Some(f), Some(p)) if f != p => return Err(Error::EngineMismatch),
            (arg, persistent) => arg.or(persistent).unwrap_or(Flavor::Kvs),
        };

        let engine: Box<dyn KvsEngine> = match flavor {
            Flavor::Kvs => Box::new(KvStore::open(&cwd)?),
            Flavor::Sled => Box::new(SledKvsEngine::open(&cwd).map_err(KvsError::Sled)?),
        };

        Ok(Self { engine })
    }

    /// Start accepting and serving incoming requests from clients on the address.
    pub fn listen(&mut self, addr: SocketAddr) -> Result<(), Error> {
        for stream in TcpListener::bind(addr)?.incoming() {
            let stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    error!("On accepting TCP connection: {}", err);
                    continue;
                }
            };

            if let Err(err) = self.serve(&stream) {
                error!("On serving client via TCP: {}", err);
                // the stream is likely unusable on a net error
                if !matches!(err, Error::Net(..)) {
                    let _ = serde_json::to_writer(&stream, &Response::Err(err.to_string()));
                }
                // server should not halt on any error that's non-fatal
                if err.should_halt() {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn serve(&mut self, mut stream: &TcpStream) -> Result<(), Error> {
        match serde_json::from_reader(stream)? {
            Command::Get(key) => {
                let value = self.engine.get(key)?;
                serde_json::to_writer(stream, &Response::Value(value)).map_err(IOError::from)?;
            }
            Command::Set(key, value) => {
                self.engine.set(key, value)?;
                serde_json::to_writer(stream, &Response::Ok).map_err(IOError::from)?;
            }
            Command::Remove(key) => {
                self.engine.remove(key)?;
                serde_json::to_writer(stream, &Response::Ok).map_err(IOError::from)?;
            }
        }

        stream.flush()?;
        Ok(())
    }
}

fn persisting_flavor<P: AsRef<Path>>(path: P) -> Option<Flavor> {
    let path = path.as_ref();
    if path.join(STORE_NAME).exists() {
        Some(Flavor::Kvs)
    } else if path.join(SLED_STORE_DIR).exists() {
        Some(Flavor::Sled)
    } else {
        None
    }
}
