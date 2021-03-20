use crate::{
    listener::{Listener, ServerOrder, ShutdownSwitch},
    log_engine::LogKvsEngine,
    sled_engine::SledKvsEngine,
    Command, Error as KvsError, KvsEngine,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    io::{Cursor, Error as IOError, Read, Write},
    net::SocketAddr,
    path::Path,
    str::FromStr,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// This type represents all possible errors that can occur when operating the
/// key-value store server and client.
#[derive(Error)]
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
    ProtocolParse(#[from] serde_json::Error),

    /// Unexpected response to a command.
    #[error("Unexpected response {1} to command {0}")]
    UnexpectedResponse(Command, Response),

    /// Server side error.
    #[error("{0}")]
    ServerError(String),

    /// Unexpected EOF.
    #[error("Unexpected EOF")]
    UnexpectedEOF,
}

impl Error {
    #[allow(dead_code)]
    fn should_halt(&self) -> bool {
        match self {
            Error::Kvs(e) => e.should_halt(),
            _ => false,
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
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

/// Server response to client comments.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    /// Response to Set and Remove.
    Ok,
    /// Response to Get.
    Value(Option<String>),
    /// Error Responses.
    Err(String),
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

/// A persistent key-value store server.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
    listener: Listener,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Start the server with the provided key-value store engine and thread pool.
    pub async fn bind(engine: E, addr: SocketAddr) -> Result<(Self, ShutdownSwitch), Error> {
        let (listener, switch) = Listener::wire(addr).await?;
        let this = Self { engine, listener };
        Ok((this, switch))
    }

    async fn listen(mut self) -> Result<(), Error> {
        while let Some(stream) = self.listener.next().await {
            let stream = match stream {
                Ok((stream, socket)) => {
                    info!("Accepted TCP stream from {}", socket);

                    let engine = self.engine.clone();
                    tokio::spawn(async move {
                        serve(engine, stream).await;
                    });
                }
                Err(e) => {
                    warn!("On accepting TCP streams: {}", e);
                    continue;
                }
            };
        }

        Ok(())
    }
}

fn persisting_flavor<P: AsRef<Path>>(path: P) -> Option<Flavor> {
    if LogKvsEngine::is_persistent(&path) {
        Some(Flavor::Kvs)
    } else if SledKvsEngine::is_persistent(&path) {
        Some(Flavor::Sled)
    } else {
        None
    }
}

/// Choose the correct given the command-line argument and the flavor of engine
/// persisting on the disk.
/// # Errors
///
/// When an engine flavor different to the flavor persisting on the disk is
/// specified, throws an [Error::EngineMismatch](Error::EngineMismatch).
pub fn choose_flavor(arg_flavor: Option<Flavor>) -> Result<Flavor, Error> {
    let cwd = std::env::current_dir().map_err(crate::Error::FS)?;

    match (arg_flavor, persisting_flavor(&cwd)) {
        (Some(a), Some(p)) if a != p => Err(Error::EngineMismatch),
        (arg, persistent) => Ok(arg.or(persistent).unwrap_or(Flavor::Kvs)),
    }
}

async fn stream_deserialize<'de, R, D>(mut reader: R) -> Result<D, Error>
where
    R: AsyncRead + Unpin,
    D: Deserialize<'de>,
{
    let len = u32::from_be(reader.read_u32().await?);
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;

    let mut de = serde_json::Deserializer::from_reader(Cursor::new(buf)).into_iter::<D>();

    match de.next().transpose()? {
        Some(item) => Ok(item),
        None => Err(Error::UnexpectedEOF),
    }
}

async fn write_response(stream: &mut TcpStream, response: &Response) -> Result<(), Error> {
    let buf = serde_json::to_vec(response)?;
    let len = (buf.len() as u32).to_be();

    // serde cannot handle WouldBlock, the serialized command must be first asynchronically read
    // into a local buffer, to decide how much bytes must be read each message has to be tagged
    // with a length.
    stream.write_u32(len).await?;
    stream.write_all(&buf).await?;

    Ok(())
}

async fn try_serve<E: KvsEngine>(engine: E, stream: &mut TcpStream) -> Result<(), Error> {
    let command = stream_deserialize(&mut *stream).await?;

    info!(
        "Received command from {:?}: {}",
        stream.peer_addr(),
        command
    );

    let response = match command {
        Command::Get(key) => {
            // still can't understand why
            // ```
            // let value = engine.get(key).await?;
            // ```
            // will capture a reference engine
            let get = engine.get(key);
            let value = get.await?;
            Response::Value(value)
        }
        Command::Set(key, value) => {
            let set = engine.set(key, value);
            set.await?;
            Response::Ok
        }
        Command::Remove(key) => {
            let remove = engine.remove(key);
            remove.await?;
            Response::Ok
        }
    };

    write_response(stream, &response).await?;
    info!("Sent response to {:?}: {}", stream.peer_addr(), response);
    Ok(())
}

async fn handle_errors(err: Error, stream: &mut TcpStream) {
    error!("On serving client via TCP: {}", err);
    // the stream is likely unusable on a net error
    if !matches!(err, Error::Net(..)) {
        let _ = write_response(stream, &Response::Err(err.to_string())).await;
    }
}

async fn serve<E: KvsEngine>(engine: E, mut stream: TcpStream) {
    if let Err(e) = try_serve(engine, &mut stream).await {
        handle_errors(e, &mut stream).await;
    }
}

/// Default listening address of the [KvsServer](KvsServer).
pub fn default_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 4000))
}
