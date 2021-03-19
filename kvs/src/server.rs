use crate::{
    listener::{Listener, ServerOrder, ShutdownSwitch},
    sled_engine::SLED_STORE_DIR,
    thread_pool::ThreadPool,
    Command, Error as KvsError, KvsEngine, STORE_NAME,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    io::{Error as IOError, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    str::FromStr,
    sync::mpsc::{channel, Receiver},
    thread,
};
use thiserror::Error;

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
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E, P> KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    /// Start the server with the provided key-value store engine and thread pool.
    pub fn open(engine: E, pool: P) -> Self {
        Self { engine, pool }
    }

    /// Spawn the server on another thread, return the remote shutdown switch.
    pub fn spawn(self, addr: Option<SocketAddr>) -> ShutdownSwitch {
        let addr = addr.unwrap_or_else(default_addr);
        info!("Initializing at {}...", addr);

        let (order_tx, order_rx) = channel();
        let switch = Listener::spawn(addr, order_tx);

        thread::spawn(move || self.listen(order_rx));

        switch
    }

    fn listen(self, order_rx: Receiver<ServerOrder>) {
        for order in order_rx {
            match order {
                ServerOrder::Stream(stream) => {
                    info!("Accepted connection from {:?}", stream.peer_addr());
                    self.serve(stream);
                }
                ServerOrder::Shutdown => {
                    info!("Received shutdown order");
                    break;
                }
            }
        }
    }

    /// Start accepting and serving incoming requests from clients on the
    /// address on the current thread.
    pub fn listen_on_current(&self, addr: Option<SocketAddr>) -> Result<(), Error> {
        let addr = addr.unwrap_or_else(default_addr);
        info!("Listening at {}...", addr);

        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            let stream = match stream {
                Ok(stream) => {
                    info!("Accepted connection from {:?}", stream.peer_addr());
                    stream
                }
                Err(err) => {
                    error!("On accepting TCP connection: {}", err);
                    continue;
                }
            };

            self.serve(stream);
        }

        Ok(())
    }

    fn serve(&self, stream: TcpStream) {
        let engine = self.engine.clone();
        self.pool.spawn(move || serve(engine, stream));
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

fn stream_deserialize<'de, R, D>(reader: R) -> Result<D, Error>
where
    R: Read,
    D: Deserialize<'de>,
{
    let mut de = serde_json::Deserializer::from_reader(reader).into_iter::<D>();

    match de.next().transpose()? {
        Some(d) => Ok(d),
        None => Err(Error::UnexpectedEOF),
    }
}

fn write_response(mut stream: &TcpStream, response: &Response) -> Result<(), Error> {
    serde_json::to_writer(stream, response).map_err(IOError::from)?;
    stream.flush()?;
    Ok(())
}

fn try_serve<E: KvsEngine>(engine: E, stream: &TcpStream) -> Result<(), Error> {
    let command = stream_deserialize(stream)?;

    info!(
        "Received command from {:?}: {}",
        stream.peer_addr(),
        command
    );

    let response = match command {
        Command::Get(key) => {
            let value = engine.get(key)?;
            Response::Value(value)
        }
        Command::Set(key, value) => {
            engine.set(key, value)?;
            Response::Ok
        }
        Command::Remove(key) => {
            engine.remove(key)?;
            Response::Ok
        }
    };

    write_response(stream, &response)?;
    info!("Sent response to {:?}: {}", stream.peer_addr(), response);
    Ok(())
}

fn handle_errors(err: Error, stream: &TcpStream) {
    error!("On serving client via TCP: {}", err);
    // the stream is likely unusable on a net error
    if !matches!(err, Error::Net(..)) {
        let _ = write_response(stream, &Response::Err(err.to_string()));
    }
}

fn serve<E: KvsEngine>(engine: E, stream: TcpStream) {
    if let Err(e) = try_serve(engine, &stream) {
        handle_errors(e, &stream);
    }
}

/// Default listening address of the [KvsServer](KvsServer).
pub fn default_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 4000))
}
