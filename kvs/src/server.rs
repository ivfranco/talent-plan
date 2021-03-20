use crate::{
    listener::{Listener, ShutdownSwitch},
    log_engine::LogKvsEngine,
    protocol::{read_tagged, write_tagged, Error, Response},
    sled_engine::SledKvsEngine,
    Command, KvsEngine,
};
use futures::StreamExt;
use std::{fmt::Debug, net::SocketAddr, path::Path, str::FromStr};
use tokio::net::TcpStream;

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

/// A persistent key-value store server.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
    listener: Listener,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Bind the server with the provided key-value store engine and address.
    pub async fn bind(engine: E, addr: SocketAddr) -> Result<(Self, ShutdownSwitch), Error> {
        let (listener, switch) = Listener::wire(addr).await?;
        let this = Self { engine, listener };

        info!("Listening at address {}", addr);

        Ok((this, switch))
    }

    /// Start listening to incoming requests via TCP.
    pub async fn listen(mut self) -> Result<(), Error> {
        while let Some(stream) = self.listener.next().await {
            match stream {
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

async fn try_serve<E: KvsEngine>(engine: E, stream: &mut TcpStream) -> Result<(), Error> {
    let command = read_tagged(&mut *stream).await?;

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

    write_tagged(&mut *stream, &response).await?;
    info!("Sent response to {:?}: {}", stream.peer_addr(), response);
    Ok(())
}

async fn handle_errors(err: Error, stream: &mut TcpStream) {
    error!("On serving client via TCP: {}", err);
    // the stream is likely unusable on a net error
    if !matches!(err, Error::Net(..)) {
        let _ = write_tagged(stream, &Response::Err(err.to_string())).await;
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[tokio::test]
    async fn command_deserialize() -> Result<(), Error> {
        let command = Command::Set("key".to_string(), "value".to_string());
        let command_buf = serde_json::to_vec(&command)?;
        let mut buf = vec![];
        buf.extend_from_slice(&(command_buf.len() as u32).to_be_bytes());
        buf.extend_from_slice(&command_buf);

        let de = read_tagged(Cursor::new(buf)).await?;

        assert_eq!(command, de);

        Ok(())
    }
}
