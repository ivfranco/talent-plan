use crate::{Command, Error as KvsError};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    io::Cursor,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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

/// Asynchronically deserialize a value with a length tag from a stream.
pub async fn read_tagged<'de, R, D>(mut reader: R) -> Result<D, Error>
where
    R: AsyncRead + Unpin,
    D: Deserialize<'de>,
{
    // tokio automatically perform read and write from a stream in big-endian order
    let len = reader.read_u32().await?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;

    let mut de = serde_json::Deserializer::from_reader(Cursor::new(buf)).into_iter::<D>();

    match de.next().transpose()? {
        Some(item) => Ok(item),
        None => Err(Error::UnexpectedEOF),
    }
}

/// Asynchronically write serialized value with a length tag to a stream.
pub async fn write_tagged<W, S>(mut writer: W, value: &S) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
    S: Serialize,
{
    let buf = serde_json::to_vec(value)?;
    // serde cannot handle WouldBlock, the serialized command must be first asynchronically read
    // into a local buffer, to decide how much bytes must be read each message has to be tagged
    // with a length.
    writer.write_u32(buf.len() as u32).await?;
    writer.write_all(&buf).await?;

    Ok(())
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
