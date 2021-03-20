//! A persistent log-structured key-value store.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

/// Error handling functionalities for command line applications.
pub mod cmd;

/// Protocol for server-client communication.
pub(crate) mod protocol;

/// A server hosting a persistent key-value store.
pub mod server;

/// A client to a persistent key-value store.
pub mod client;

/// A log-structured implementation of key-value store.
pub mod log_engine;

/// An alternative persisten key-value store based on sled.
pub mod sled_engine;

/// A TcpListener that can be remotely shutdown without resorting to SIGTERM or
/// SIGKILL.
pub mod listener;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    path::PathBuf,
    sync::PoisonError,
};
use thiserror::Error;

/// This type represents all possible errors that can occur when accessing the
/// key-value store engines. Wraps std::io::Error serde_json error and
/// sled::Error.
#[derive(Error)]
pub enum Error {
    /// IO errors thrown by the file system underneath.
    #[error("File system error: {0}")]
    FS(#[from] std::io::Error),

    /// Json [de]serialization errors.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Removing a non-exist key.
    #[error("Key not found")]
    KeyNotFound,

    /// Open path is not a directory.
    #[error("Open path is not a directory: {0}")]
    NotDirectory(PathBuf),

    /// On-disk store file is corrupted / out of sync with the in-memory indices.
    #[error("Store file is corrupted around {0}, {1:?}")]
    StoreFileCorrupted(u64, Corruption),

    /// Sled errors.
    #[error("Sled Error: {0}")]
    Sled(#[from] sled::Error),

    /// Failed to spawn the given number of threads on construction.
    #[error("Failed to spawn threads on construction")]
    FailedToSpawn,

    /// Other general unrecoverable errors.
    #[error("{0}")]
    Other(Box<dyn Display + Send + Sync>),
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Error {
    // the remove command returns Result<()>, hence KeyNotFound must be an error code, but unlike
    // other errors it's not fatal and should not halt the program.
    fn should_halt(&self) -> bool {
        !matches!(self, Error::KeyNotFound)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error::Other(Box::new("Lock poisoned"))
    }
}

impl From<rayon_core::ThreadPoolBuildError> for Error {
    fn from(err: rayon_core::ThreadPoolBuildError) -> Self {
        Error::Other(Box::new(err))
    }
}

/// Possible kinds of store corruption.
#[derive(Debug)]
pub enum Corruption {
    /// Bytes cannot be deserialized to a command.
    DeserializeFailure,
    /// Get command found in on-disk store.
    UnexpectedCommandInStorage,
    /// Deserialized command does not contain a value.
    HasNoValue,
}

/// Command used both in on-disk store and network protocol.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Command {
    /// Set the value of a string key to a string value.
    Set(String, String),
    /// Remove a given key.
    Remove(String),
    /// *network protocol only* Get the string value of a given string key.
    Get(String),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

/// Alias for a `Result` with the error type `kvs::Error`.
pub type Result<T> = std::result::Result<T, Error>;

/// A key-value store that supports Set, Get and Remove operations.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set or overwite a string key to a string value in the key-value store.
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, log_engine::LogKvsEngine, Result};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    fn set(&self, key: String, value: String) -> BoxFuture<Result<()>>;

    /// Get the value corresponding to a string key in the key-value store, return None if the key
    /// doesn't exist.
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, log_engine::LogKvsEngine, Result};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string()).await?;
    /// assert_eq!(engine.get("key".to_string()).await?, Some("value".to_string()));
    /// assert_eq!(engine.get("none".to_string()).await?, None);
    /// # Ok(())
    /// # }
    /// ```
    fn get(&self, key: String) -> BoxFuture<Result<Option<String>>>;

    /// Delete a string key and the corresponding value from the key-value store. Return an error
    /// when the key doesn't exist.
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, log_engine::LogKvsEngine, Result};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string()).await?;
    /// assert_eq!(engine.get("key".to_string()).await?, Some("value".to_string()));
    /// engine.remove("key".to_string()).await?;
    /// assert_eq!(engine.get("key".to_string()).await?, None);
    /// assert!(engine.remove("key".to_string()).await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    fn remove(&self, key: String) -> BoxFuture<Result<()>>;
}
