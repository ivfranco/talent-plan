//! A persistent log-structured key-value store.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

/// Error handling functionalities for command line applications.
pub mod cmd;
/// A persistent key-value store server.
pub mod server;

/// A persistent key-value store client.
pub mod client;

/// An alternative persisten key-value store based on sled.
pub mod sled_engine;

/// Thread pool used to execute jobs in parallel on a fixed number of threads.
pub mod thread_pool;

/// A TcpListener that can be remotely shutdown without resorting to SIGTERM or
/// SIGKILL.
pub mod listener;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{Debug, Display},
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, PoisonError, RwLock,
    },
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
    Other(Box<dyn Display>),
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Error {
    // the remove command returns Result<()>, hence KeyNotFound must be an error
    // code, but unlike other errors it's not fatal and should not halt the
    // program.
    fn should_halt(&self) -> bool {
        !matches!(self, Error::KeyNotFound)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_err: PoisonError<T>) -> Self {
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
#[derive(Debug, Serialize, Deserialize)]
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

/// A persistent log-structured key-value store of pairs of Strings. Currently
/// commands are serialized to JSON for readability.
pub struct KvStore {
    writer: BufWriter<File>,
    indices: Indices,
    dir: PathBuf,
}

/// The on-disk file name of the store.
pub const STORE_NAME: &str = "0.kvs";
const BACKUP_NAME: &str = "1.kvs";

impl KvStore {
    /// Creates a handle to the on-disk key-value store.
    /// # Examples
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let mut store = KvStore::open(std::env::temp_dir());
    /// ```
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir: PathBuf = dir.as_ref().to_owned();
        if !dir.is_dir() {
            return Err(Error::NotDirectory(dir));
        }
        let path = dir.join(STORE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;

        file.seek(SeekFrom::Start(0))?;
        let indices = build_indices(&mut file)?;
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            writer: BufWriter::new(file),
            indices,
            dir,
        })
    }

    /// Insert a key-value pair into the store.
    /// # Examples
    ///
    /// ```
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(std::env::temp_dir())?;
    /// store.set("key".to_string(), "value".to_string())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.pos()?;
        self.append(&Command::Set(key.clone(), value))?;
        self.indices.insert(key, pos);
        self.compact()?;
        Ok(())
    }

    /// Returns a owned String value corresponding to the key.
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(std::env::temp_dir())?;
    /// store.set("key".to_string(), "value".to_string())?;
    /// assert_eq!(store.get("key".to_string())?, Some("value".to_string()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let value_pos = if let Some(pos) = self.indices.get(&key) {
            pos
        } else {
            return Ok(None);
        };

        self.read_value_from(value_pos).map(Some)
    }

    /// Removes a key from the store.
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(std::env::temp_dir())?;
    /// store.set("key".to_string(), "value".to_string())?;
    /// store.remove("key".to_string())?;
    /// assert_eq!(store.get("key".to_string())?, None);
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.indices.contains_key(&key) {
            Err(Error::KeyNotFound)
        } else {
            self.append(&Command::Remove(key.clone()))?;
            self.indices.remove(&key);
            self.compact()?;
            Ok(())
        }
    }

    fn pos(&mut self) -> Result<u64> {
        self.writer
            // skip the flush
            .get_mut()
            // this seek is safe because the write position is not actually
            // changed
            .seek(SeekFrom::Current(0))
            .map_err(From::from)
    }

    // Calls to KvStore::set and KvStore::remove, when the indice is in place,
    // should not force the BufWriter to be flushed (mainly by seek), as a
    // consequence other public methods must seek to the end of the file on
    // exit.
    fn append(&mut self, command: &Command) -> Result<()> {
        append(&mut self.writer, command)
    }

    fn read_value_from(&mut self, pos: u64) -> Result<String> {
        self.writer.flush()?;
        let value = match seek_read_from(self.writer.get_ref(), pos)? {
            Command::Set(_, value) => value,
            _ => return Err(Error::StoreFileCorrupted(pos, Corruption::HasNoValue)),
        };
        self.writer.seek(SeekFrom::End(0))?;
        Ok(value)
    }

    /// The compaction test always succeeds on x86_64-pc-windows-msvc, the first
    /// step towards a functioning compaction strategy is to make the test fail
    /// when it should.
    pub fn logs(&self) -> u32 {
        self.indices.logs()
    }

    fn compact(&mut self) -> Result<()> {
        self.writer.flush()?;

        if !self.indices.should_compact() {
            return Ok(());
        }
        let indices = mem::take(&mut self.indices);

        let dir = self.dir.clone();
        let mut backup = BufWriter::new(File::create(dir.join(BACKUP_NAME))?);

        for (key, pos) in indices.iter() {
            let value = self.read_value_from(pos)?;
            append(&mut backup, &Command::Set(key, value))?;
        }
        backup.flush()?;

        // otherwise renaming 1.kvs to 0.kvs will cause permission error
        self.writer = backup;

        std::fs::rename(dir.join(BACKUP_NAME), dir.join(STORE_NAME))?;
        *self = Self::open(dir)?;

        Ok(())
    }
}

fn append<W: Write>(writer: W, command: &Command) -> Result<()> {
    serde_json::to_writer(writer, command).map_err(|e| Error::FS(e.into()))?;
    Ok(())
}

#[derive(Default)]
struct Stats {
    // The number of logs on disk.
    logs: AtomicU32,
    // The number of values on disk.
    values: AtomicU32,
}

impl Stats {
    fn update(&self, is_overwrite: bool) {
        if !is_overwrite {
            self.values.fetch_add(1, Ordering::Release);
        }
        self.logs.fetch_add(1, Ordering::Release);
    }

    fn utilization(&self) -> f64 {
        self.values.load(Ordering::Acquire) as f64 / self.logs.load(Ordering::Acquire) as f64
    }

    fn should_compact(&self) -> bool {
        self.utilization() < 0.25 && self.logs.load(Ordering::Acquire) >= 100_000
    }
}

/// Indices into the on-disk store file.
#[derive(Default)]
struct Indices {
    inner: DashMap<String, u64>,
    stats: Stats,
}

impl Indices {
    fn new() -> Self {
        Self::default()
    }

    fn logs(&self) -> u32 {
        self.stats.logs.load(Ordering::Acquire)
    }

    fn insert(&self, key: String, pos: u64) {
        let is_overwrite = self.inner.insert(key, pos).is_some();
        self.stats.update(is_overwrite);
    }

    fn remove(&self, key: &str) {
        let is_overwrite = self.inner.remove(key).is_some();
        self.stats.update(is_overwrite);
    }

    fn get(&self, key: &str) -> Option<u64> {
        self.inner.get(key).map(|entry| *entry.value())
    }

    fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    fn should_compact(&self) -> bool {
        self.stats.should_compact()
    }

    fn iter(&self) -> impl Iterator<Item = (String, u64)> + '_ {
        self.inner
            .iter()
            .map(|entry| (entry.key().to_string(), *entry.value()))
    }
}

fn seek_read_from<R: Read + Seek>(mut reader: R, pos: u64) -> Result<Command> {
    reader.seek(SeekFrom::Start(pos))?;
    // The non-streaming deserializer will check if the character after the value is EOF or
    // whitespace and throw an error otherwise, there's no way to match against a specific ErrorCode
    // from serde_json (it's private), as a result serde_json::StreamDeserializer which skips this
    // check is the only way to read a JSON value from the middle of an input stream.
    let mut de = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

    match de.next().transpose()? {
        Some(command) => Ok(command),
        _ => Err(Error::StoreFileCorrupted(
            pos,
            Corruption::DeserializeFailure,
        )),
    }
}

fn build_indices<R: Read>(reader: R) -> Result<Indices> {
    let mut de = serde_json::Deserializer::from_reader(reader).into_iter();
    let indices = Indices::new();

    loop {
        let pos = de.byte_offset() as u64;
        match de.next().transpose()? {
            Some(Command::Set(key, _)) => {
                indices.insert(key, pos);
            }
            Some(Command::Remove(key)) => {
                indices.remove(&key);
            }
            Some(..) => {
                return Err(Error::StoreFileCorrupted(
                    pos,
                    Corruption::UnexpectedCommandInStorage,
                ));
            }
            None => {
                break;
            }
        }
    }

    Ok(indices)
}

/// A key-value store that supports Set, Get and Remove operations.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set or overwite a string key to a string value in the key-value store.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the value corresponding to a string key in the key-value store, return None if the key
    /// doesn't exist.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Delete a string key and the corresponding value from the key-value store. Return an error
    /// when the key doesn't exist.
    fn remove(&self, key: String) -> Result<()>;
}

/// Sharable [KvStore](KvStore).
pub struct LogKvsEngine {
    store: Arc<RwLock<KvStore>>,
}

impl LogKvsEngine {
    /// Creates a handle to the on-disk key-value store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore::open(path)?;
        Ok(Self {
            store: Arc::new(RwLock::new(store)),
        })
    }

    /// Exposed for tests.
    pub fn on_disk_size(&self) -> Result<u32> {
        Ok(self.store.read()?.logs())
    }
}

impl Clone for LogKvsEngine {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

impl KvsEngine for LogKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.store.write()?.set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.store.write()?.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.store.write()?.remove(key)
    }
}
