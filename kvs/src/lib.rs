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

use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{Debug, Display},
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
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
    handle: BufWriter<File>,
    indices: Option<Indices>,
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

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            handle: BufWriter::new(file),
            indices: None,
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
        self.indices()?.insert(key, pos);
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
        let value_pos = if let Some(pos) = self.indices()?.get(&key) {
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
        if !self.indices()?.contains_key(&key) {
            Err(Error::KeyNotFound)
        } else {
            self.append(&Command::Remove(key.clone()))?;
            self.indices()?.remove(&key);
            self.compact()?;
            Ok(())
        }
    }

    fn pos(&mut self) -> Result<u64> {
        self.handle
            // skip the flush
            .get_mut()
            // this seek is safe because the write position is not actually
            // changed
            .seek(SeekFrom::Current(0))
            .map_err(From::from)
    }

    fn indices(&mut self) -> Result<&mut Indices> {
        if self.indices.is_none() {
            self.indices = Some(self.build_indices()?);
        }

        // the only way to express this until `Option::insert` is stabilized
        Ok(self.indices.as_mut().unwrap())
    }

    fn build_indices(&mut self) -> Result<Indices> {
        // BufWriter::seek always flushes the internal buffer
        self.handle.seek(SeekFrom::Start(0))?;

        let file = self.handle.get_mut();
        let mut de = serde_json::Deserializer::from_reader(file).into_iter();
        let mut indices = Indices::new();

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

    // Calls to KvStore::set and KvStore::remove, when the indice is in place,
    // should not force the BufWriter to be flushed (mainly by seek), as a
    // consequence other public methods must seek to the end of the file on
    // exit.
    fn append(&mut self, command: &Command) -> Result<()> {
        append(&mut self.handle, command)
    }

    fn read_value_from(&mut self, pos: u64) -> Result<String> {
        self.handle.flush()?;
        let value = match seek_read_from(self.handle.get_ref(), pos)? {
            Command::Set(_, value) => value,
            _ => return Err(Error::StoreFileCorrupted(pos, Corruption::HasNoValue)),
        };
        self.handle.seek(SeekFrom::End(0))?;
        Ok(value)
    }

    /// The compaction test always succeeds on x86_64-pc-windows-msvc, the first
    /// step towards a functioning compaction strategy is to make the test fail
    /// when it should.
    pub fn on_disk_size(&mut self) -> Result<u64> {
        let file = self.handle.get_mut();
        let start = file.seek(SeekFrom::Start(0))?;
        file.seek(SeekFrom::End(0))
            .map(|end| end - start)
            .map_err(From::from)
    }

    fn compact(&mut self) -> Result<()> {
        self.handle.flush()?;

        if !self.indices()?.should_compact() {
            return Ok(());
        }
        let indices = mem::take(self.indices()?);

        let dir = self.dir.clone();
        let mut backup = BufWriter::new(File::create(dir.join(BACKUP_NAME))?);

        for (key, pos) in indices.into_iter() {
            let value = self.read_value_from(pos)?;
            append(&mut backup, &Command::Set(key, value))?;
        }
        backup.flush()?;

        // otherwise renaming 1.kvs to 0.kvs will cause permission error
        self.handle = backup;

        std::fs::rename(dir.join(BACKUP_NAME), dir.join(STORE_NAME))?;
        *self = Self::open(dir)?;

        Ok(())
    }
}

fn append<W: Write>(writer: W, command: &Command) -> Result<()> {
    serde_json::to_writer(writer, command).map_err(|e| Error::FS(e.into()))?;
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Stats {
    // The number of logs on disk.
    logs: u32,
    // The number of values on disk.
    values: u32,
}

impl Stats {
    fn update(&mut self, is_overwrite: bool) {
        if !is_overwrite {
            self.values += 1;
        }
        self.logs += 1;
    }

    fn utilization(&self) -> f64 {
        self.values as f64 / self.logs as f64
    }

    fn should_compact(&self) -> bool {
        self.utilization() < 0.25 && self.logs >= 100_000
    }
}

type Revision = u32;

#[derive(Default)]
struct Indices {
    // TODO: make use of revisions.
    inner: HashMap<String, (Option<u64>, Revision)>,
    stats: Stats,
}

impl Indices {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, key: String, pos: u64) {
        match self.inner.entry(key) {
            Entry::Occupied(mut e) => {
                let (_, rev) = *e.get();
                e.insert((Some(pos), rev + 1));
                self.stats.update(true);
            }
            Entry::Vacant(e) => {
                e.insert((Some(pos), 0));
                self.stats.update(false);
            }
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some((pos, rev)) = self.inner.get_mut(key) {
            *pos = None;
            *rev += 1;
            self.stats.update(true);
        }
    }

    fn get(&self, key: &str) -> Option<u64> {
        self.inner.get(key).and_then(|(pos, _)| *pos)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    fn should_compact(&self) -> bool {
        self.stats.should_compact()
    }

    fn into_iter(self) -> impl Iterator<Item = (String, u64)> {
        self.inner
            .into_iter()
            .filter_map(|(key, (pos, _))| pos.map(|pos| (key, pos)))
    }
}

fn seek_read_from<R: Read + Seek>(mut reader: R, pos: u64) -> Result<Command> {
    reader.seek(SeekFrom::Start(pos))?;
    // The non-streaming deserializer will check if the character after the
    // value is EOF or whitespace and throw an error otherwise, there's no
    // way to match against a specific ErrorCode from serde_json (it's
    // private), as a result serde_json::StreamDeserializer which skips this
    // check is the only way to read a JSON value from the middle of an input
    // stream.
    let mut de = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

    match de.next().transpose()? {
        Some(command) => Ok(command),
        _ => Err(Error::StoreFileCorrupted(
            pos,
            Corruption::DeserializeFailure,
        )),
    }
}

/// A key-value store that supports Set, Get and Remove operations.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set or overwite a string key to a string value in the key-value store.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the value corresponding to a string key in the key-value store,
    /// return None if the key doesn't exist.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Delete a string key and the corresponding value from the key-value
    /// store. Return an error when the key doesn't exist.
    fn remove(&self, key: String) -> Result<()>;
}

/// Sharable [KvStore](KvStore).
pub struct LogKvsEngine {
    store: Arc<Mutex<KvStore>>,
}

impl LogKvsEngine {
    /// Creates a handle to the on-disk key-value store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore::open(path)?;
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
        })
    }

    /// Exposed for tests.
    pub fn on_disk_size(&self) -> Result<u64> {
        self.store.lock()?.on_disk_size()
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
        self.store.lock()?.set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.store.lock()?.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.store.lock()?.remove(key)
    }
}
