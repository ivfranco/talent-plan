//! A persistent log-structured key-value store.

#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
};
use thiserror::Error;

/// This type represents all possible errors that can occur when accessing the
/// KvStore. Wraps std::io::Error and serde_json error.
#[derive(Error, Debug)]
pub enum Error {
    /// IO errors thrown by the file system underneath.
    #[error("File system error: {0}")]
    FS(#[from] std::io::Error),

    /// JSON [de]serialization errors.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Removing a non-exist key.
    #[error("Key not found")]
    KeyNotFound,

    /// Open path is not a directory.
    #[error("Open path is not a directory: {0}")]
    NotDirectory(PathBuf),

    /// on-disk store file is corrupted / out of sync with the in-memory indices.
    #[error("Store file is corrupted around {0}")]
    StoreFileCorrupted(u64),
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Remove(String),
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

const STORE_NAME: &str = "0.kvs";
const BACKUP_NAME: &str = "1.kvs";

impl KvStore {
    /// Creates a handle to the on-disk key-value store.
    /// # Examples
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let mut store = KvStore::open(std::env::temp_dir());
    /// ```
    pub fn open<P: Into<PathBuf>>(dir: P) -> Result<Self> {
        let dir: PathBuf = dir.into();
        if !dir.is_dir() {
            return Err(Error::NotDirectory(dir));
        }
        let path = dir.join(STORE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
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

        match self.read_from(value_pos)? {
            Command::Set(_, value) => Ok(Some(value)),
            _ => Err(Error::StoreFileCorrupted(value_pos)),
        }
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

    fn read_from(&mut self, pos: u64) -> Result<Command> {
        self.handle.seek(SeekFrom::Start(pos))?;
        let mut file = self.handle.get_mut();
        // The non-streaming deserializer will check if the character after the
        // value is EOF or whitespace and throw an error otherwise, there's no
        // way to match against a specific ErrorCode from serde_json (it's
        // private), as a result serde_json::StreamDeserializer which skips this
        // check is the only way to read a JSON value from the middle of an input
        // stream
        let mut de = serde_json::Deserializer::from_reader(&mut file).into_iter::<Command>();

        let command = match de.next().transpose()? {
            Some(command) => command,
            _ => return Err(Error::StoreFileCorrupted(pos)),
        };

        file.seek(SeekFrom::End(0))?;
        Ok(command)
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
        Ok(())
    }
}

fn append<W: Write>(writer: W, command: &Command) -> Result<()> {
    serde_json::to_writer(writer, command)?;
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Stats {
    logs: u32,
    values: u32,
}

impl Stats {
    fn new() -> Self {
        Self::default()
    }

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
}
