//! A persistent log-structured key-value store.

#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom},
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

    /// A value index points to a remove command.
    #[error("Index {0} does not point to a command")]
    InvalidPointer(u64),

    /// Deserialized command does not contain a value.
    #[error("Deserialized command does not contain a value")]
    NonValueCommand,
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
    indices: Option<HashMap<String, u64>>,
}

const STORE_NAME: &str = "0.kvs";
const BACKUP_NAME: &str = "1.kvs";

impl KvStore {
    /// Creates a handle to the on-disk key-value store.
    /// # Examples
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let mut store = KvStore::open(".");
    /// ```
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let mut path: PathBuf = path.into();
        if !path.is_dir() {
            return Err(Error::NotDirectory(path));
        }
        path = path.join(STORE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&path)?;

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            handle: BufWriter::new(file),
            indices: None,
        })
    }

    /// Insert a key-value pair into the store.
    /// # Examples
    ///
    /// ```
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(".")?;
    /// store.set("key".to_string(), "value".to_string())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.pos()?;
        self.append(&Command::Set(key.clone(), value))?;
        self.indices()?.insert(key, pos);
        Ok(())
    }

    /// Returns a owned String value corresponding to the key.
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(".")?;
    /// store.set("key".to_string(), "value".to_string())?;
    /// assert_eq!(store.get("key".to_string())?, Some("value".to_string()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let value_pos = if let Some(pos) = self.indices()?.get(&key) {
            *pos
        } else {
            return Ok(None);
        };

        match self.read_from(value_pos)? {
            Command::Set(_, value) => Ok(Some(value)),
            _ => Err(Error::NonValueCommand),
        }
    }

    /// Removes a key from the store.
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::{Result, KvStore};
    /// # fn main() -> Result<()> {
    /// let mut store = KvStore::open(".")?;
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

    fn indices(&mut self) -> Result<&mut HashMap<String, u64>> {
        if self.indices.is_none() {
            self.indices = Some(self.build_indices()?);
        }

        // the only way to express this until `Option::insert` is stabilized
        Ok(self.indices.as_mut().unwrap())
    }

    fn build_indices(&mut self) -> Result<HashMap<String, u64>> {
        // BufWriter::seek always flushes the internal buffer
        self.handle.seek(SeekFrom::Start(0))?;

        let file = self.handle.get_mut();
        let mut de = serde_json::Deserializer::from_reader(file).into_iter();
        let mut indices: HashMap<String, u64> = HashMap::new();

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
        serde_json::to_writer(&mut self.handle, command)?;
        Ok(())
    }

    fn read_from(&mut self, pos: u64) -> Result<Command> {
        self.handle.seek(SeekFrom::Start(pos))?;
        let mut file = self.handle.get_mut();
        // The non-streaming deserializer will check if the character after the
        // value is EOF or whitespace and throw an error otherwise, there's no
        // way to match against a specific ErrorCode from serde_json (it's
        // private), as a result serde_json::StreamDeserializer which skips this
        // check is the only way to read a JSON value from middle of an input
        // stream.
        let mut de = serde_json::Deserializer::from_reader(&mut file).into_iter::<Command>();

        let command = match de.next().transpose()? {
            Some(command) => command,
            _ => return Err(Error::InvalidPointer(pos)),
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
        Ok(file.seek(SeekFrom::End(0))? - start)
    }
}
