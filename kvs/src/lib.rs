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
    fmt::{Debug, Display},
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard,
    },
};
use tempfile::tempfile;
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
    stats: Stats,
    reader: RwLock<File>,
    writer: Mutex<PBufWriter<File>>,
    indices: DashMap<String, Span>,
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
        info!("Initiating KvStore under directory {:?}", dir);

        if !dir.is_dir() {
            return Err(Error::NotDirectory(dir));
        }

        let (writer, mut reader) = open_file(&dir)?;
        let stats = Stats::default();
        let indices = build_indices(&mut reader, &stats)?;

        Ok(Self {
            stats,
            reader: RwLock::new(reader),
            writer: Mutex::new(writer),
            indices,
            dir,
        })
    }

    fn to_reader(&self) -> Result<KvsReader> {
        if self.stats.buf_len() > 0 {
            // buffered data must be first flushed to the disk
            self.to_writer()?.flush()?;
        }

        let reader = KvsReader {
            reader: self.reader.read()?,
            indices: &self.indices,
        };

        Ok(reader)
    }

    fn to_writer(&self) -> Result<KvsWriter> {
        Ok(KvsWriter {
            stats: &self.stats,
            writer: self.writer.lock()?,
            indices: &self.indices,
        })
    }

    /// The compaction test always succeeds on x86_64-pc-windows-msvc, the first step towards a
    /// functioning compaction strategy is to make the test fail when it should.
    pub fn logs(&self) -> u32 {
        self.stats.logs()
    }

    fn should_compact(&self) -> bool {
        self.stats.should_compact()
    }

    fn compact(&self) -> Result<()> {
        info!("Compaction triggered");

        self.to_writer()?.flush()?;

        // lock both reader and writer, compact must have exclusive control over the store
        let mut reader = self.reader.write()?;
        let mut writer = self.writer.lock()?;

        // clear the indices, all values in it will be invalid after compaction
        let indices = self.indices.clone();
        self.indices.clear();

        // write only non-overwritten Get commands to the new file
        let mut backup = BufWriter::new(File::create(self.dir.join(BACKUP_NAME))?);
        for (key, span) in indices {
            let value = read_value_from(&reader, span)?;
            append(&mut backup, &Command::Set(key, value))?;
        }
        backup.flush()?;

        // drop the old file descriptors, otherwise renaming stores will cause permission error.
        let temp = tempfile()?;
        *reader = temp.try_clone()?;
        *writer = PBufWriter::new(temp, 0);

        std::fs::rename(self.dir.join(BACKUP_NAME), self.dir.join(STORE_NAME))?;

        // rebuild reader and writer
        let (write_fs, read_fs) = open_file(&self.dir)?;
        *reader = read_fs;
        *writer = write_fs;

        // rebuild indices and stats
        self.stats.clear();
        let entries = build_indices(&mut *reader, &self.stats)?;
        for (key, value) in entries {
            self.indices.insert(key, value);
        }

        // sanity check
        assert!(1.0 - self.stats.utilization() < 1e-10);
        assert_eq!(self.indices.len(), self.logs() as usize);

        Ok(())
    }
}

fn open_file(dir: &Path) -> Result<(PBufWriter<File>, File)> {
    let path = dir.join(STORE_NAME);

    let mut write_fd = OpenOptions::new().create(true).append(true).open(&path)?;
    let end = write_fd.seek(SeekFrom::End(0))?;

    let read_fd = File::open(path)?;

    Ok((PBufWriter::new(write_fd, end), read_fd))
}

#[derive(Debug, Clone, Copy)]
struct Span {
    pos: u64,
    len: u64,
}

fn read_span(file: &File, span: Span) -> Result<Command> {
    info!("Reading span: {:?}", span);

    let Span { pos, len } = span;
    let mut buf = vec![0u8; len as usize];
    read_exact_at(file, &mut buf, pos)?;

    match serde_json::from_reader(buf.as_slice())? {
        Some(command) => Ok(command),
        _ => Err(Error::StoreFileCorrupted(
            pos,
            Corruption::DeserializeFailure,
        )),
    }
}

#[cfg(any(target_os = "windows", target_os = "linux"))]
fn read_exact_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<()> {
    #[cfg(target_os = "windows")]
    fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
        use std::os::windows::fs::FileExt;
        file.seek_read(buf, pos)
    }

    #[cfg(target_os = "linux")]
    fn read_at(file: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, pos)
    }

    let mut amt = 0;
    while amt < buf.len() {
        // dbg!(amt);
        amt += read_at(file, &mut buf[amt..], pos + amt as u64)?;
    }
    Ok(())
}

struct KvsReader<'a> {
    indices: &'a Indices,
    reader: RwLockReadGuard<'a, File>,
}

impl<'a> KvsReader<'a> {
    /// Returns a owned String value corresponding to the key.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let span = if let Some(entry) = self.indices.get(&key) {
            *entry.value()
        } else {
            return Ok(None);
        };

        read_value_from(&self.reader, span).map(Some)
    }
}

fn read_value_from(file: &File, span: Span) -> Result<String> {
    let value = match read_span(file, span)? {
        Command::Set(_, value) => value,
        _ => return Err(Error::StoreFileCorrupted(span.pos, Corruption::HasNoValue)),
    };

    Ok(value)
}

struct KvsWriter<'a> {
    stats: &'a Stats,
    writer: MutexGuard<'a, PBufWriter<File>>,
    indices: &'a Indices,
}

impl<'a> KvsWriter<'a> {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let span = self.append(&Command::Set(key.clone(), value))?;

        let is_overwrite = self.indices.insert(key, span).is_some();
        self.update_stats(is_overwrite);

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if !self.indices.contains_key(&key) {
            Err(Error::KeyNotFound)
        } else {
            self.indices.remove(&key);
            self.append(&Command::Remove(key))?;

            self.update_stats(true);

            Ok(())
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.set_buf_len();
        assert_eq!(self.stats.buf_len(), 0);
        Ok(())
    }

    fn append(&mut self, command: &Command) -> Result<Span> {
        let pos = self.writer.pos();
        append(&mut *self.writer, command)?;
        let len = self.writer.pos() - pos;
        Ok(Span { pos, len })
    }

    fn update_stats(&self, is_overwite: bool) {
        self.stats.update(is_overwite);
        self.set_buf_len();
    }

    fn set_buf_len(&self) {
        self.stats.set_buf_len(self.writer.buf_len());
    }
}

impl<'a> Drop for KvsWriter<'a> {
    fn drop(&mut self) {
        // The same problem as the sled engine, `Child::kill` will skip `Drop` implements on
        // KvsStore, hence `flush` must be called here.
        let _ = self.flush();
    }
}

struct PBufWriter<W: Write> {
    inner: BufWriter<W>,
    pos: u64,
}

impl<W: Write> PBufWriter<W> {
    fn new(writer: W, pos: u64) -> Self {
        Self {
            inner: BufWriter::new(writer),
            pos,
        }
    }

    fn pos(&self) -> u64 {
        self.pos
    }

    fn buf_len(&self) -> usize {
        self.inner.buffer().len()
    }
}

impl<W: Write> Write for PBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.pos += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
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
    // bytes in the write buffer.
    buf_len: AtomicUsize,
}

impl Stats {
    fn update(&self, is_overwrite: bool) {
        if !is_overwrite {
            self.values.fetch_add(1, Ordering::AcqRel);
        }
        self.logs.fetch_add(1, Ordering::AcqRel);
    }

    fn clear(&self) {
        self.logs.store(0, Ordering::SeqCst);
        self.values.store(0, Ordering::SeqCst);
        self.buf_len.store(0, Ordering::SeqCst);
    }

    fn logs(&self) -> u32 {
        self.logs.load(Ordering::Acquire)
    }

    fn values(&self) -> u32 {
        self.values.load(Ordering::Acquire)
    }

    fn utilization(&self) -> f64 {
        self.values() as f64 / self.logs() as f64
    }

    fn should_compact(&self) -> bool {
        self.utilization() < 0.4 && self.logs() >= 100_000
    }

    fn set_buf_len(&self, buf_len: usize) {
        self.buf_len.store(buf_len, Ordering::Release);
    }

    fn buf_len(&self) -> usize {
        self.buf_len.load(Ordering::Acquire)
    }
}

type Indices = DashMap<String, Span>;

fn build_indices<R: Read>(reader: R, stats: &Stats) -> Result<Indices> {
    let mut de = serde_json::Deserializer::from_reader(BufReader::new(reader)).into_iter();
    let indices = DashMap::new();

    loop {
        let pos = de.byte_offset() as u64;
        match de.next().transpose()? {
            Some(Command::Set(key, _)) => {
                let len = de.byte_offset() as u64 - pos;
                let is_overwrite = indices.insert(key, Span { pos, len }).is_some();
                stats.update(is_overwrite);
            }
            Some(Command::Remove(key)) => {
                indices.remove(&key);
                stats.update(true);
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
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, LogKvsEngine, Result};
    /// # fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string())?;
    /// # Ok(())
    /// # }
    /// ```
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get the value corresponding to a string key in the key-value store, return None if the key
    /// doesn't exist.
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, LogKvsEngine, Result};
    /// # fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string())?;
    /// assert_eq!(engine.get("key".to_string())?, Some("value".to_string()));
    /// assert_eq!(engine.get("none".to_string())?, None);
    /// # Ok(())
    /// # }
    /// ```
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Delete a string key and the corresponding value from the key-value store. Return an error
    /// when the key doesn't exist.
    /// #Example
    ///
    /// ```rust
    /// # use kvs::{KvsEngine, LogKvsEngine, Result};
    /// # fn main() -> Result<()> {
    /// let engine = LogKvsEngine::open(tempfile::tempdir()?)?;
    /// engine.set("key".to_string(), "value".to_string())?;
    /// assert_eq!(engine.get("key".to_string())?, Some("value".to_string()));
    /// engine.remove("key".to_string())?;
    /// assert_eq!(engine.get("key".to_string())?, None);
    /// assert!(engine.remove("key".to_string()).is_err());
    /// # Ok(())
    /// # }
    /// ```
    fn remove(&self, key: String) -> Result<()>;
}

/// Sharable [KvStore](KvStore).
pub struct LogKvsEngine {
    store: Arc<KvStore>,
}

impl LogKvsEngine {
    /// Creates a handle to the on-disk key-value store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore::open(path)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Exposed for tests.
    pub fn on_disk_size(&self) -> u32 {
        self.store.logs()
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
        self.store.to_writer()?.set(key, value)?;
        if self.store.should_compact() {
            self.store.compact()?;
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.store.to_reader()?.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.store.to_writer()?.remove(key)?;
        if self.store.should_compact() {
            self.store.compact()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempfile;

    #[cfg(any(target_os = "windows", target_os = "linux"))]
    #[test]
    fn pread_exact() {
        let mut file = tempfile().unwrap();
        file.write_all(b"Hello, world!").unwrap();

        let mut buf = [0u8; 5];
        read_exact_at(&file, &mut buf, 7).unwrap();

        assert_eq!(&buf, b"world");
    }
}
