use crate::{Command, Corruption, Error, KvsEngine, Result};
use dashmap::{lock::RwLockWriteGuard, DashMap};
use futures::{future::BoxFuture, FutureExt};
use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard,
    },
};
use tempfile::tempfile;

const STORE_EXT: &str = "kvs";

const MAX_SIZE: u64 = 1024 * 1024 /* one megabyte */;

fn store_file_name(epoch: Epoch) -> String {
    format!("{}.{}", epoch, STORE_EXT)
}

/// commands are serialized to JSON for readability.
struct KvStore {
    stats: Stats,
    reader: RwLock<Vec<File>>,
    writer: Mutex<PBufWriter<File>>,
    indices: DashMap<String, Span>,
    dir: PathBuf,
}

impl KvStore {
    fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir: PathBuf = dir.as_ref().to_owned();
        info!("Initiating KvStore under directory {:?}", dir);

        if !dir.is_dir() {
            return Err(Error::NotDirectory(dir));
        }

        let stats = Stats::default();
        let indices = Indices::new();
        let mut epoch = 0;
        let mut reader = vec![];

        // update the indices and stats with all existing store files
        loop {
            let path = dir.join(store_file_name(epoch));
            if !path.is_file() {
                break;
            }
            let file = File::open(path)?;

            update_indices(&file, &indices, epoch, &stats)?;
            reader.push(file);

            epoch += 1;
        }

        let writer = {
            let last_epoch = reader.len().saturating_sub(1);
            open_file(&dir, last_epoch)?
        };

        if reader.is_empty() {
            reader.push(File::open(dir.join(store_file_name(0)))?);
        }

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
            epoch: self.reader.read()?.len() - 1,
            stats: &self.stats,
            writer: self.writer.lock()?,
            indices: &self.indices,
        })
    }

    /// The compaction test always succeeds on x86_64-pc-windows-msvc, the first step towards a
    /// functioning compaction strategy is to make the test fail when it should.
    fn logs(&self) -> u32 {
        self.stats.logs()
    }

    fn advance_epoch(&self) -> Result<()> {
        let mut reader = self.reader.write()?;
        let mut writer = self.writer.lock()?;

        writer.flush()?;

        let new_epoch = reader.len();
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(self.dir.join(store_file_name(new_epoch)))?;
        let pos = file.stream_position()?;

        *writer = PBufWriter::new(file, pos);

        reader.push(File::open(self.dir.join(store_file_name(new_epoch)))?);

        Ok(())
    }

    fn should_compact(&self) -> bool {
        self.stats.should_compact()
    }

    fn compact(&self) -> Result<()> {
        // info!("Compaction triggered");

        // self.to_writer()?.flush()?;

        // // lock both reader and writer, compact must have exclusive control over the store
        // let mut reader = self.reader.write()?;
        // let mut writer = self.writer.lock()?;

        // // clear the indices, all values in it will be invalid after compaction
        // let indices = self.indices.clone();
        // self.indices.clear();

        // // write only non-overwritten Get commands to the new file
        // let mut backup = BufWriter::new(File::create(self.dir.join(BACKUP_NAME))?);
        // for (key, span) in indices {
        //     let value = read_value_from(&reader, span)?;
        //     append(&mut backup, &Command::Set(key, value))?;
        // }
        // backup.flush()?;

        // // drop the old file descriptors, otherwise renaming stores will cause permission error.
        // let temp = tempfile()?;
        // *reader = temp.try_clone()?;
        // *writer = PBufWriter::new(temp, 0);

        // std::fs::rename(self.dir.join(BACKUP_NAME), self.dir.join(STORE_NAME))?;

        // // rebuild reader and writer
        // let (write_fs, read_fs) = open_file(&self.dir)?;
        // *reader = read_fs;
        // *writer = write_fs;

        // // rebuild indices and stats
        // self.stats.clear();
        // let entries = build_indices(&mut *reader, &self.stats)?;
        // for (key, value) in entries {
        //     self.indices.insert(key, value);
        // }

        // // sanity check
        // assert!(1.0 - self.stats.utilization() < 1e-10);
        // assert_eq!(self.indices.len(), self.logs() as usize);

        Ok(())
    }
}

fn open_file(dir: &Path, epoch: Epoch) -> Result<PBufWriter<File>> {
    let path = dir.join(store_file_name(epoch));

    let mut write_fd = OpenOptions::new().create(true).append(true).open(&path)?;
    let end = write_fd.seek(SeekFrom::End(0))?;

    Ok(PBufWriter::new(write_fd, end))
}

type Epoch = usize;

#[derive(Debug, Clone, Copy)]
struct Span {
    epoch: Epoch,
    pos: u64,
    len: u64,
}

impl Span {
    fn should_advance(&self) -> bool {
        self.pos + self.len > MAX_SIZE
    }
}

fn read_span(file: &File, pos: u64, len: u64) -> Result<Command> {
    info!("Reading at: {}, {} bytes", pos, len);

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
    reader: RwLockReadGuard<'a, Vec<File>>,
}

impl<'a> KvsReader<'a> {
    /// Returns a owned String value corresponding to the key.
    fn get(&self, key: String) -> Result<Option<String>> {
        let span = if let Some(entry) = self.indices.get(&key) {
            *entry.value()
        } else {
            return Ok(None);
        };

        read_value_from(&self.reader[span.epoch], span.pos, span.len).map(Some)
    }
}

fn read_value_from(file: &File, pos: u64, len: u64) -> Result<String> {
    let value = match read_span(file, pos, len)? {
        Command::Set(_, value) => value,
        _ => return Err(Error::StoreFileCorrupted(pos, Corruption::HasNoValue)),
    };

    Ok(value)
}

struct KvsWriter<'a> {
    epoch: Epoch,
    stats: &'a Stats,
    writer: MutexGuard<'a, PBufWriter<File>>,
    indices: &'a Indices,
}

impl<'a> KvsWriter<'a> {
    fn set(&mut self, key: String, value: String) -> Result<Span> {
        let span = self.append(&Command::Set(key.clone(), value))?;
        let is_overwrite = self.indices.insert(key, span).is_some();
        self.update_stats(is_overwrite);

        Ok(span)
    }

    fn remove(&mut self, key: String) -> Result<Span> {
        if !self.indices.contains_key(&key) {
            Err(Error::KeyNotFound)
        } else {
            self.indices.remove(&key);
            let span = self.append(&Command::Remove(key))?;
            self.update_stats(true);

            Ok(span)
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
        Ok(Span {
            epoch: self.epoch,
            pos,
            len,
        })
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

struct KvsCompactor<'a> {
    stats: &'a Stats,
    reader: RwLockWriteGuard<'a, Vec<File>>,
    writer: MutexGuard<'a, PBufWriter<File>>,
    indices: &'a Indices,
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

const COMPACT_STATE_IDLE: u8 = 0;
const COMPACT_STATE_ONGOING: u8 = 1;
const COMPACT_STATE_READY: u8 = 2;

#[derive(Default)]
struct Stats {
    // The number of logs on disk.
    logs: AtomicU32,
    // The number of values on disk.
    values: AtomicU32,
    // bytes in the write buffer.
    buf_len: AtomicUsize,
    // state of the possibly ongoing compaction.
    compact_state: AtomicU8,
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

    fn set_compact_state(&self, state: u8) {
        assert!(state <= COMPACT_STATE_READY);
        self.compact_state.store(state, Ordering::Release);
    }

    fn compact_state(&self) -> u8 {
        self.compact_state.load(Ordering::Acquire)
    }
}

type Indices = DashMap<String, Span>;

fn update_indices<R: Read>(
    reader: R,
    indices: &DashMap<String, Span>,
    epoch: Epoch,
    stats: &Stats,
) -> Result<()> {
    let reader = BufReader::new(reader);
    let mut de = serde_json::Deserializer::from_reader(reader).into_iter();

    loop {
        let pos = de.byte_offset() as u64;
        match de.next().transpose()? {
            Some(Command::Set(key, _)) => {
                let len = de.byte_offset() as u64 - pos;
                let is_overwrite = indices.insert(key, Span { epoch, pos, len }).is_some();
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

    Ok(())
}

/// Sharable [KvStore](KvStore).
pub struct LogKvsEngine {
    store: Arc<KvStore>,
}

impl LogKvsEngine {
    /// Create a handle to the on-disk key-value store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore::open(path)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Test whether log engine is persistent under the given directory.
    pub fn is_persistent<P: AsRef<Path>>(path: P) -> bool {
        let path = path.as_ref();
        path.is_dir() && path.join(store_file_name(0)).is_file()
    }

    /// Exposed for tests.
    pub fn on_disk_size(&self) -> u32 {
        self.store.logs()
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let span = self.store.to_writer()?.set(key, value)?;

        if span.should_advance() {
            self.store.advance_epoch()?;
        }

        if self.store.should_compact() {
            self.store.compact()?;
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.store.to_reader()?.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        let span = self.store.to_writer()?.remove(key)?;

        if span.should_advance() {
            self.store.advance_epoch()?;
        }
        if self.store.should_compact() {
            self.store.compact()?;
        }
        Ok(())
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
    fn set(&self, key: String, value: String) -> BoxFuture<'static, Result<()>> {
        let engine = self.clone();
        futures::future::lazy(move |_| engine.set(key, value)).boxed()
    }

    fn get(&self, key: String) -> BoxFuture<'static, Result<Option<String>>> {
        let engine = self.clone();
        futures::future::lazy(move |_| engine.get(key)).boxed()
    }

    fn remove(&self, key: String) -> BoxFuture<'static, Result<()>> {
        let engine = self.clone();
        futures::future::lazy(move |_| engine.remove(key)).boxed()
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
