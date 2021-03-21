use crate::{Error as KvError, KvsEngine};
use futures::{future, FutureExt, TryFutureExt};
use sled::{Db, Error as SledError};
use std::{path::Path, str::from_utf8};

/// A wrapper over sled::Db.
#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

/// The on-disk directory of the sled store.
pub const SLED_STORE_DIR: &str = ".sled";

impl SledKvsEngine {
    /// Creates a handle to the on-disk key-value store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SledError> {
        let path = path.as_ref();
        let db = sled::open(path.join(SLED_STORE_DIR))?;
        info!("Sled db was recovered: {}", db.was_recovered());
        Ok(Self { db })
    }

    /// Test whether sled engine is persistent under the given directory.
    pub fn is_persistent<P: AsRef<Path>>(path: P) -> bool {
        let path = path.as_ref();
        path.is_dir() && path.join(SLED_STORE_DIR).is_dir()
    }

    /// Insert a key-value pair into the store.
    pub async fn set(&self, key: String, value: String) -> Result<(), SledError> {
        self.db.insert(key, value.as_str())?;
        // `sled` by default caches all writes and only flushes to disk every 1000ms, a few tests
        // spawns the server on a child process then calls `std::process::Child::kill` on to
        // terminate it. At least on x86_64-pc-windows-msvc, `std::process::Child::kill` will skip
        // `Drop` implementations, when used as a KvsEngine the `sled::Db` must be flushed after
        // every modifying operation otherwise a few tests won't pass.
        self.db.flush_async().await?;
        Ok(())
    }

    /// Returns a owned String value corresponding to the key.
    pub fn get(&self, key: String) -> Result<Option<String>, SledError> {
        if let Some(value) = self.db.get(key)? {
            from_utf8(&value)
                .map(|s| Some(s.to_string()))
                .map_err(|_| SledError::Corruption { at: None, bt: () })
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the store. Return true if an old value is removed.
    pub async fn remove(&self, key: String) -> Result<bool, SledError> {
        let value = self.db.remove(key)?;
        self.db.flush_async().await?;
        Ok(value.is_some())
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> futures::future::BoxFuture<crate::Result<()>> {
        self.set(key, value).map_err(From::from).boxed()
    }

    fn get(&self, key: String) -> futures::future::BoxFuture<crate::Result<Option<String>>> {
        let engine = self.clone();
        future::lazy(move |_| engine.get(key).map_err(From::from)).boxed()
    }

    fn remove(&self, key: String) -> futures::future::BoxFuture<crate::Result<()>> {
        self.remove(key)
            .map(|res| match res {
                Ok(true) => Ok(()),
                Ok(false) => Err(KvError::KeyNotFound),
                Err(e) => Err(From::from(e)),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn sled_set() -> Result<(), SledError> {
        let store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string()).await
    }

    #[tokio::test]
    async fn sled_get() -> Result<(), SledError> {
        let store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string()).await?;
        assert_eq!(store.get("Key".to_string())?, Some("Value".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn sled_remove() -> Result<(), SledError> {
        let store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string()).await?;
        assert_eq!(store.get("Key".to_string())?, Some("Value".to_string()));
        store.remove("Key".to_string()).await?;
        assert_eq!(store.get("Key".to_string())?, None);
        Ok(())
    }
}
