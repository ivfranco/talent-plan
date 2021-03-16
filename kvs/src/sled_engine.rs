use crate::{Error as KvError, KvsEngine};
use sled::{Db, Error as SledError};
use std::{path::Path, str::from_utf8};

pub struct SledKvsEngine {
    db: Db,
}

pub const SLED_STORE_DIR: &str = ".sled";

impl SledKvsEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SledError> {
        let path = path.as_ref();
        let db = sled::open(path.join(SLED_STORE_DIR))?;
        Ok(Self { db })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<(), SledError> {
        self.db.insert(key, value.as_str())?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>, SledError> {
        if let Some(value) = self.db.get(key)? {
            from_utf8(&value)
                .map(|s| Some(s.to_string()))
                .map_err(|_| SledError::Corruption { at: None, bt: () })
        } else {
            Ok(None)
        }
    }

    pub fn remove(&self, key: String) -> Result<(), SledError> {
        self.db.remove(key)?;
        Ok(())
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<(), KvError> {
        self.set(key, value).map_err(From::from)
    }

    fn get(&mut self, key: String) -> Result<Option<String>, KvError> {
        SledKvsEngine::get(self, key).map_err(From::from)
    }

    fn remove(&mut self, key: String) -> Result<(), KvError> {
        SledKvsEngine::remove(self, key).map_err(From::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sled_set() -> Result<(), SledError> {
        let mut store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string())
    }

    #[test]
    fn sled_get() -> Result<(), SledError> {
        let mut store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string())?;
        assert_eq!(store.get("Key".to_string())?, Some("Value".to_string()));
        Ok(())
    }

    #[test]
    fn sled_remove() -> Result<(), SledError> {
        let mut store = SledKvsEngine::open(tempdir()?)?;
        store.set("Key".to_string(), "Value".to_string())?;
        assert_eq!(store.get("Key".to_string())?, Some("Value".to_string()));
        store.remove("Key".to_string())?;
        assert_eq!(store.get("Key".to_string())?, None);
        Ok(())
    }
}
