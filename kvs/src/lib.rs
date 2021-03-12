//! A persistent key-value store.

#![deny(missing_docs)]

use std::collections::HashMap;

#[derive(Default)]
/// [WIP] A persistent key-value store of pairs of Strings.
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// Creates a handle to the persistent key-value store.
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a key-value pair into the store.
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    /// Returns a owned String value corresponding to the key.
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// assert_eq!(store.get("key".to_string()), Some("value".to_string()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.0.get(&key).cloned()
    }

    /// Removes a key from the store,
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// store.remove("key".to_string());
    /// assert_eq!(store.get("key".to_string()), None);
    /// ```
    pub fn remove(&mut self, key: String) {
        self.0.remove(&key);
    }
}
