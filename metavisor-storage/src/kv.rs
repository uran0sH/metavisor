//! KV store implementation using surrealkv

use serde::{de::DeserializeOwned, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::{Result, StorageError};

/// KV Store wrapper
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<surrealkv::Tree>,
}

impl KvStore {
    /// Open a KV store at the given path
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let inner = surrealkv::TreeBuilder::new()
            .with_path(path.into())
            .build()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Get a value by key
    pub async fn get<V>(&self, key: &[u8]) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        let bytes = txn
            .get(key.to_vec())
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        match bytes {
            Some(b) => {
                let value = serde_json::from_slice(&b)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Put a key-value pair
    pub async fn put<V>(&self, key: &[u8], value: &V) -> Result<()>
    where
        V: Serialize,
    {
        let bytes = serde_json::to_vec(value)?;
        let mut txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        txn.set(key.to_vec(), bytes)
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(())
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        txn.delete(key.to_vec())
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(())
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &[u8]) -> Result<bool> {
        let txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        let result = txn
            .get(key.to_vec())
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(result.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::KvStore;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_get_put_delete() {
        let tempdir = TempDir::new().unwrap();
        let store = KvStore::open(tempdir.path()).unwrap();
        store.put(b"a", b"b").await.unwrap();
        let value: Vec<u8> = store.get(b"a").await.unwrap().unwrap();
        assert_eq!(b"b", value.as_slice());
    }
}
