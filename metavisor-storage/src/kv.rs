//! KV store implementation using surrealkv

use serde::{de::DeserializeOwned, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use surrealkv::LSMIterator;

use crate::error::{Result, StorageError};

/// A write operation for batch transactions
pub enum WriteOp {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// A precondition check for conditional writes
pub enum CheckOp {
    /// Key must NOT exist (for uniqueness enforcement)
    Absent { key: Vec<u8> },
    /// Key must exist with exactly this value (for optimistic concurrency / stale-snapshot detection)
    ValueEquals { key: Vec<u8>, expected: Vec<u8> },
}

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

    /// Get raw bytes by key (no deserialization)
    pub async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        txn.get(key.to_vec())
            .map_err(|e| StorageError::Kv(e.to_string()))
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

    /// Execute multiple write operations in a single transaction
    /// This ensures atomicity - either all operations succeed or none do
    pub async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<()> {
        let mut txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        for op in ops {
            match op {
                WriteOp::Set { key, value } => {
                    txn.set(key, value)
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                }
                WriteOp::Delete { key } => {
                    txn.delete(key)
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                }
            }
        }

        txn.commit()
            .await
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(())
    }

    /// Execute precondition checks followed by write operations in a single transaction.
    /// If any check fails, no writes are performed.
    /// This eliminates TOCTOU races between read-checks and writes.
    pub async fn conditional_batch_write(
        &self,
        checks: Vec<CheckOp>,
        ops: Vec<WriteOp>,
    ) -> Result<()> {
        let mut txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        // Phase 1: verify preconditions
        for check in &checks {
            match check {
                CheckOp::Absent { key } => {
                    let existing = txn
                        .get(key.to_vec())
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                    if existing.is_some() {
                        return Err(StorageError::AlreadyExists(
                            String::from_utf8_lossy(key).into(),
                        ));
                    }
                }
                CheckOp::ValueEquals { key, expected } => {
                    let actual = txn
                        .get(key.to_vec())
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                    match actual {
                        None => {
                            return Err(StorageError::Conflict(
                                String::from_utf8_lossy(key).into(),
                            ));
                        }
                        Some(bytes) if bytes != *expected => {
                            return Err(StorageError::Conflict(
                                String::from_utf8_lossy(key).into(),
                            ));
                        }
                        _ => {} // matches
                    }
                }
            }
        }

        // Phase 2: apply writes (only reached if all checks passed)
        for op in ops {
            match op {
                WriteOp::Set { key, value } => {
                    txn.set(key, value)
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                }
                WriteOp::Delete { key } => {
                    txn.delete(key)
                        .map_err(|e| StorageError::Kv(e.to_string()))?;
                }
            }
        }

        txn.commit()
            .await
            .map_err(|e| StorageError::Kv(e.to_string()))?;
        Ok(())
    }

    /// Scan all keys with a given prefix
    /// Returns a vector of (key, value) pairs where key starts with prefix
    pub fn scan_prefix<V>(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, V)>>
    where
        V: DeserializeOwned,
    {
        let txn = self
            .inner
            .begin()
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        // Create end bound by incrementing the last byte of the prefix
        // This gives us the range [prefix, prefix+1) which covers all keys starting with prefix
        let start = prefix.to_vec();
        let end = Self::increment_prefix(prefix);

        let mut iter = txn
            .range(start, end)
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        // Seek to the first key with the prefix
        iter.seek_first()
            .map_err(|e| StorageError::Kv(e.to_string()))?;

        let mut results = Vec::new();
        while iter.valid() {
            let key_ref = iter.key();
            let user_key = key_ref.user_key().to_vec();

            // Get the value
            let value_bytes = iter.value().map_err(|e| StorageError::Kv(e.to_string()))?;
            let value: V = serde_json::from_slice(&value_bytes)?;
            results.push((user_key, value));

            // Move to next entry
            iter.next().map_err(|e| StorageError::Kv(e.to_string()))?;
        }

        Ok(results)
    }

    /// Increment the last byte of a prefix to create an exclusive upper bound
    fn increment_prefix(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        if let Some(last) = end.last_mut() {
            *last = last.saturating_add(1);
        } else {
            // Empty prefix - use a single byte as upper bound
            end.push(0xFF);
        }
        end
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
