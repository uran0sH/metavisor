//! Write Ahead Log (WAL) for transactional KV writes and recoverable graph projection
//!
//! The WAL is split into:
//! - transaction metadata (`TxMeta`) keyed by transaction id
//! - append-only operation records (`TxOpRecord`) keyed by transaction id + sequence
//!
//! KV is the source of truth. Graph updates are treated as a recoverable projection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, StorageError};
use crate::kv::KvStore;

/// Transaction operation type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OpType {
    CreateEntity {
        guid: String,
        entity_type: String,
        serialized_data: Vec<u8>,
    },
    DeleteEntity {
        guid: String,
    },
    UpdateEntity {
        guid: String,
        serialized_data: Vec<u8>,
    },
    UpdateRelationship {
        guid: String,
        serialized_data: Vec<u8>,
    },
    CreateRelationship {
        guid: String,
        serialized_data: Vec<u8>,
    },
    DeleteRelationship {
        guid: String,
    },
    AddGraphNode {
        entity_guid: String,
        entity_type: String,
    },
    RemoveGraphNode {
        entity_guid: String,
    },
    AddGraphEdge {
        relationship_guid: String,
        relationship_type: String,
        from_guid: String,
        to_guid: String,
        propagate_tags: Option<String>,
    },
    RemoveGraphEdge {
        relationship_guid: String,
    },
    RebuildGraph,
}

/// Transaction state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TxState {
    Preparing,
    Committed,
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TxMeta {
    pub tx_id: String,
    pub state: TxState,
    pub op_count: u64,
    pub created_at: u64,
    pub committed_at: Option<u64>,
    pub kv_applied: bool,
    pub graph_applied: bool,
    pub version: u32,
}

impl TxMeta {
    const VERSION: u32 = 2;

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    pub fn new(tx_id: String) -> Self {
        Self {
            tx_id,
            state: TxState::Preparing,
            op_count: 0,
            created_at: Self::now_secs(),
            committed_at: None,
            kv_applied: false,
            graph_applied: false,
            version: Self::VERSION,
        }
    }
}

/// Append-only WAL operation record
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TxOpRecord {
    pub tx_id: String,
    pub seq: u64,
    pub op: OpType,
}

impl TxOpRecord {
    pub fn new(tx_id: String, seq: u64, op: OpType) -> Self {
        Self { tx_id, seq, op }
    }
}

/// Write Ahead Log manager
pub struct WriteAheadLog {
    kv: KvStore,
    tx_prefix: Vec<u8>,
}

impl WriteAheadLog {
    const PREFIX: &'static [u8] = b"wal2:tx:";

    pub fn new(kv: KvStore) -> Self {
        Self {
            kv,
            tx_prefix: Self::PREFIX.to_vec(),
        }
    }

    fn tx_root_key(&self, tx_id: &str) -> Vec<u8> {
        let mut key = self.tx_prefix.clone();
        key.extend_from_slice(tx_id.as_bytes());
        key
    }

    fn meta_key(&self, tx_id: &str) -> Vec<u8> {
        let mut key = self.tx_root_key(tx_id);
        key.extend_from_slice(b":meta");
        key
    }

    fn ops_prefix(&self, tx_id: &str) -> Vec<u8> {
        let mut key = self.tx_root_key(tx_id);
        key.extend_from_slice(b":ops:");
        key
    }

    fn op_key(&self, tx_id: &str, seq: u64) -> Vec<u8> {
        let mut key = self.ops_prefix(tx_id);
        key.extend_from_slice(format!("{seq:020}").as_bytes());
        key
    }

    pub async fn create_transaction(&self, tx_id: &str) -> Result<TxMeta> {
        let meta = TxMeta::new(tx_id.to_string());
        self.kv.put(&self.meta_key(tx_id), &meta).await?;
        Ok(meta)
    }

    pub async fn get_transaction_meta(&self, tx_id: &str) -> Result<Option<TxMeta>> {
        self.kv.get(&self.meta_key(tx_id)).await
    }

    pub async fn put_transaction_meta(&self, meta: &TxMeta) -> Result<()> {
        self.kv.put(&self.meta_key(&meta.tx_id), meta).await
    }

    pub async fn append_operation(&self, tx_id: &str, seq: u64, op: OpType) -> Result<()> {
        let record = TxOpRecord::new(tx_id.to_string(), seq, op);
        self.kv.put(&self.op_key(tx_id, seq), &record).await?;

        let mut meta = self
            .get_transaction_meta(tx_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WAL transaction not found: {tx_id}")))?;
        meta.op_count = meta.op_count.max(seq + 1);
        self.put_transaction_meta(&meta).await
    }

    pub async fn mark_kv_applied(&self, tx_id: &str) -> Result<()> {
        let mut meta = self
            .get_transaction_meta(tx_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WAL transaction not found: {tx_id}")))?;
        meta.kv_applied = true;
        self.put_transaction_meta(&meta).await
    }

    pub async fn mark_committed(&self, tx_id: &str) -> Result<()> {
        let mut meta = self
            .get_transaction_meta(tx_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WAL transaction not found: {tx_id}")))?;
        meta.state = TxState::Committed;
        meta.committed_at = Some(TxMeta::now_secs());
        self.put_transaction_meta(&meta).await
    }

    pub async fn mark_graph_applied(&self, tx_id: &str) -> Result<()> {
        let mut meta = self
            .get_transaction_meta(tx_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WAL transaction not found: {tx_id}")))?;
        meta.graph_applied = true;
        self.put_transaction_meta(&meta).await
    }

    pub async fn mark_aborted(&self, tx_id: &str) -> Result<()> {
        let mut meta = self
            .get_transaction_meta(tx_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WAL transaction not found: {tx_id}")))?;
        meta.state = TxState::Aborted;
        self.put_transaction_meta(&meta).await
    }

    pub async fn get_transaction_ops(&self, tx_id: &str) -> Result<Vec<TxOpRecord>> {
        let records: Vec<(Vec<u8>, TxOpRecord)> = self.kv.scan_prefix(&self.ops_prefix(tx_id))?;
        let mut records: Vec<TxOpRecord> = records.into_iter().map(|(_, record)| record).collect();
        records.sort_by_key(|record| record.seq);
        Ok(records)
    }

    pub async fn list_transaction_metas(&self) -> Result<HashMap<String, TxMeta>> {
        let records: Vec<(Vec<u8>, serde_json::Value)> = self.kv.scan_prefix(&self.tx_prefix)?;
        let mut metas = HashMap::new();

        for (key, value) in records {
            if key.ends_with(b":meta") {
                let meta: TxMeta =
                    serde_json::from_value(value).map_err(StorageError::Serialization)?;
                metas.insert(meta.tx_id.clone(), meta);
            }
        }

        Ok(metas)
    }

    pub async fn list_incomplete_transactions(&self) -> Result<Vec<TxMeta>> {
        let mut metas: Vec<TxMeta> = self
            .list_transaction_metas()
            .await?
            .into_values()
            .filter(|meta| meta.state != TxState::Aborted && (!meta.kv_applied || !meta.graph_applied || meta.state != TxState::Committed))
            .collect();
        metas.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.tx_id.cmp(&b.tx_id)));
        Ok(metas)
    }

    pub async fn cleanup_old_transactions(&self, before_timestamp: u64) -> Result<usize> {
        let metas = self.list_transaction_metas().await?;
        let mut cleaned = 0;

        for meta in metas.into_values() {
            let old_enough = meta
                .committed_at
                .map(|ts| ts < before_timestamp)
                .unwrap_or(false);
            let removable = (meta.state == TxState::Committed && meta.graph_applied && old_enough)
                || meta.state == TxState::Aborted;

            if !removable {
                continue;
            }

            self.kv.delete(&self.meta_key(&meta.tx_id)).await?;
            for record in self.get_transaction_ops(&meta.tx_id).await? {
                self.kv.delete(&self.op_key(&record.tx_id, record.seq)).await?;
                cleaned += 1;
            }
            cleaned += 1;
        }

        Ok(cleaned)
    }
}

/// Transaction context for WAL-backed writes
pub struct Transaction {
    tx_id: String,
    wal: Arc<WriteAheadLog>,
    seq: u64,
    active: bool,
}

impl Transaction {
    pub async fn begin(wal: Arc<WriteAheadLog>) -> Result<Self> {
        let tx_id = uuid::Uuid::new_v4().to_string();
        wal.create_transaction(&tx_id).await?;

        Ok(Self {
            tx_id,
            wal,
            seq: 0,
            active: true,
        })
    }

    pub fn id(&self) -> &str {
        &self.tx_id
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub async fn log_operation(&mut self, op: OpType) -> Result<u64> {
        if !self.active {
            return Err(StorageError::Kv("Transaction is not active".to_string()));
        }

        let current_seq = self.seq;
        self.wal.append_operation(&self.tx_id, current_seq, op).await?;
        self.seq += 1;
        Ok(current_seq)
    }

    pub async fn mark_kv_applied(&self) -> Result<()> {
        self.wal.mark_kv_applied(&self.tx_id).await
    }

    pub async fn commit(&mut self) -> Result<()> {
        if !self.active {
            return Err(StorageError::Kv("Transaction is not active".to_string()));
        }

        self.wal.mark_committed(&self.tx_id).await?;
        self.active = false;
        Ok(())
    }

    pub async fn mark_graph_applied(&self) -> Result<()> {
        self.wal.mark_graph_applied(&self.tx_id).await
    }

    pub async fn abort(&mut self) -> Result<()> {
        if !self.active {
            return Err(StorageError::Kv("Transaction is not active".to_string()));
        }

        self.wal.mark_aborted(&self.tx_id).await?;
        self.active = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_wal() -> (Arc<WriteAheadLog>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let wal = Arc::new(WriteAheadLog::new(kv));
        (wal, tempdir)
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let (wal, _temp) = create_test_wal().await;

        let mut tx = Transaction::begin(wal.clone()).await.unwrap();
        tx.log_operation(OpType::CreateEntity {
            guid: "guid-1".to_string(),
            entity_type: "Table".to_string(),
            serialized_data: vec![],
        })
        .await
        .unwrap();
        tx.mark_kv_applied().await.unwrap();
        tx.commit().await.unwrap();
        tx.mark_graph_applied().await.unwrap();

        let meta = wal.get_transaction_meta(tx.id()).await.unwrap().unwrap();
        assert_eq!(meta.state, TxState::Committed);
        assert!(meta.kv_applied);
        assert!(meta.graph_applied);
        assert_eq!(meta.op_count, 1);
    }

    #[tokio::test]
    async fn test_incomplete_transactions() {
        let (wal, _temp) = create_test_wal().await;

        let mut tx = Transaction::begin(wal.clone()).await.unwrap();
        tx.log_operation(OpType::CreateEntity {
            guid: "guid-1".to_string(),
            entity_type: "Table".to_string(),
            serialized_data: vec![],
        })
        .await
        .unwrap();
        tx.mark_kv_applied().await.unwrap();
        tx.commit().await.unwrap();

        let metas = wal.list_incomplete_transactions().await.unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].tx_id, tx.id());
        assert!(!metas[0].graph_applied);
    }

    #[tokio::test]
    async fn test_abort_transaction() {
        let (wal, _temp) = create_test_wal().await;

        let mut tx = Transaction::begin(wal.clone()).await.unwrap();
        tx.log_operation(OpType::CreateEntity {
            guid: "guid-1".to_string(),
            entity_type: "Table".to_string(),
            serialized_data: vec![],
        })
        .await
        .unwrap();
        tx.abort().await.unwrap();

        let meta = wal.get_transaction_meta(tx.id()).await.unwrap().unwrap();
        assert_eq!(meta.state, TxState::Aborted);
    }
}
