//! Metavisor Storage - KV store and search index
//!
//! This crate provides storage backends for the Metavisor platform.

pub mod consistency;
pub mod entity_store;
pub mod error;
pub mod grafeo_graph_store;
pub mod index;
pub mod kv;
pub mod metavisor_store;
pub mod relationship_store;
pub mod transaction;
pub mod type_store;
pub mod wal;

pub use consistency::{ConsistencyChecker, ConsistencyReport, RepairResult};
pub use entity_store::KvEntityStore;
pub use error::{Result, StorageError};
pub use grafeo_graph_store::GrafeoGraphStore;
pub use kv::KvStore;
pub use metavisor_store::DefaultMetavisorStore;
pub use relationship_store::KvRelationshipStore;
pub use transaction::{InitializationResult, RecoveryResult, RecoveryStats, TransactionManager};
pub use type_store::KvTypeStore;
pub use wal::{OpType, Transaction, TxMeta, TxOpRecord, TxState, WriteAheadLog};

/// Get the graph store type name for logging/debugging
pub fn graph_store_type() -> &'static str {
    "grafeo (graph database)"
}
