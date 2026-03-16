//! Metavisor Storage - KV store and search index
//!
//! This crate provides storage backends for the Metavisor platform.

pub mod entity_store;
pub mod error;
pub mod graph_store;
pub mod index;
pub mod kv;
pub mod metavisor_store;
pub mod relationship_store;
pub mod type_store;

pub use entity_store::KvEntityStore;
pub use error::{Result, StorageError};
pub use graph_store::InMemoryGraphStore;
pub use kv::KvStore;
pub use metavisor_store::DefaultMetavisorStore;
pub use relationship_store::KvRelationshipStore;
pub use type_store::KvTypeStore;
