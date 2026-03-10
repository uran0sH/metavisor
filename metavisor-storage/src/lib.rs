//! Metavisor Storage - KV store and search index
//!
//! This crate provides storage backends for the Metavisor platform.

pub mod entity_store;
pub mod error;
pub mod index;
pub mod kv;
pub mod type_store;

pub use entity_store::KvEntityStore;
pub use error::{Result, StorageError};
pub use kv::KvStore;
pub use type_store::KvTypeStore;
