//! Storage error types

use thiserror::Error;

/// Result type alias for storage operations
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage error type
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Key not found: {0}")]
    NotFound(String),

    #[error("Key already exists: {0}")]
    AlreadyExists(String),

    #[error("Conflict: value changed for key: {0}")]
    Conflict(String),

    #[error("KV store error: {0}")]
    Kv(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Graph store error: {0}")]
    Graph(String),
}
