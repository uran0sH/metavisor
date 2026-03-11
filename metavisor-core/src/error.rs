//! Core error types

use thiserror::Error;

/// Result type alias for core operations
pub type Result<T> = std::result::Result<T, CoreError>;

/// Core error type
#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Type not found: {0}")]
    TypeNotFound(String),

    #[error("Type already exists: {0}")]
    TypeAlreadyExists(String),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Entity already exists: {0}")]
    EntityAlreadyExists(String),

    #[error("Relationship not found: {0}")]
    RelationshipNotFound(String),

    #[error("Relationship already exists: {0}")]
    RelationshipAlreadyExists(String),

    #[error("Classification not found: {0}")]
    ClassificationNotFound(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid attribute: {0}")]
    InvalidAttribute(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
