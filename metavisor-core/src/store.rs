//! TypeStore trait for type system CRUD operations

use async_trait::async_trait;

use crate::{Result, TypeDef};

/// TypeStore trait for managing type definitions
#[async_trait]
pub trait TypeStore: Send + Sync {
    /// Create a new type definition
    async fn create_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Get a type definition by name
    async fn get_type(&self, name: &str) -> Result<TypeDef>;

    /// Update a type definition
    async fn update_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Delete a type definition by name
    async fn delete_type(&self, name: &str) -> Result<()>;

    /// Check if a type exists
    async fn type_exists(&self, name: &str) -> Result<bool>;

    /// List all type names
    async fn list_types(&self) -> Result<Vec<String>>;

    /// List types by category
    async fn list_types_by_category(&self, category: crate::TypeCategory) -> Result<Vec<String>>;
}

/// Type prefix for KV storage
const TYPE_PREFIX: &[u8] = b"type:";

/// Build the key for storing a type definition
pub fn type_key(name: &str) -> Vec<u8> {
    let mut key = TYPE_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}
