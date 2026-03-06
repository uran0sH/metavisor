//! TypeStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{type_key, CoreError, Result, TypeCategory, TypeDef, TypeStore};

use crate::kv::KvStore;

/// KvTypeStore - TypeStore implementation using surrealkv
pub struct KvTypeStore {
    kv: KvStore,
}

impl KvTypeStore {
    /// Create a new KvTypeStore
    pub fn new(kv: KvStore) -> Self {
        Self { kv }
    }
}

#[async_trait]
impl TypeStore for KvTypeStore {
    async fn create_type(&self, type_def: &TypeDef) -> Result<()> {
        let key = type_key(type_def.name());

        // Check if type already exists
        if self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::TypeAlreadyExists(type_def.name().to_string()));
        }

        self.kv
            .put(&key, type_def)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_type(&self, name: &str) -> Result<TypeDef> {
        let key = type_key(name);

        self.kv
            .get(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::TypeNotFound(name.to_string()))
    }

    async fn update_type(&self, type_def: &TypeDef) -> Result<()> {
        let key = type_key(type_def.name());

        // Check if type exists
        if !self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::TypeNotFound(type_def.name().to_string()));
        }

        self.kv
            .put(&key, type_def)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_type(&self, name: &str) -> Result<()> {
        let key = type_key(name);

        // Check if type exists
        if !self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::TypeNotFound(name.to_string()));
        }

        self.kv
            .delete(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn type_exists(&self, name: &str) -> Result<bool> {
        let key = type_key(name);
        self.kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))
    }

    async fn list_types(&self) -> Result<Vec<String>> {
        // TODO: Implement proper scan with prefix
        // For now, we'll need to add a scan method to KvStore
        Ok(Vec::new())
    }

    async fn list_types_by_category(&self, _category: TypeCategory) -> Result<Vec<String>> {
        // TODO: Implement with secondary index
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{AttributeDef, EntityDef};
    use tempfile::TempDir;

    async fn create_test_store() -> KvTypeStore {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        KvTypeStore::new(kv)
    }

    #[tokio::test]
    async fn test_create_and_get_type() {
        let store = create_test_store().await;

        let entity_def = EntityDef::new("Table")
            .description("A database table")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("schema", "string"));
        let type_def = TypeDef::from(entity_def);

        // Create type
        store.create_type(&type_def).await.unwrap();

        // Get type
        let retrieved = store.get_type("Table").await.unwrap();
        assert_eq!(retrieved.name(), "Table");
    }

    #[tokio::test]
    async fn test_create_duplicate_type() {
        let store = create_test_store().await;

        let entity_def = EntityDef::new("Column").attribute(AttributeDef::new("name", "string"));
        let type_def = TypeDef::from(entity_def);

        store.create_type(&type_def).await.unwrap();

        // Try to create again
        let result = store.create_type(&type_def).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::TypeAlreadyExists(_)
        ));
    }

    #[tokio::test]
    async fn test_update_type() {
        let store = create_test_store().await;

        let entity_def = EntityDef::new("Dataset").attribute(AttributeDef::new("name", "string"));
        let type_def = TypeDef::from(entity_def);

        store.create_type(&type_def).await.unwrap();

        // Update with more attributes
        let updated_entity = EntityDef::new("Dataset")
            .description("A dataset")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("description", "string"));
        let updated = TypeDef::from(updated_entity);

        store.update_type(&updated).await.unwrap();

        let retrieved = store.get_type("Dataset").await.unwrap();
        assert_eq!(retrieved.name(), "Dataset");
    }

    #[tokio::test]
    async fn test_delete_type() {
        let store = create_test_store().await;

        let entity_def = EntityDef::new("TempType").attribute(AttributeDef::new("id", "string"));
        let type_def = TypeDef::from(entity_def);

        store.create_type(&type_def).await.unwrap();
        assert!(store.type_exists("TempType").await.unwrap());

        store.delete_type("TempType").await.unwrap();
        assert!(!store.type_exists("TempType").await.unwrap());

        // Try to get deleted type
        let result = store.get_type("TempType").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_type_not_found() {
        let store = create_test_store().await;

        let result = store.get_type("NonExistent").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));
    }
}
