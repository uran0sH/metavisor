//! TypeStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{type_key, CoreError, Result, TypeCategory, TypeDef, TypeStore};

use crate::kv::{KvStore, WriteOp};

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
        // Scan the KV store for all keys under the type prefix.
        // Format: "type:{name}"
        const TYPE_PREFIX: &[u8] = b"type:";

        let entries: Vec<(Vec<u8>, TypeDef)> = self
            .kv
            .scan_prefix(TYPE_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut names: Vec<String> = entries
            .into_iter()
            .filter_map(|(key, _)| {
                key.strip_prefix(TYPE_PREFIX).map(|suffix| {
                    // Type names are ASCII-ish identifiers in this project; be permissive anyway.
                    String::from_utf8_lossy(suffix).to_string()
                })
            })
            .collect();

        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn list_types_by_category(&self, _category: TypeCategory) -> Result<Vec<String>> {
        // No secondary index yet; do a prefix scan and filter in memory.
        const TYPE_PREFIX: &[u8] = b"type:";

        let entries: Vec<(Vec<u8>, TypeDef)> = self
            .kv
            .scan_prefix(TYPE_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut names: Vec<String> = entries
            .into_iter()
            .filter_map(|(key, def)| {
                if !matches!(
                    (&def, _category),
                    (TypeDef::Entity(_), TypeCategory::Entity)
                        | (TypeDef::Relationship(_), TypeCategory::Relationship)
                ) {
                    return None;
                }

                key.strip_prefix(TYPE_PREFIX)
                    .map(|suffix| String::from_utf8_lossy(suffix).to_string())
            })
            .collect();

        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn batch_create_types(&self, type_defs: &[TypeDef]) -> Result<()> {
        // Phase 1: Validate all types don't already exist
        for type_def in type_defs {
            let key = type_key(type_def.name());
            if self
                .kv
                .exists(&key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?
            {
                return Err(CoreError::TypeAlreadyExists(type_def.name().to_string()));
            }
        }

        // Phase 2: Build batch operations and write atomically
        let mut ops = Vec::with_capacity(type_defs.len());
        for type_def in type_defs {
            let key = type_key(type_def.name());
            let value =
                serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;
            ops.push(WriteOp::Set { key, value });
        }

        self.kv
            .batch_write(ops)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn batch_update_types(&self, type_defs: &[TypeDef]) -> Result<()> {
        // Phase 1: Validate all types exist
        for type_def in type_defs {
            let key = type_key(type_def.name());
            if !self
                .kv
                .exists(&key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?
            {
                return Err(CoreError::TypeNotFound(type_def.name().to_string()));
            }
        }

        // Phase 2: Build batch operations and write atomically
        let mut ops = Vec::with_capacity(type_defs.len());
        for type_def in type_defs {
            let key = type_key(type_def.name());
            let value =
                serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;
            ops.push(WriteOp::Set { key, value });
        }

        self.kv
            .batch_write(ops)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
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

    #[tokio::test]
    async fn test_batch_create_types_success() {
        let store = create_test_store().await;

        let type_defs: Vec<TypeDef> = vec!["Alpha", "Beta", "Gamma"]
            .into_iter()
            .map(|name| {
                TypeDef::from(EntityDef::new(name).attribute(AttributeDef::new("id", "string")))
            })
            .collect();

        store.batch_create_types(&type_defs).await.unwrap();

        // All three should exist
        assert!(store.type_exists("Alpha").await.unwrap());
        assert!(store.type_exists("Beta").await.unwrap());
        assert!(store.type_exists("Gamma").await.unwrap());
    }

    #[tokio::test]
    async fn test_batch_create_types_partial_failure_rollback() {
        let store = create_test_store().await;

        // Pre-create one type
        let existing =
            TypeDef::from(EntityDef::new("Existing").attribute(AttributeDef::new("id", "string")));
        store.create_type(&existing).await.unwrap();

        // Try to batch create including the existing one
        let type_defs: Vec<TypeDef> = vec!["New1", "Existing", "New2"]
            .into_iter()
            .map(|name| {
                TypeDef::from(EntityDef::new(name).attribute(AttributeDef::new("id", "string")))
            })
            .collect();

        let result = store.batch_create_types(&type_defs).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::TypeAlreadyExists(_)
        ));

        // New1 and New2 should NOT exist — batch was atomic
        assert!(!store.type_exists("New1").await.unwrap());
        assert!(!store.type_exists("New2").await.unwrap());
    }

    #[tokio::test]
    async fn test_batch_update_types_success() {
        let store = create_test_store().await;

        // Create two types
        let type_defs: Vec<TypeDef> = vec![
            TypeDef::from(EntityDef::new("A").attribute(AttributeDef::new("id", "string"))),
            TypeDef::from(EntityDef::new("B").attribute(AttributeDef::new("id", "string"))),
        ];
        store.batch_create_types(&type_defs).await.unwrap();

        // Update both
        let updated_defs: Vec<TypeDef> = vec![
            TypeDef::from(
                EntityDef::new("A")
                    .description("updated A")
                    .attribute(AttributeDef::new("id", "string")),
            ),
            TypeDef::from(
                EntityDef::new("B")
                    .description("updated B")
                    .attribute(AttributeDef::new("id", "string")),
            ),
        ];
        store.batch_update_types(&updated_defs).await.unwrap();

        let a = store.get_type("A").await.unwrap();
        assert_eq!(a.name(), "A");
    }

    #[tokio::test]
    async fn test_batch_update_types_missing_rollback() {
        let store = create_test_store().await;

        // Create only one type
        let existing =
            TypeDef::from(EntityDef::new("Exists").attribute(AttributeDef::new("id", "string")));
        store.create_type(&existing).await.unwrap();

        // Try to update both an existing and non-existing type
        let update_defs: Vec<TypeDef> = vec![
            TypeDef::from(
                EntityDef::new("Exists")
                    .description("v2")
                    .attribute(AttributeDef::new("id", "string")),
            ),
            TypeDef::from(
                EntityDef::new("Ghost")
                    .description("v2")
                    .attribute(AttributeDef::new("id", "string")),
            ),
        ];

        let result = store.batch_update_types(&update_defs).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));

        // "Exists" should NOT be updated — batch was atomic
        let fetched = store.get_type("Exists").await.unwrap();
        // The original has no description
        match fetched {
            TypeDef::Entity(def) => {
                assert!(def.description.is_none() || def.description.as_deref() == Some(""))
            }
            _ => panic!("Expected Entity variant"),
        }
    }
}
