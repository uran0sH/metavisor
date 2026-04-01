//! TypeStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{
    type_guid_index_key, type_key, CoreError, Result, TypeCategory, TypeDef, TypeStore, TYPE_PREFIX,
};
use std::collections::HashSet;

use crate::kv::{CheckOp, KvStore, WriteOp};

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

fn validate_unique_type_names<'a, I>(names: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen: HashSet<&'a str> = HashSet::new();
    let mut duplicates: Vec<&'a str> = Vec::new();

    for name in names {
        if !seen.insert(name) {
            duplicates.push(name);
        }
    }

    if duplicates.is_empty() {
        return Ok(());
    }

    Err(CoreError::Validation(format!(
        "Duplicate type names in request: {}",
        duplicates.join(", ")
    )))
}

/// Generate a stable GUID from type name using SHA-256
fn generate_type_guid(name: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let hash = hasher.finalize();

    // Format as UUID-like string (first 16 bytes)
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hash[0], hash[1], hash[2], hash[3],
        hash[4], hash[5],
        hash[6], hash[7],
        hash[8], hash[9],
        hash[10], hash[11], hash[12], hash[13], hash[14], hash[15]
    )
}

/// Extract GUID from TypeDef (stored or derived from name)
fn type_guid(type_def: &TypeDef) -> String {
    type_def
        .guid()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| generate_type_guid(type_def.name()))
}

#[async_trait]
impl TypeStore for KvTypeStore {
    async fn create_type(&self, type_def: &TypeDef) -> Result<()> {
        let key = type_key(type_def.name());
        let value = serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;

        // Build GUID index for O(1) lookup by GUID
        let guid = type_guid(type_def);
        let guid_key = type_guid_index_key(&guid);
        let name_bytes = type_def.name().as_bytes().to_vec();

        // Use conditional write to eliminate TOCTOU race:
        // atomically check key absent and write value + guid index
        self.kv
            .conditional_batch_write(
                vec![
                    CheckOp::Absent { key: key.clone() },
                    CheckOp::Absent {
                        key: guid_key.clone(),
                    },
                ],
                vec![
                    WriteOp::Set { key, value },
                    WriteOp::Set {
                        key: guid_key,
                        value: name_bytes,
                    },
                ],
            )
            .await
            .map_err(|e| match e {
                crate::error::StorageError::AlreadyExists(_) => {
                    CoreError::TypeAlreadyExists(type_def.name().to_string())
                }
                _ => CoreError::Storage(e.to_string()),
            })?;

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
        let new_value =
            serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;

        // Read current value for optimistic concurrency control
        let old_value: Option<Vec<u8>> = self
            .kv
            .get_raw(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let old_value =
            old_value.ok_or_else(|| CoreError::TypeNotFound(type_def.name().to_string()))?;

        // Parse old type to get old GUID for index update
        let old_type: TypeDef =
            serde_json::from_slice(&old_value).map_err(|e| CoreError::Storage(e.to_string()))?;
        let old_guid = type_guid(&old_type);
        let new_guid = type_guid(type_def);

        // Build GUID index operations
        let old_guid_key = type_guid_index_key(&old_guid);
        let new_guid_key = type_guid_index_key(&new_guid);
        let name_bytes = type_def.name().as_bytes().to_vec();

        let mut checks = vec![CheckOp::ValueEquals {
            key: key.clone(),
            expected: old_value,
        }];
        let mut ops = vec![WriteOp::Set {
            key,
            value: new_value,
        }];

        // Update GUID index if GUID changed
        if old_guid != new_guid {
            checks.push(CheckOp::Absent {
                key: new_guid_key.clone(),
            });
            ops.push(WriteOp::Delete { key: old_guid_key });
            ops.push(WriteOp::Set {
                key: new_guid_key,
                value: name_bytes,
            });
        }

        // Use conditional write to eliminate TOCTOU race
        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| match e {
                crate::error::StorageError::Conflict(_) => {
                    CoreError::Conflict("Type was modified by another request".to_string())
                }
                crate::error::StorageError::AlreadyExists(_) => {
                    CoreError::Validation("New GUID already exists".to_string())
                }
                _ => CoreError::Storage(e.to_string()),
            })?;

        Ok(())
    }

    async fn delete_type(&self, name: &str) -> Result<()> {
        let key = type_key(name);

        // Read current value for optimistic concurrency control
        let old_value: Option<Vec<u8>> = self
            .kv
            .get_raw(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let old_value = old_value.ok_or_else(|| CoreError::TypeNotFound(name.to_string()))?;

        // Parse old type to get GUID for index deletion
        let old_type: TypeDef =
            serde_json::from_slice(&old_value).map_err(|e| CoreError::Storage(e.to_string()))?;
        let old_guid = type_guid(&old_type);
        let guid_key = type_guid_index_key(&old_guid);

        // Use conditional write to eliminate TOCTOU race:
        // atomically check value unchanged and delete data + guid index
        self.kv
            .conditional_batch_write(
                vec![CheckOp::ValueEquals {
                    key: key.clone(),
                    expected: old_value,
                }],
                vec![WriteOp::Delete { key }, WriteOp::Delete { key: guid_key }],
            )
            .await
            .map_err(|e| match e {
                crate::error::StorageError::Conflict(_) => {
                    // Value was modified by another thread
                    CoreError::Conflict("Type was modified by another request".to_string())
                }
                _ => CoreError::Storage(e.to_string()),
            })?;

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

        // Sort and dedup to ensure uniqueness (dedup only removes consecutive duplicates)
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn list_type_defs(&self) -> Result<Vec<TypeDef>> {
        // Single scan to get all type definitions directly (more efficient than list_types + get_type)
        let entries: Vec<(Vec<u8>, TypeDef)> = self
            .kv
            .scan_prefix(TYPE_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut defs: Vec<TypeDef> = entries.into_iter().map(|(_, def)| def).collect();

        // Sort by type name for consistent ordering
        defs.sort_by(|a, b| a.name().cmp(b.name()));
        defs.dedup_by(|a, b| a.name() == b.name());
        Ok(defs)
    }

    async fn list_types_by_category(&self, category: TypeCategory) -> Result<Vec<String>> {
        // No secondary index yet; do a prefix scan and filter in memory.
        let entries: Vec<(Vec<u8>, TypeDef)> = self
            .kv
            .scan_prefix(TYPE_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut names: Vec<String> = entries
            .into_iter()
            .filter_map(|(key, def)| {
                // Filter by category
                if !matches!(
                    (&def, category),
                    (TypeDef::Entity(_), TypeCategory::Entity)
                        | (TypeDef::Relationship(_), TypeCategory::Relationship)
                        | (TypeDef::Struct(_), TypeCategory::Struct)
                        | (TypeDef::Enum(_), TypeCategory::Enum)
                        | (TypeDef::Classification(_), TypeCategory::Classification)
                        | (TypeDef::BusinessMetadata(_), TypeCategory::BusinessMetadata)
                ) {
                    return None;
                }

                key.strip_prefix(TYPE_PREFIX)
                    .map(|suffix| String::from_utf8_lossy(suffix).to_string())
            })
            .collect();

        // Sort and dedup to ensure uniqueness (dedup only removes consecutive duplicates)
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn get_type_by_guid(&self, guid: &str) -> Result<TypeDef> {
        // Use GUID index for O(1) lookup instead of O(n) scan
        let guid_key = type_guid_index_key(guid);

        let type_name: String = self
            .kv
            .get(&guid_key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::TypeNotFound(format!("GUID: {}", guid)))?;

        // Now get the actual type definition by name
        self.get_type(&type_name).await
    }

    async fn batch_create_types(&self, type_defs: &[TypeDef]) -> Result<()> {
        validate_unique_type_names(type_defs.iter().map(|type_def| type_def.name()))?;

        // Build conditional checks and write operations atomically
        // This eliminates TOCTOU race between validation and write
        let mut checks = Vec::with_capacity(type_defs.len() * 2);
        let mut ops = Vec::with_capacity(type_defs.len() * 2);

        for type_def in type_defs {
            let key = type_key(type_def.name());
            let value =
                serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;
            checks.push(CheckOp::Absent { key: key.clone() });
            ops.push(WriteOp::Set { key, value });

            // Add GUID index
            let guid = type_guid(type_def);
            let guid_key = type_guid_index_key(&guid);
            let name_bytes = type_def.name().as_bytes().to_vec();
            checks.push(CheckOp::Absent {
                key: guid_key.clone(),
            });
            ops.push(WriteOp::Set {
                key: guid_key,
                value: name_bytes,
            });
        }

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| match e {
                crate::error::StorageError::AlreadyExists(key) => {
                    if key.starts_with("type_guid:") {
                        let guid = key.strip_prefix("type_guid:").unwrap_or(&key);
                        CoreError::TypeAlreadyExists(format!(
                            "Type with GUID '{}' already exists",
                            guid
                        ))
                    } else {
                        let type_name = key
                            .strip_prefix("type:")
                            .map(|s| s.to_string())
                            .unwrap_or(key);
                        CoreError::TypeAlreadyExists(type_name)
                    }
                }
                _ => CoreError::Storage(e.to_string()),
            })?;

        Ok(())
    }

    async fn batch_update_types(&self, type_defs: &[TypeDef]) -> Result<()> {
        validate_unique_type_names(type_defs.iter().map(|type_def| type_def.name()))?;

        // Read current values for all types first (for optimistic concurrency control)
        // Note: This is not atomic with the write, but provides best-effort conflict detection.
        // True atomicity would require snapshot isolation or distributed transactions.
        //
        // TODO: Optimize with batch read if KV store supports it (currently serial N calls).
        // This is O(N) serial I/O; for large batches, consider pipelining or parallel fetch.
        let mut checks = Vec::with_capacity(type_defs.len() * 2);
        let mut ops = Vec::with_capacity(type_defs.len() * 3);
        let mut missing_types = Vec::new();

        for type_def in type_defs {
            let key = type_key(type_def.name());
            let new_guid = type_guid(type_def);

            // Read current value
            let current: Option<Vec<u8>> = self
                .kv
                .get_raw(&key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?;

            match current {
                Some(value) => {
                    checks.push(CheckOp::ValueEquals {
                        key: key.clone(),
                        expected: value.clone(),
                    });

                    // Parse old type to get old GUID for index update
                    if let Ok(old_type_def) = serde_json::from_slice::<TypeDef>(&value) {
                        let old_guid = type_guid(&old_type_def);
                        if old_guid != new_guid {
                            // GUID changed: delete old index, add new index
                            let old_guid_key = type_guid_index_key(&old_guid);
                            let new_guid_key = type_guid_index_key(&new_guid);
                            let name_bytes = type_def.name().as_bytes().to_vec();

                            // Check old index exists (it should, since type exists)
                            checks.push(CheckOp::ValueEquals {
                                key: old_guid_key.clone(),
                                expected: name_bytes.clone(),
                            });

                            ops.push(WriteOp::Delete { key: old_guid_key });
                            ops.push(WriteOp::Set {
                                key: new_guid_key,
                                value: name_bytes,
                            });
                        }
                    }
                }
                None => {
                    missing_types.push(type_def.name().to_string());
                }
            }

            let new_value =
                serde_json::to_vec(type_def).map_err(|e| CoreError::Storage(e.to_string()))?;
            ops.push(WriteOp::Set {
                key,
                value: new_value,
            });
        }

        // If any type doesn't exist, fail early
        if !missing_types.is_empty() {
            return Err(CoreError::TypeNotFound(missing_types.join(", ")));
        }

        // Atomic conditional write
        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| match e {
                crate::error::StorageError::Conflict(_) => CoreError::Conflict(
                    "One or more types were modified by another request".to_string(),
                ),
                _ => CoreError::Storage(e.to_string()),
            })?;

        Ok(())
    }

    async fn batch_delete_types(&self, names: &[String]) -> Result<()> {
        validate_unique_type_names(names.iter().map(|s| s.as_str()))?;

        let mut checks = Vec::with_capacity(names.len());
        let mut ops = Vec::with_capacity(names.len() * 2);
        let mut missing_types = Vec::new();

        for name in names {
            let key = type_key(name);
            let current: Option<Vec<u8>> = self
                .kv
                .get_raw(&key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?;

            match current {
                Some(value) => {
                    checks.push(CheckOp::ValueEquals {
                        key: key.clone(),
                        expected: value.clone(),
                    });

                    // Parse type to get GUID for index deletion
                    if let Ok(type_def) = serde_json::from_slice::<TypeDef>(&value) {
                        let guid = type_guid(&type_def);
                        let guid_key = type_guid_index_key(&guid);
                        ops.push(WriteOp::Delete { key: guid_key });
                    }
                }
                None => {
                    missing_types.push(name.clone());
                }
            }

            ops.push(WriteOp::Delete { key });
        }

        if !missing_types.is_empty() {
            return Err(CoreError::TypeNotFound(missing_types.join(", ")));
        }

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| match e {
                crate::error::StorageError::Conflict(_) => CoreError::Conflict(
                    "One or more types were modified by another request".to_string(),
                ),
                _ => CoreError::Storage(e.to_string()),
            })?;

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

    #[test]
    fn test_validate_unique_type_names_rejects_duplicates() {
        let result = validate_unique_type_names(["A", "B", "A"]);
        assert!(matches!(result.unwrap_err(), CoreError::Validation(_)));
    }

    #[tokio::test]
    async fn test_batch_delete_types_success() {
        let store = create_test_store().await;

        let type_defs: Vec<TypeDef> = vec!["DropA", "DropB"]
            .into_iter()
            .map(|name| {
                TypeDef::from(EntityDef::new(name).attribute(AttributeDef::new("id", "string")))
            })
            .collect();
        store.batch_create_types(&type_defs).await.unwrap();

        let names = vec!["DropA".to_string(), "DropB".to_string()];
        store.batch_delete_types(&names).await.unwrap();

        assert!(!store.type_exists("DropA").await.unwrap());
        assert!(!store.type_exists("DropB").await.unwrap());
    }

    #[tokio::test]
    async fn test_batch_delete_types_missing_rollback() {
        let store = create_test_store().await;

        let existing =
            TypeDef::from(EntityDef::new("StillHere").attribute(AttributeDef::new("id", "string")));
        store.create_type(&existing).await.unwrap();

        let names = vec!["StillHere".to_string(), "Ghost".to_string()];
        let result = store.batch_delete_types(&names).await;
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));

        assert!(store.type_exists("StillHere").await.unwrap());
    }
}
