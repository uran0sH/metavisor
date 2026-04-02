//! EntityStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{
    entity_key, entity_type_index_key, entity_unique_index_key, CoreError, Entity, EntityHeader,
    EntityStore, Result, TypeDef, TypeStore,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use crate::kv::{KvStore, WriteOp};

/// KvEntityStore - EntityStore implementation using surrealkv
pub struct KvEntityStore {
    kv: KvStore,
    type_store: Arc<dyn TypeStore>,
}

impl KvEntityStore {
    /// Create a new KvEntityStore with type validation
    pub fn new(kv: KvStore, type_store: Arc<dyn TypeStore>) -> Self {
        Self { kv, type_store }
    }

    /// Generate a new GUID for an entity
    fn generate_guid() -> String {
        Uuid::new_v4().to_string()
    }

    /// Convert a JSON value to a string suitable for use as an index key component.
    /// Returns None for non-primitive types.
    fn attr_to_index_value(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Null => None,
            _ => None, // Array, Object — serialization ambiguous
        }
    }

    /// Parse a unique index key to extract attribute details for error messages.
    /// Key format: `entity_unique\0{type_name}\0{attr_name}\0{attr_value}`
    /// Uses null byte (\0) as separator to avoid ambiguity with values containing ':'
    fn parse_unique_violation(key_desc: &str) -> String {
        // Split by null byte (\0)
        let parts: Vec<&str> = key_desc.split('\0').collect();
        if parts.len() >= 4 {
            format!(
                "attribute '{}' with value '{}' already exists",
                parts[2],              // attr_name
                parts[3..].join("\0")  // attr_value (may contain \0 if original value had it)
            )
        } else {
            key_desc.to_string()
        }
    }

    /// Validate entity against its type definition
    async fn validate_entity(&self, entity: &Entity) -> Result<()> {
        // 1. Get type definition
        let type_def = self
            .type_store
            .get_type(&entity.type_name)
            .await
            .map_err(|_| CoreError::TypeNotFound(entity.type_name.clone()))?;

        // 2. Validate it's an Entity type (not Classification, Relationship, etc.)
        let entity_def = match type_def {
            TypeDef::Entity(ref def) => def,
            other => {
                return Err(CoreError::Validation(format!(
                    "Type '{}' is a {:?}, not an Entity type. Entities can only be created for Entity types.",
                    entity.type_name,
                    other.category()
                )));
            }
        };

        // 3. Collect all attribute definitions including inherited ones
        let attr_defs = self.collect_all_attributes(entity_def).await?;

        // 3. Validate required attributes
        for attr_def in &attr_defs {
            if !attr_def.is_optional && !entity.attributes.contains_key(&attr_def.name) {
                return Err(CoreError::Validation(format!(
                    "Required attribute '{}' is missing for type '{}'",
                    attr_def.name, entity.type_name
                )));
            }
        }

        // 4. Validate all entity attributes are defined in type (including inherited)
        for name in entity.attributes.keys() {
            if !attr_defs.iter().any(|a| &a.name == name) {
                return Err(CoreError::Validation(format!(
                    "Attribute '{}' is not defined for type '{}'",
                    name, entity.type_name
                )));
            }
        }

        // 5. Validate attribute types
        let mut seen_types = std::collections::HashSet::new();
        for (name, value) in &entity.attributes {
            if let Some(attr_def) = attr_defs.iter().find(|a| &a.name == name) {
                self.validate_attribute_type(&attr_def.type_name, value, name, &mut seen_types)
                    .await?;
            }
        }

        Ok(())
    }

    /// Collect all attribute definitions including inherited ones from super types
    /// Uses iterative approach to avoid recursion in async fn
    async fn collect_all_attributes(
        &self,
        entity_def: &metavisor_core::EntityDef,
    ) -> Result<Vec<metavisor_core::AttributeDef>> {
        let mut all_attrs = Vec::new();
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_types = std::collections::HashSet::new();
        let mut types_to_process: Vec<metavisor_core::EntityDef> = vec![entity_def.clone()];

        // Track the current type to detect circular inheritance
        seen_types.insert(entity_def.name.clone());

        while let Some(current_def) = types_to_process.pop() {
            // Add current type's attributes
            for attr in &current_def.attribute_defs {
                if seen_names.insert(attr.name.clone()) {
                    all_attrs.push(attr.clone());
                }
            }

            // Queue super types for processing (with cycle detection)
            for super_type_name in &current_def.super_types {
                // Skip if we've already processed this type (circular inheritance)
                if !seen_types.insert(super_type_name.clone()) {
                    continue;
                }

                if let Ok(TypeDef::Entity(super_def)) =
                    self.type_store.get_type(super_type_name).await
                {
                    types_to_process.push(super_def);
                }
            }
        }

        Ok(all_attrs)
    }

    /// Get unique attribute names for an entity type (including inherited from superTypes)
    /// Returns empty vec if type not found (for graceful handling)
    async fn get_unique_attributes(&self, type_name: &str) -> Result<Vec<String>> {
        let type_def = match self.type_store.get_type(type_name).await {
            Ok(td) => td,
            Err(CoreError::TypeNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let entity_def = match type_def {
            TypeDef::Entity(def) => def,
            _ => return Ok(Vec::new()),
        };

        // Collect all attributes including inherited from superTypes
        let all_attrs = self.collect_all_attributes(&entity_def).await?;

        let unique_attrs: Vec<String> = all_attrs
            .into_iter()
            .filter(|attr| attr.is_unique == Some(true))
            .map(|attr| attr.name)
            .collect();

        Ok(unique_attrs)
    }

    /// Build unique constraint checks and index write ops in a single pass.
    /// Returns `(checks, ops)` for the `conditional_batch_write` — no I/O, no TOCTOU.
    /// `exclude_guid`: when updating, skip the index key that currently belongs to this entity.
    fn build_unique_ops(
        entity: &Entity,
        is_delete: bool,
        unique_attrs: &[String],
        is_update: bool,
        existing_index_keys: &[(String, String)],
    ) -> (Vec<crate::kv::CheckOp>, Vec<WriteOp>) {
        use crate::kv::{CheckOp, WriteOp};

        let mut checks = Vec::new();
        let mut ops = Vec::new();

        for attr_name in unique_attrs {
            if let Some(value) = entity.attributes.get(attr_name) {
                let Some(attr_value) = Self::attr_to_index_value(value) else {
                    continue;
                };
                if attr_value.is_empty() {
                    continue;
                }

                let index_key = entity_unique_index_key(&entity.type_name, attr_name, &attr_value);

                // When updating: if this unique attr value is the same as the existing entity's,
                // the index key already exists and belongs to us — skip the check.
                let is_own = is_update
                    && existing_index_keys
                        .iter()
                        .any(|(name, val)| *name == *attr_name && *val == attr_value);

                if !is_own {
                    checks.push(CheckOp::Absent {
                        key: index_key.clone(),
                    });
                }

                // Value changed on update: delete old index entry
                if is_update && !is_own {
                    if let Some((_, old_val)) = existing_index_keys
                        .iter()
                        .find(|(name, _)| name == attr_name)
                    {
                        ops.push(WriteOp::Delete {
                            key: entity_unique_index_key(&entity.type_name, attr_name, old_val),
                        });
                    }
                }

                if is_delete {
                    ops.push(WriteOp::Delete { key: index_key });
                } else if let Some(ref guid) = entity.guid {
                    if let Ok(bytes) = serde_json::to_vec(guid) {
                        ops.push(WriteOp::Set {
                            key: index_key,
                            value: bytes,
                        });
                    }
                }
            } else if is_update {
                // Attribute was removed — delete its old index entry
                if let Some((_, old_val)) = existing_index_keys
                    .iter()
                    .find(|(name, _)| name == attr_name)
                {
                    ops.push(WriteOp::Delete {
                        key: entity_unique_index_key(&entity.type_name, attr_name, old_val),
                    });
                }
            }
        }

        (checks, ops)
    }

    /// Validate attribute value against its declared type
    fn validate_attribute_type<'a>(
        &'a self,
        type_name: &'a str,
        value: &'a serde_json::Value,
        attr_name: &'a str,
        seen_types: &'a mut std::collections::HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let is_valid = match type_name {
                "string" => value.is_string(),
                "int" | "long" | "integer" => value.is_i64() || value.is_u64(),
                "short" => value.is_i64() || value.is_u64(),
                "byte" => value.is_i64() || value.is_u64(),
                "float" | "double" => value.is_number(),
                "boolean" => value.is_boolean(),
                "date" | "datetime" => value.is_string(),
                _ => {
                    // Generic collection types: array<T>, map<K,V>
                    if let Some(element_type) = type_name
                        .strip_prefix("array<")
                        .and_then(|s| s.strip_suffix(">"))
                    {
                        self.validate_array_element_type(value, element_type, attr_name, seen_types)
                            .await?
                    } else if let Some(inner) = type_name
                        .strip_prefix("map<")
                        .and_then(|s| s.strip_suffix(">"))
                    {
                        let parts: Vec<&str> = inner.splitn(2, ',').collect();
                        if parts.len() == 2 {
                            self.validate_map_element_types(
                                value,
                                parts[0].trim(),
                                parts[1].trim(),
                                attr_name,
                                seen_types,
                            )
                            .await?
                        } else {
                            false
                        }
                    } else {
                        self.validate_custom_type(type_name, value, attr_name, seen_types)
                            .await?
                    }
                }
            };

            if !is_valid {
                return Err(CoreError::Validation(format!(
                    "Attribute '{}' has invalid type. Expected '{}', got value: {}",
                    attr_name,
                    type_name,
                    if value.is_string() {
                        format!("\"{}\"", value.as_str().unwrap_or(""))
                    } else {
                        value.to_string()
                    }
                )));
            }

            Ok(())
        })
    }

    /// Validate custom type values (Struct, Enum, Entity, Classification, Relationship, BusinessMetadata)
    fn validate_custom_type<'a>(
        &'a self,
        type_name: &'a str,
        value: &'a serde_json::Value,
        attr_name: &'a str,
        seen_types: &'a mut std::collections::HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            // Cycle detection: if we've already seen this type in the current validation chain,
            // perform only shallow validation to avoid infinite recursion.
            if !seen_types.insert(type_name.to_string()) {
                return match self.type_store.get_type(type_name).await {
                    Ok(TypeDef::Struct(_))
                    | Ok(TypeDef::Classification(_))
                    | Ok(TypeDef::BusinessMetadata(_)) => Ok(value.is_object()),
                    Ok(TypeDef::Enum(_)) => Ok(value.is_string()),
                    Ok(TypeDef::Entity(_)) | Ok(TypeDef::Relationship(_)) => Ok(value.is_string()
                        || value.as_object().is_some_and(|o| o.contains_key("guid"))),
                    _ => Ok(true),
                };
            }

            let result = match self.type_store.get_type(type_name).await {
                Ok(TypeDef::Struct(struct_def)) => {
                    if !value.is_object() {
                        return Ok(false);
                    }
                    let obj = value.as_object().unwrap();

                    // Support both AtlasStruct format and plain attribute map
                    let attrs = if obj.contains_key("typeName") && obj.contains_key("attributes") {
                        obj.get("attributes")
                            .and_then(|a| a.as_object())
                            .ok_or_else(|| {
                                CoreError::Validation(format!(
                                    "Attribute '{}' has invalid AtlasStruct format for type '{}'",
                                    attr_name, type_name
                                ))
                            })?
                    } else {
                        obj
                    };

                    // Validate required attributes
                    for attr_def in &struct_def.attribute_defs {
                        if !attr_def.is_optional && !attrs.contains_key(&attr_def.name) {
                            return Err(CoreError::Validation(format!(
                                "Required attribute '{}' is missing for struct type '{}' in attribute '{}'",
                                attr_def.name, type_name, attr_name
                            )));
                        }
                    }

                    // Validate all provided attributes
                    for (k, v) in attrs.iter() {
                        if let Some(attr_def) =
                            struct_def.attribute_defs.iter().find(|a| a.name == *k)
                        {
                            self.validate_attribute_type(
                                &attr_def.type_name,
                                v,
                                &format!("{}.{}", attr_name, k),
                                seen_types,
                            )
                            .await?;
                        } else {
                            return Err(CoreError::Validation(format!(
                                "Attribute '{}' is not defined for struct type '{}' in attribute '{}'",
                                k, type_name, attr_name
                            )));
                        }
                    }
                    Ok(true)
                }
                Ok(TypeDef::Enum(enum_def)) => {
                    if let Some(s) = value.as_str() {
                        let valid = enum_def.element_defs.iter().any(|e| e.value == s);
                        if !valid {
                            let valid_values: Vec<String> = enum_def
                                .element_defs
                                .iter()
                                .map(|e| e.value.clone())
                                .collect();
                            return Err(CoreError::Validation(format!(
                                "Invalid enum value '{}' for type '{}' in attribute '{}'. Valid values: {:?}",
                                s, type_name, attr_name, valid_values
                            )));
                        }
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                Ok(TypeDef::Entity(_)) => {
                    Ok(value.is_string()
                        || value.as_object().is_some_and(|o| o.contains_key("guid")))
                }
                Ok(TypeDef::Classification(class_def)) => {
                    if !value.is_object() {
                        return Ok(false);
                    }
                    let obj = value.as_object().unwrap();

                    // Classification should have matching typeName if present
                    if let Some(tn) = obj.get("typeName").and_then(|v| v.as_str()) {
                        if tn != class_def.name {
                            return Ok(false);
                        }
                    }

                    // Validate attributes if present
                    let attrs = if let Some(attrs_val) = obj.get("attributes") {
                        attrs_val.as_object().ok_or_else(|| {
                            CoreError::Validation(format!(
                                "Attribute '{}' has invalid classification attributes for type '{}'",
                                attr_name, type_name
                            ))
                        })?
                    } else {
                        obj
                    };

                    for attr_def in &class_def.attribute_defs {
                        if !attr_def.is_optional && !attrs.contains_key(&attr_def.name) {
                            return Err(CoreError::Validation(format!(
                                "Required attribute '{}' is missing for classification type '{}' in attribute '{}'",
                                attr_def.name, type_name, attr_name
                            )));
                        }
                    }

                    for (k, v) in attrs.iter() {
                        if let Some(attr_def) =
                            class_def.attribute_defs.iter().find(|a| a.name == *k)
                        {
                            self.validate_attribute_type(
                                &attr_def.type_name,
                                v,
                                &format!("{}.{}", attr_name, k),
                                seen_types,
                            )
                            .await?;
                        } else if k != "typeName" {
                            return Err(CoreError::Validation(format!(
                                "Attribute '{}' is not defined for classification type '{}' in attribute '{}'",
                                k, type_name, attr_name
                            )));
                        }
                    }
                    Ok(true)
                }
                Ok(TypeDef::Relationship(_)) => {
                    Ok(value.is_string()
                        || value.as_object().is_some_and(|o| o.contains_key("guid")))
                }
                Ok(TypeDef::BusinessMetadata(_)) => Ok(value.is_object()),
                Err(CoreError::TypeNotFound(_)) => Err(CoreError::Validation(format!(
                    "Unknown attribute type '{}' for attribute '{}'",
                    type_name, attr_name
                ))),
                Err(e) => Err(e),
            };

            seen_types.remove(type_name);
            result
        })
    }

    /// Validate array elements match the expected element type
    fn validate_array_element_type<'a>(
        &'a self,
        value: &'a serde_json::Value,
        element_type: &'a str,
        attr_name: &'a str,
        seen_types: &'a mut std::collections::HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            if !value.is_array() {
                return Ok(false);
            }

            let arr = value.as_array().unwrap();
            for (i, elem) in arr.iter().enumerate() {
                match self
                    .validate_attribute_type(
                        element_type,
                        elem,
                        &format!("{}[{}]", attr_name, i),
                        seen_types,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CoreError::Validation(msg)) => {
                        return Err(CoreError::Validation(format!(
                            "Attribute '{}' array element at index {} has invalid type. Expected '{}', got: {}. {}",
                            attr_name, i, element_type, elem, msg
                        )));
                    }
                    Err(e) => return Err(e),
                }
            }

            Ok(true)
        })
    }

    /// Validate map key and value types
    fn validate_map_element_types<'a>(
        &'a self,
        value: &'a serde_json::Value,
        key_type: &'a str,
        value_type: &'a str,
        attr_name: &'a str,
        seen_types: &'a mut std::collections::HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            if !value.is_object() {
                return Ok(false);
            }

            let obj = value.as_object().unwrap();
            for (k, v) in obj.iter() {
                // Validate key type (only string keys are supported for JSON maps)
                if key_type == "string" && k.is_empty() {
                    return Err(CoreError::Validation(format!(
                        "Attribute '{}' map contains empty key",
                        attr_name
                    )));
                }

                // Validate value type recursively
                match self
                    .validate_attribute_type(
                        value_type,
                        v,
                        &format!("{}[{}]", attr_name, k),
                        seen_types,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(CoreError::Validation(msg)) => {
                        return Err(CoreError::Validation(format!(
                            "Attribute '{}' map value for key '{}' has invalid type. Expected '{}', got: {}. {}",
                            attr_name, k, value_type, v, msg
                        )));
                    }
                    Err(e) => return Err(e),
                }
            }

            Ok(true)
        })
    }
}

#[async_trait]
impl EntityStore for KvEntityStore {
    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        // Validate entity against type definition
        self.validate_entity(entity).await?;

        // Generate GUID if not provided
        let guid = entity.guid.clone().unwrap_or_else(Self::generate_guid);

        let key = entity_key(&guid);

        // Prepare entity with GUID
        let mut entity_with_guid = entity.clone();
        entity_with_guid.guid = Some(guid.clone());

        // Fetch unique attribute names once, shared by check and index build
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Build precondition checks: entity key must not exist + unique attrs must not exist
        let (unique_checks, unique_ops) =
            Self::build_unique_ops(&entity_with_guid, false, &unique_attrs, false, &[]);
        let mut checks = Vec::with_capacity(1 + unique_checks.len());
        checks.push(crate::kv::CheckOp::Absent { key: key.clone() });
        checks.extend(unique_checks);

        let header = entity_with_guid.to_header();

        // Build atomic batch: entity data + type index + unique indices
        let entity_bytes =
            serde_json::to_vec(&entity_with_guid).map_err(|e| CoreError::Storage(e.to_string()))?;
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut ops = Vec::with_capacity(2 + unique_ops.len());
        ops.push(WriteOp::Set {
            key,
            value: entity_bytes,
        });
        ops.push(WriteOp::Set {
            key: entity_type_index_key(&entity.type_name, &guid),
            value: header_bytes,
        });
        ops.extend(unique_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if let crate::error::StorageError::AlreadyExists(ref key_desc) = e {
                    if key_desc.starts_with("entity_unique:\0") {
                        let detail = Self::parse_unique_violation(key_desc);
                        CoreError::Validation(format!(
                            "Unique constraint violation on type '{}': {}",
                            entity.type_name, detail
                        ))
                    } else {
                        CoreError::EntityAlreadyExists(guid.clone())
                    }
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(guid)
    }

    async fn get_entity(&self, guid: &str) -> Result<Entity> {
        let key = entity_key(guid);

        self.kv
            .get(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.to_string()))
    }

    async fn get_entity_by_unique_attrs(
        &self,
        type_name: &str,
        unique_attrs: &HashMap<String, serde_json::Value>,
    ) -> Result<Entity> {
        if unique_attrs.is_empty() {
            return Err(CoreError::Validation(format!(
                "At least one unique attribute is required for type '{}'",
                type_name
            )));
        }

        // Get unique attribute names for this type (including inherited)
        let unique_attr_names = self.get_unique_attributes(type_name).await?;

        // Validate that at least one provided attribute is unique
        let has_unique_attr = unique_attrs
            .keys()
            .any(|attr_name| unique_attr_names.contains(attr_name));

        if !has_unique_attr {
            return Err(CoreError::Validation(format!(
                "At least one unique attribute is required for lookup. \
                 Unique attributes for type '{}': {:?}",
                type_name, unique_attr_names
            )));
        }

        // Try to find using unique attribute index
        for (attr_name, attr_value) in unique_attrs {
            // Skip non-unique attributes
            if !unique_attr_names.contains(attr_name) {
                continue;
            }

            // Convert attribute value to string for index lookup
            let attr_value_str = match attr_value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => continue,
            };

            // Lookup in unique index
            let index_key = entity_unique_index_key(type_name, attr_name, &attr_value_str);
            if let Some(guid) = self
                .kv
                .get::<String>(&index_key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?
            {
                return self.get_entity(&guid).await;
            }
        }

        Err(CoreError::EntityNotFound(format!(
            "{} with unique attributes {:?}",
            type_name, unique_attrs
        )))
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        // Validate entity against type definition
        self.validate_entity(entity).await?;

        let guid = entity.guid.as_ref().ok_or_else(|| {
            CoreError::Validation("Entity GUID is required for update".to_string())
        })?;

        let key = entity_key(guid);

        // Read raw bytes for optimistic concurrency check and deserialize for validation.
        // Using raw bytes avoids serde round-trip mismatch (key order, float repr, etc.)
        let existing_bytes = self
            .kv
            .get_raw(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.clone()))?;

        let existing: Entity = serde_json::from_slice(&existing_bytes)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // typeName is immutable
        if existing.type_name != entity.type_name {
            return Err(CoreError::Validation(
                "Cannot change entity typeName. Delete and recreate the entity instead."
                    .to_string(),
            ));
        }

        // Fetch unique attribute names once, shared by check and index build
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Collect existing entity's unique index key values (to exclude self from checks)
        let existing_index_keys: Vec<(String, String)> = unique_attrs
            .iter()
            .filter_map(|name| {
                existing
                    .attributes
                    .get(name)
                    .and_then(Self::attr_to_index_value)
                    .map(|v| (name.clone(), v))
            })
            .collect();

        // Build precondition checks + index ops for unique constraints (atomic with writes)
        let (unique_checks, unique_ops) =
            Self::build_unique_ops(entity, false, &unique_attrs, true, &existing_index_keys);

        // Build checks: entity key must equal existing (optimistic locking) + unique constraints
        let mut checks = Vec::with_capacity(1 + unique_checks.len());
        checks.push(crate::kv::CheckOp::ValueEquals {
            key: key.clone(),
            expected: existing_bytes,
        });
        checks.extend(unique_checks);

        // Build atomic batch: entity data + type index + unique indices update
        let entity_bytes =
            serde_json::to_vec(entity).map_err(|e| CoreError::Storage(e.to_string()))?;
        let header = entity.to_header();
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut ops = vec![
            WriteOp::Set {
                key,
                value: entity_bytes,
            },
            WriteOp::Set {
                key: entity_type_index_key(&entity.type_name, guid),
                value: header_bytes,
            },
        ];

        // Add unique index ops (only changed values due to is_own exclusion)
        ops.extend(unique_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if let crate::error::StorageError::AlreadyExists(ref key_desc) = e {
                    let detail = Self::parse_unique_violation(key_desc);
                    CoreError::Validation(format!(
                        "Unique constraint violation on type '{}': {}",
                        entity.type_name, detail
                    ))
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        let key = entity_key(guid);

        // Read raw bytes for optimistic concurrency check and deserialize for index ops.
        // Using raw bytes avoids serde round-trip mismatch (key order, float repr, etc.)
        let entity_bytes = self
            .kv
            .get_raw(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.to_string()))?;

        let entity: Entity =
            serde_json::from_slice(&entity_bytes).map_err(|e| CoreError::Storage(e.to_string()))?;

        let checks = vec![crate::kv::CheckOp::ValueEquals {
            key: key.clone(),
            expected: entity_bytes,
        }];

        // Fetch unique attribute names once
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Build atomic batch: delete entity data + type index + unique indices
        let mut ops = vec![
            WriteOp::Delete { key },
            WriteOp::Delete {
                key: entity_type_index_key(&entity.type_name, guid),
            },
        ];

        // Remove unique indices
        let (_, delete_ops) = Self::build_unique_ops(&entity, true, &unique_attrs, false, &[]);
        ops.extend(delete_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if matches!(e, crate::error::StorageError::Conflict(_)) {
                    CoreError::Conflict(format!(
                        "Entity '{}' was modified by another request",
                        guid
                    ))
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(())
    }

    async fn entity_exists(&self, guid: &str) -> Result<bool> {
        let key = entity_key(guid);
        self.kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))
    }

    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>> {
        let prefix = format!("entity_type:{type_name}:");
        let mut entries: Vec<(Vec<u8>, EntityHeader)> = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut headers: Vec<EntityHeader> = entries.drain(..).map(|(_, header)| header).collect();
        headers.sort_by(|a, b| a.guid.cmp(&b.guid));
        Ok(headers)
    }

    async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
        // Scan entity_type index (stores EntityHeader directly) for better performance
        // This avoids deserializing full Entity objects just to extract headers
        const TYPE_PREFIX: &[u8] = b"entity_type:";

        let mut entries: Vec<(Vec<u8>, EntityHeader)> = self
            .kv
            .scan_prefix(TYPE_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut headers: Vec<EntityHeader> = entries.drain(..).map(|(_, header)| header).collect();
        headers.sort_by(|a, b| a.guid.cmp(&b.guid));
        Ok(headers)
    }

    /// Create multiple entities atomically.
    /// Either all entities are created successfully, or none are (all-or-nothing).
    async fn batch_create_entities(&self, entities: &[Entity]) -> Result<Vec<String>> {
        // Phase 1: Validate all entities and prepare data
        let mut entities_with_guid: Vec<(Entity, String)> = Vec::with_capacity(entities.len());
        let mut all_checks: Vec<crate::kv::CheckOp> = Vec::new();
        let mut all_ops: Vec<WriteOp> = Vec::new();

        // Track unique constraint violations within the batch
        let mut seen_unique_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for entity in entities {
            // Validate entity against type definition
            self.validate_entity(entity).await?;

            // Generate GUID if not provided
            let guid = entity.guid.clone().unwrap_or_else(Self::generate_guid);

            // Check for duplicate GUIDs within the batch
            if entities_with_guid.iter().any(|(_, g)| g == &guid) {
                return Err(CoreError::Validation(format!(
                    "Duplicate GUID '{}' within batch",
                    guid
                )));
            }

            // Prepare entity with GUID
            let mut entity_with_guid = entity.clone();
            entity_with_guid.guid = Some(guid.clone());

            // Get unique attributes for this entity type
            let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

            // Check for duplicate unique constraints within the batch
            for attr_name in &unique_attrs {
                if let Some(value) = entity.attributes.get(attr_name) {
                    if let Some(attr_value) = Self::attr_to_index_value(value) {
                        if !attr_value.is_empty() {
                            let unique_key = format!(
                                "{}:{}:{}:{}",
                                entity.type_name, attr_name, attr_value, guid
                            );
                            if !seen_unique_keys.insert(unique_key.clone()) {
                                return Err(CoreError::Validation(format!(
                                    "Duplicate unique attribute '{}' with value '{}' for type '{}' within batch",
                                    attr_name, attr_value, entity.type_name
                                )));
                            }
                        }
                    }
                }
            }

            // Build checks and ops for this entity
            let key = entity_key(&guid);
            let (unique_checks, unique_ops) =
                Self::build_unique_ops(&entity_with_guid, false, &unique_attrs, false, &[]);

            // Entity key must not exist
            all_checks.push(crate::kv::CheckOp::Absent { key: key.clone() });
            all_checks.extend(unique_checks);

            // Build entity data
            let entity_bytes = serde_json::to_vec(&entity_with_guid)
                .map_err(|e| CoreError::Storage(e.to_string()))?;
            let header = entity_with_guid.to_header();
            let header_bytes =
                serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;

            all_ops.push(WriteOp::Set {
                key,
                value: entity_bytes,
            });
            all_ops.push(WriteOp::Set {
                key: entity_type_index_key(&entity.type_name, &guid),
                value: header_bytes,
            });
            all_ops.extend(unique_ops);

            entities_with_guid.push((entity_with_guid, guid));
        }

        // Phase 2: Atomic write
        self.kv
            .conditional_batch_write(all_checks, all_ops)
            .await
            .map_err(|e| {
                if let crate::error::StorageError::AlreadyExists(ref key_desc) = e {
                    if key_desc.starts_with("entity_unique:\0") {
                        let detail = Self::parse_unique_violation(key_desc);
                        CoreError::Validation(format!("Unique constraint violation: {}", detail))
                    } else {
                        CoreError::EntityAlreadyExists(key_desc.clone())
                    }
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        // Extract and return GUIDs in order
        let guids: Vec<String> = entities_with_guid
            .into_iter()
            .map(|(_, guid)| guid)
            .collect();
        Ok(guids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{
        AttributeDef, Classification, EntityDef, EnumDef, EnumElementDef, StructDef,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use crate::type_store::KvTypeStore;

    async fn create_test_stores() -> (KvEntityStore, Arc<KvTypeStore>) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = KvEntityStore::new(kv, type_store.clone());
        (entity_store, type_store)
    }

    async fn create_test_type(type_store: &KvTypeStore) {
        let entity_def = EntityDef::new("TestTable")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("rowCount", "int"));
        let type_def = TypeDef::from(entity_def);
        type_store.create_type(&type_def).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_entity_with_valid_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("users"))
            .with_attribute("rowCount", json!(1000));

        let guid = store.create_entity(&entity).await.unwrap();
        assert!(!guid.is_empty());

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "TestTable");
        assert_eq!(retrieved.attributes.get("name"), Some(&json!("users")));
    }

    #[tokio::test]
    async fn test_create_entity_missing_required_attribute() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        // Missing required "name" attribute
        let entity = Entity::new("TestTable").with_attribute("rowCount", json!(1000));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err
            .to_string()
            .contains("Required attribute 'name' is missing"));
    }

    #[tokio::test]
    async fn test_create_entity_invalid_type_name() {
        let (store, _) = create_test_stores().await;

        // Type doesn't exist
        let entity = Entity::new("NonExistentType").with_attribute("name", json!("test"));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));
    }

    #[tokio::test]
    async fn test_create_entity_invalid_attribute_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        // rowCount should be int, but we pass a string
        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("users"))
            .with_attribute("rowCount", json!("not a number"));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains("invalid type"));
    }

    #[tokio::test]
    async fn test_update_entity_with_validation() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("original"))
            .with_attribute("rowCount", json!(100));

        let guid = store.create_entity(&entity).await.unwrap();

        // Valid update
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated
            .attributes
            .insert("rowCount".to_string(), json!(200));

        store.update_entity(&updated).await.unwrap();

        // Invalid update - missing required attribute
        updated.attributes.remove("name");
        let result = store.update_entity(&updated).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_entity_with_guid() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_guid("custom-guid-123")
            .with_attribute("name", json!("test"));

        let guid = store.create_entity(&entity).await.unwrap();
        assert_eq!(guid, "custom-guid-123");

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.guid, Some("custom-guid-123".to_string()));
    }

    #[tokio::test]
    async fn test_create_duplicate_entity() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_guid("dup-guid")
            .with_attribute("name", json!("test"));

        store.create_entity(&entity).await.unwrap();

        // Try to create again with same GUID
        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::EntityAlreadyExists(_)
        ));
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable").with_attribute("name", json!("temp"));

        let guid = store.create_entity(&entity).await.unwrap();
        assert!(store.entity_exists(&guid).await.unwrap());

        store.delete_entity(&guid).await.unwrap();
        assert!(!store.entity_exists(&guid).await.unwrap());

        // Try to get deleted entity
        let result = store.get_entity(&guid).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::EntityNotFound(_)));
    }

    #[tokio::test]
    async fn test_entity_with_classification() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("sensitive_data"))
            .with_classification(Classification::new("PII"))
            .with_label("confidential");

        let guid = store.create_entity(&entity).await.unwrap();

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.classifications.len(), 1);
        assert_eq!(retrieved.classifications[0].type_name, "PII");
        assert_eq!(retrieved.labels, vec!["confidential"]);
    }

    #[tokio::test]
    async fn test_entity_not_found() {
        let (store, _) = create_test_stores().await;

        let result = store.get_entity("nonexistent-guid").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::EntityNotFound(_)));
    }

    // ============================================================================
    // Unique Attribute Index Tests
    // ============================================================================

    async fn create_test_type_with_unique_attr(type_store: &KvTypeStore) {
        let entity_def = EntityDef::new("UniqueTestTable")
            .attribute(AttributeDef::new("name", "string").required().unique())
            .attribute(AttributeDef::new("code", "string").unique())
            .attribute(AttributeDef::new("rowCount", "int"));
        let type_def = TypeDef::from(entity_def);
        type_store.create_type(&type_def).await.unwrap();
    }

    #[tokio::test]
    async fn test_unique_index_lookup() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create an entity with unique attribute
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("test_table"))
            .with_attribute("code", json!("TT001"))
            .with_attribute("rowCount", json!(100));

        let guid = store.create_entity(&entity).await.unwrap();

        // Lookup by unique attribute using index
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("test_table"));

        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();

        assert_eq!(found.guid, Some(guid));
        assert_eq!(found.attributes.get("name"), Some(&json!("test_table")));
    }

    #[tokio::test]
    async fn test_unique_index_update() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("old_name"))
            .with_attribute("code", json!("TT001"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Update unique attribute
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated
            .attributes
            .insert("name".to_string(), json!("new_name"));

        store.update_entity(&updated).await.unwrap();

        // Old unique value should not find the entity
        let mut old_attrs = HashMap::new();
        old_attrs.insert("name".to_string(), json!("old_name"));
        let result = store
            .get_entity_by_unique_attrs("UniqueTestTable", &old_attrs)
            .await;
        assert!(result.is_err()); // Should not find

        // New unique value should find the entity
        let mut new_attrs = HashMap::new();
        new_attrs.insert("name".to_string(), json!("new_name"));
        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &new_attrs)
            .await
            .unwrap();
        assert_eq!(found.guid, Some(guid));
    }

    #[tokio::test]
    async fn test_unique_index_delete() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("to_delete"))
            .with_attribute("code", json!("TT001"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Verify entity can be found by unique attribute
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("to_delete"));
        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();
        assert_eq!(found.guid, Some(guid.clone()));

        // Delete entity
        store.delete_entity(&guid).await.unwrap();

        // Should not find deleted entity by unique attribute
        let result = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unique_index_multiple_attributes() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity with multiple unique attributes
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("my_table"))
            .with_attribute("code", json!("CODE123"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Lookup by first unique attribute
        let mut attrs1 = HashMap::new();
        attrs1.insert("name".to_string(), json!("my_table"));
        let found1 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs1)
            .await
            .unwrap();
        assert_eq!(found1.guid, Some(guid.clone()));

        // Lookup by second unique attribute
        let mut attrs2 = HashMap::new();
        attrs2.insert("code".to_string(), json!("CODE123"));
        let found2 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs2)
            .await
            .unwrap();
        assert_eq!(found2.guid, Some(guid.clone()));

        // Lookup by both unique attributes (composite lookup)
        let mut attrs3 = HashMap::new();
        attrs3.insert("name".to_string(), json!("my_table"));
        attrs3.insert("code".to_string(), json!("CODE123"));
        let found3 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs3)
            .await
            .unwrap();
        assert_eq!(found3.guid, Some(guid));
    }

    #[tokio::test]
    async fn test_unique_index_performance() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create 100 entities
        for i in 0..100 {
            let entity = Entity::new("UniqueTestTable")
                .with_attribute("name", json!(format!("table_{}", i)))
                .with_attribute("code", json!(format!("CODE{}", i)));
            store.create_entity(&entity).await.unwrap();
        }

        // Lookup by unique attribute should be O(1) regardless of entity count
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("table_50"));

        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();

        assert_eq!(found.attributes.get("name"), Some(&json!("table_50")));
    }

    #[tokio::test]
    async fn test_unique_constraint_violation_on_create() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create first entity with unique attribute
        let entity1 = Entity::new("UniqueTestTable").with_attribute("name", json!("unique_name"));
        store.create_entity(&entity1).await.unwrap();

        // Attempt to create second entity with same unique attribute value
        let entity2 = Entity::new("UniqueTestTable").with_attribute("name", json!("unique_name"));
        let result = store.create_entity(&entity2).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unique constraint violation"),
            "Expected unique constraint error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_unique_constraint_violation_on_update() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create two entities with different unique values
        let entity1 = Entity::new("UniqueTestTable").with_attribute("name", json!("name_a"));
        let entity2 = Entity::new("UniqueTestTable").with_attribute("name", json!("name_b"));
        store.create_entity(&entity1).await.unwrap();
        let guid2 = store.create_entity(&entity2).await.unwrap();

        // Try to update entity2 to have the same unique value as entity1
        let mut updated = store.get_entity(&guid2).await.unwrap();
        updated
            .attributes
            .insert("name".to_string(), json!("name_a"));
        let result = store.update_entity(&updated).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unique constraint violation"),
            "Expected unique constraint error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_update_entity_keeps_own_unique_value() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity with unique attribute
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("my_name"))
            .with_attribute("code", json!("C001"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Update non-unique attribute — should succeed
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated.attributes.insert("rowCount".to_string(), json!(42));
        store.update_entity(&updated).await.unwrap();

        // Update unique attribute to a new value — should succeed
        let mut updated2 = store.get_entity(&guid).await.unwrap();
        updated2
            .attributes
            .insert("name".to_string(), json!("new_name"));
        store.update_entity(&updated2).await.unwrap();

        // Verify the update took effect
        let found = store.get_entity(&guid).await.unwrap();
        assert_eq!(found.attributes.get("name"), Some(&json!("new_name")));
    }

    #[tokio::test]
    async fn test_concurrent_create_unique_constraint() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;
        let store = Arc::new(store);

        // Spawn N concurrent creates with the same unique attribute value
        let mut handles = Vec::new();
        for _ in 0..10 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let entity =
                    Entity::new("UniqueTestTable").with_attribute("name", json!("race_value"));
                store.create_entity(&entity).await
            }));
        }

        let mut successes = 0usize;
        let mut violations = 0usize;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => successes += 1,
                Err(e) if e.to_string().contains("Unique constraint") => violations += 1,
                Err(_) => {}
            }
        }

        assert_eq!(
            successes, 1,
            "Expected exactly 1 successful create, got {}",
            successes
        );
        assert_eq!(
            successes + violations,
            10,
            "Expected remaining 9 to be constraint violations"
        );
    }

    #[tokio::test]
    async fn test_create_entity_with_struct_attribute() {
        let (store, type_store) = create_test_stores().await;

        // Create Address struct type
        let address_struct = StructDef::new("Address")
            .attribute(AttributeDef::new("street", "string").required())
            .attribute(AttributeDef::new("city", "string").required())
            .attribute(AttributeDef::new("zip", "string"));
        type_store
            .create_type(&TypeDef::from(address_struct))
            .await
            .unwrap();

        // Create Person entity type with Address attribute
        let person_def = EntityDef::new("Person")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("address", "Address"));
        type_store
            .create_type(&TypeDef::from(person_def))
            .await
            .unwrap();

        // Create entity with struct attribute (plain attribute map)
        let entity = Entity::new("Person")
            .with_attribute("name", json!("Alice"))
            .with_attribute(
                "address",
                json!({
                    "street": "123 Main St",
                    "city": "NYC"
                }),
            );

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.attributes.get("name"), Some(&json!("Alice")));
    }

    #[tokio::test]
    async fn test_create_entity_with_struct_attribute_missing_required() {
        let (store, type_store) = create_test_stores().await;

        let address_struct = StructDef::new("Address")
            .attribute(AttributeDef::new("street", "string").required())
            .attribute(AttributeDef::new("city", "string"));
        type_store
            .create_type(&TypeDef::from(address_struct))
            .await
            .unwrap();

        let person_def = EntityDef::new("Person")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new("address", "Address"));
        type_store
            .create_type(&TypeDef::from(person_def))
            .await
            .unwrap();

        // Missing required "street" in struct
        let entity = Entity::new("Person").with_attribute(
            "address",
            json!({
                "city": "NYC"
            }),
        );

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Required attribute 'street' is missing"));
    }

    #[tokio::test]
    async fn test_create_entity_with_struct_atlas_format() {
        let (store, type_store) = create_test_stores().await;

        let address_struct = StructDef::new("Address")
            .attribute(AttributeDef::new("street", "string").required())
            .attribute(AttributeDef::new("city", "string").required());
        type_store
            .create_type(&TypeDef::from(address_struct))
            .await
            .unwrap();

        let person_def =
            EntityDef::new("Person").attribute(AttributeDef::new("address", "Address"));
        type_store
            .create_type(&TypeDef::from(person_def))
            .await
            .unwrap();

        // AtlasStruct format: {"typeName": "Address", "attributes": {...}}
        let entity = Entity::new("Person").with_attribute(
            "address",
            json!({
                "typeName": "Address",
                "attributes": {
                    "street": "456 Oak Ave",
                    "city": "LA"
                }
            }),
        );

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert!(retrieved.attributes.contains_key("address"));
    }

    #[tokio::test]
    async fn test_create_entity_with_enum_attribute() {
        let (store, type_store) = create_test_stores().await;

        // Create Status enum type
        let status_enum = EnumDef::new("Status")
            .element(EnumElementDef::new("ACTIVE"))
            .element(EnumElementDef::new("INACTIVE"))
            .element(EnumElementDef::new("PENDING"));
        type_store
            .create_type(&TypeDef::from(status_enum))
            .await
            .unwrap();

        let task_def = EntityDef::new("Task")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("status", "Status"));
        type_store
            .create_type(&TypeDef::from(task_def))
            .await
            .unwrap();

        let entity = Entity::new("Task")
            .with_attribute("name", json!("cleanup"))
            .with_attribute("status", json!("ACTIVE"));

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.attributes.get("status"), Some(&json!("ACTIVE")));
    }

    #[tokio::test]
    async fn test_create_entity_with_invalid_enum_value() {
        let (store, type_store) = create_test_stores().await;

        let status_enum = EnumDef::new("Status")
            .element(EnumElementDef::new("ACTIVE"))
            .element(EnumElementDef::new("INACTIVE"));
        type_store
            .create_type(&TypeDef::from(status_enum))
            .await
            .unwrap();

        let task_def = EntityDef::new("Task")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new("status", "Status"));
        type_store
            .create_type(&TypeDef::from(task_def))
            .await
            .unwrap();

        let entity = Entity::new("Task")
            .with_attribute("name", json!("cleanup"))
            .with_attribute("status", json!("UNKNOWN"));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid enum value 'UNKNOWN'"));
        assert!(err.contains("ACTIVE") && err.contains("INACTIVE"));
    }

    #[tokio::test]
    async fn test_create_entity_with_array_of_struct() {
        let (store, type_store) = create_test_stores().await;

        let tag_struct = StructDef::new("Tag")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("color", "string"));
        type_store
            .create_type(&TypeDef::from(tag_struct))
            .await
            .unwrap();

        let asset_def = EntityDef::new("Asset")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new("tags", "array<Tag>"));
        type_store
            .create_type(&TypeDef::from(asset_def))
            .await
            .unwrap();

        let entity = Entity::new("Asset")
            .with_attribute("name", json!("image.png"))
            .with_attribute(
                "tags",
                json!([
                    {"name": "important", "color": "red"},
                    {"name": "archive"}
                ]),
            );

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.attributes.get("name"), Some(&json!("image.png")));
    }

    #[tokio::test]
    async fn test_create_entity_with_map_of_enum() {
        let (store, type_store) = create_test_stores().await;

        let priority_enum = EnumDef::new("Priority")
            .element(EnumElementDef::new("LOW"))
            .element(EnumElementDef::new("MEDIUM"))
            .element(EnumElementDef::new("HIGH"));
        type_store
            .create_type(&TypeDef::from(priority_enum))
            .await
            .unwrap();

        let project_def = EntityDef::new("Project")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new(
                "module_priorities",
                "map<string,Priority>",
            ));
        type_store
            .create_type(&TypeDef::from(project_def))
            .await
            .unwrap();

        let entity = Entity::new("Project")
            .with_attribute("name", json!("metavisor"))
            .with_attribute(
                "module_priorities",
                json!({
                    "core": "HIGH",
                    "ui": "MEDIUM"
                }),
            );

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert!(retrieved.attributes.contains_key("module_priorities"));
    }

    #[tokio::test]
    async fn test_create_entity_with_entity_reference() {
        let (store, type_store) = create_test_stores().await;

        // Create referenced type
        let db_def = EntityDef::new("Database").attribute(AttributeDef::new("name", "string"));
        type_store
            .create_type(&TypeDef::from(db_def))
            .await
            .unwrap();

        // Create referencing type
        let table_def = EntityDef::new("DBTable")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new("database", "Database"));
        type_store
            .create_type(&TypeDef::from(table_def))
            .await
            .unwrap();

        // Reference by GUID string
        let entity = Entity::new("DBTable")
            .with_attribute("name", json!("users"))
            .with_attribute("database", json!("db-guid-123"));

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(
            retrieved.attributes.get("database"),
            Some(&json!("db-guid-123"))
        );

        // Reference by ObjectId (Atlas format)
        let entity2 = Entity::new("DBTable")
            .with_attribute("name", json!("orders"))
            .with_attribute(
                "database",
                json!({"typeName": "Database", "guid": "db-guid-456"}),
            );

        let guid2 = store.create_entity(&entity2).await.unwrap();
        let retrieved2 = store.get_entity(&guid2).await.unwrap();
        assert!(retrieved2.attributes.contains_key("database"));
    }

    #[tokio::test]
    async fn test_nested_struct_validation() {
        let (store, type_store) = create_test_stores().await;

        let geo_struct = StructDef::new("GeoPoint")
            .attribute(AttributeDef::new("lat", "float").required())
            .attribute(AttributeDef::new("lon", "float").required());
        type_store
            .create_type(&TypeDef::from(geo_struct))
            .await
            .unwrap();

        let location_struct = StructDef::new("Location")
            .attribute(AttributeDef::new("name", "string"))
            .attribute(AttributeDef::new("coordinates", "GeoPoint"));
        type_store
            .create_type(&TypeDef::from(location_struct))
            .await
            .unwrap();

        let event_def = EntityDef::new("Event")
            .attribute(AttributeDef::new("title", "string"))
            .attribute(AttributeDef::new("location", "Location"));
        type_store
            .create_type(&TypeDef::from(event_def))
            .await
            .unwrap();

        let entity = Entity::new("Event")
            .with_attribute("title", json!("RustConf"))
            .with_attribute(
                "location",
                json!({
                    "name": "Convention Center",
                    "coordinates": {
                        "lat": 37.7749,
                        "lon": -122.4194
                    }
                }),
            );

        let guid = store.create_entity(&entity).await.unwrap();
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert!(retrieved.attributes.contains_key("location"));
    }
}
