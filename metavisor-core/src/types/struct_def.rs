//! Struct type definition (AtlasStructDef)
//!
//! Contains:
//! - StructDef - struct type definition
//! - AttributeDef - attribute definition
//! - ConstraintDef - constraint definition

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::common::{Cardinality, IndexType};

// ============================================================================
// Constraint Definition
// ============================================================================

/// Constraint definition (AtlasConstraintDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDef {
    /// Constraint type (e.g., "ownedRef", "mappedFromRef")
    #[serde(rename = "type")]
    pub constraint_type: String,

    /// Constraint parameters
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, serde_json::Value>,
}

impl ConstraintDef {
    pub fn new(constraint_type: impl Into<String>) -> Self {
        Self {
            constraint_type: constraint_type.into(),
            params: HashMap::new(),
        }
    }

    pub fn param(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.params.insert(key.into(), value);
        self
    }

    /// Create an "ownedRef" constraint (entity is owned by the referencing entity)
    pub fn owned_ref() -> Self {
        Self::new("ownedRef")
    }

    /// Create a "mappedFromRef" constraint
    pub fn mapped_from_ref(type_name: impl Into<String>) -> Self {
        Self::new("mappedFromRef").param("typeName", serde_json::Value::String(type_name.into()))
    }
}

// ============================================================================
// Attribute Definition
// ============================================================================

/// Attribute definition (AtlasAttributeDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDef {
    /// Attribute name
    pub name: String,

    /// Type name (e.g., "string", "array<string>", "map<string,int>")
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Whether this attribute is optional
    #[serde(default = "default_true")]
    #[serde(rename = "isOptional")]
    pub is_optional: bool,

    /// Cardinality: SINGLE, LIST, SET
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<Cardinality>,

    /// Minimum number of values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "valuesMinCount")]
    pub values_min_count: Option<i32>,

    /// Maximum number of values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "valuesMaxCount")]
    pub values_max_count: Option<i32>,

    /// Whether this attribute must be unique
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isUnique")]
    pub is_unique: Option<bool>,

    /// Whether this attribute is indexed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isIndexable")]
    pub is_indexable: Option<bool>,

    /// Include in notification events
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "includeInNotification")]
    pub include_in_notification: Option<bool>,

    /// Default value
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultValue")]
    pub default_value: Option<String>,

    /// Description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Search weight (higher = more important in search)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "searchWeight")]
    pub search_weight: Option<i32>,

    /// Index type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "indexType")]
    pub index_type: Option<IndexType>,

    /// Constraints for this attribute
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<ConstraintDef>,

    /// Options map
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,

    /// Display name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
}

fn default_true() -> bool {
    true
}

impl AttributeDef {
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            is_optional: true,
            cardinality: None,
            values_min_count: None,
            values_max_count: None,
            is_unique: None,
            is_indexable: None,
            include_in_notification: None,
            default_value: None,
            description: None,
            search_weight: None,
            index_type: None,
            constraints: Vec::new(),
            options: HashMap::new(),
            display_name: None,
        }
    }

    pub fn required(mut self) -> Self {
        self.is_optional = false;
        self.values_min_count = Some(1);
        self
    }

    pub fn optional(mut self) -> Self {
        self.is_optional = true;
        self
    }

    pub fn cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = Some(cardinality);
        match cardinality {
            Cardinality::Single => self.values_max_count = Some(1),
            Cardinality::List | Cardinality::Set => self.values_max_count = None,
        }
        self
    }

    pub fn unique(mut self) -> Self {
        self.is_unique = Some(true);
        self
    }

    pub fn indexed(mut self) -> Self {
        self.is_indexable = Some(true);
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn search_weight(mut self, weight: i32) -> Self {
        self.search_weight = Some(weight);
        self
    }

    pub fn constraint(mut self, constraint: ConstraintDef) -> Self {
        self.constraints.push(constraint);
        self
    }

    pub fn display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }
}

// ============================================================================
// Struct Definition
// ============================================================================

/// Struct type definition (AtlasStructDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructDef {
    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Type name
    pub name: String,

    /// Description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Type version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "typeVersion")]
    pub type_version: Option<String>,

    /// Service type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "serviceType")]
    pub service_type: Option<String>,

    /// Attribute definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "attributeDefs")]
    pub attribute_defs: Vec<AttributeDef>,

    /// Created by
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "createdBy")]
    pub created_by: Option<String>,

    /// Updated by
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "updatedBy")]
    pub updated_by: Option<String>,

    /// Create time (epoch milliseconds)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "createTime")]
    pub create_time: Option<i64>,

    /// Update time (epoch milliseconds)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "updateTime")]
    pub update_time: Option<i64>,

    /// Version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,

    /// Options
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

impl StructDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            guid: None,
            description: None,
            type_version: None,
            service_type: None,
            attribute_defs: Vec::new(),
            created_by: None,
            updated_by: None,
            create_time: None,
            update_time: None,
            version: None,
            options: HashMap::new(),
        }
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn attribute(mut self, attr: AttributeDef) -> Self {
        self.attribute_defs.push(attr);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_builder() {
        let attr = AttributeDef::new("name", "string")
            .required()
            .unique()
            .indexed()
            .default("unknown")
            .description("The name of the entity");

        assert_eq!(attr.name, "name");
        assert_eq!(attr.type_name, "string");
        assert!(!attr.is_optional);
        assert_eq!(attr.is_unique, Some(true));
        assert_eq!(attr.is_indexable, Some(true));
        assert!(attr.default_value.is_some());
        assert!(attr.description.is_some());
    }

    #[test]
    fn test_constraint() {
        let constraint = ConstraintDef::mapped_from_ref("SomeType");
        assert_eq!(constraint.constraint_type, "mappedFromRef");
        assert!(constraint.params.contains_key("typeName"));
    }

    #[test]
    fn test_struct_def() {
        let struct_def = StructDef::new("Address")
            .description("A physical address")
            .attribute(AttributeDef::new("street", "string").required())
            .attribute(AttributeDef::new("city", "string").required());

        assert_eq!(struct_def.name, "Address");
        assert_eq!(struct_def.attribute_defs.len(), 2);
    }
}
