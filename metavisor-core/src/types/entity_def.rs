//! Entity type definition (AtlasEntityDef)
//!
//! Contains:
//! - EntityDef - entity type definition
//! - RelationshipAttributeDef - relationship attribute definition

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::common::Cardinality;
use super::struct_def::AttributeDef;

// ============================================================================
// Relationship Attribute Definition
// ============================================================================

/// Relationship attribute definition (AtlasRelationshipAttributeDef)
/// Used in EntityDef to represent attributes that are relationships to other entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipAttributeDef {
    /// Relationship type name
    #[serde(rename = "relationshipTypeName")]
    pub relationship_type_name: String,

    /// Whether this is a legacy attribute
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isLegacyAttribute")]
    pub is_legacy_attribute: Option<bool>,

    // Flattened attribute fields
    /// Attribute name
    pub name: String,

    /// Type name
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Whether this attribute is optional
    #[serde(default = "default_true")]
    #[serde(rename = "isOptional")]
    pub is_optional: bool,

    /// Cardinality
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<Cardinality>,

    /// Whether unique
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isUnique")]
    pub is_unique: Option<bool>,

    /// Whether indexed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isIndexable")]
    pub is_indexable: Option<bool>,

    /// Default value
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultValue")]
    pub default_value: Option<String>,

    /// Description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Search weight
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "searchWeight")]
    pub search_weight: Option<i32>,

    /// Options
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

fn default_true() -> bool {
    true
}

impl RelationshipAttributeDef {
    pub fn new(
        name: impl Into<String>,
        type_name: impl Into<String>,
        relationship_type_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            relationship_type_name: relationship_type_name.into(),
            is_legacy_attribute: None,
            is_optional: true,
            cardinality: None,
            is_unique: None,
            is_indexable: None,
            default_value: None,
            description: None,
            search_weight: None,
            options: HashMap::new(),
        }
    }

    pub fn required(mut self) -> Self {
        self.is_optional = false;
        self
    }

    pub fn cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = Some(cardinality);
        self
    }

    pub fn legacy(mut self) -> Self {
        self.is_legacy_attribute = Some(true);
        self
    }
}

// ============================================================================
// Entity Definition
// ============================================================================

/// Entity type definition (AtlasEntityDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityDef {
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

    /// Parent types (inheritance)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "superTypes")]
    pub super_types: Vec<String>,

    /// Sub types (read-only, derived from superTypes of other types)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "subTypes")]
    pub sub_types: Vec<String>,

    /// Whether this type is abstract (cannot create instances)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isAbstract")]
    pub is_abstract: Option<bool>,

    /// Attribute definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "attributeDefs")]
    pub attribute_defs: Vec<AttributeDef>,

    /// Relationship attribute definitions (read-only, derived from relationshipDefs)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "relationshipAttributeDefs")]
    pub relationship_attribute_defs: Vec<RelationshipAttributeDef>,

    /// Business attribute definitions (read-only, derived from businessMetadataDefs)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "businessAttributeDefs")]
    pub business_attribute_defs: HashMap<String, Vec<AttributeDef>>,

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

impl EntityDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            guid: None,
            description: None,
            type_version: None,
            service_type: None,
            super_types: Vec::new(),
            sub_types: Vec::new(),
            is_abstract: None,
            attribute_defs: Vec::new(),
            relationship_attribute_defs: Vec::new(),
            business_attribute_defs: HashMap::new(),
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

    pub fn super_type(mut self, name: impl Into<String>) -> Self {
        self.super_types.push(name.into());
        self
    }

    pub fn abstract_type(mut self) -> Self {
        self.is_abstract = Some(true);
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
    fn test_entity_def() {
        let entity = EntityDef::new("Table")
            .description("A database table")
            .super_type("Asset")
            .abstract_type()
            .attribute(AttributeDef::new("name", "string").required().unique());

        assert_eq!(entity.name, "Table");
        assert_eq!(entity.description, Some("A database table".to_string()));
        assert_eq!(entity.super_types, vec!["Asset"]);
        assert_eq!(entity.is_abstract, Some(true));
        assert_eq!(entity.attribute_defs.len(), 1);
    }

    #[test]
    fn test_relationship_attribute_def() {
        let rel_attr = RelationshipAttributeDef::new("columns", "array<Column>", "table_columns")
            .cardinality(Cardinality::Set);

        assert_eq!(rel_attr.name, "columns");
        assert_eq!(rel_attr.type_name, "array<Column>");
        assert_eq!(rel_attr.relationship_type_name, "table_columns");
        assert_eq!(rel_attr.cardinality, Some(Cardinality::Set));
    }
}
