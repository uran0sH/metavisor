//! Types definition container (AtlasTypesDef)
//!
//! Container for all type definitions, corresponding to AtlasTypesDef in Apache Atlas.

use serde::{Deserialize, Serialize};

use super::business_metadata_def::BusinessMetadataDef;
use super::classification_def::ClassificationDef;
use super::common::TypeCategory;
use super::entity_def::EntityDef;
use super::enum_def::EnumDef;
use super::relationship_def::RelationshipDef;
use super::struct_def::StructDef;

// ============================================================================
// TypeDef - Unified enum for any type definition
// ============================================================================

/// Unified type definition that can hold any type category
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TypeDef {
    Entity(EntityDef),
    Struct(StructDef),
    Enum(EnumDef),
    Classification(ClassificationDef),
    Relationship(RelationshipDef),
    BusinessMetadata(BusinessMetadataDef),
}

impl TypeDef {
    /// Get the type name
    pub fn name(&self) -> &str {
        match self {
            TypeDef::Entity(def) => &def.name,
            TypeDef::Struct(def) => &def.name,
            TypeDef::Enum(def) => &def.name,
            TypeDef::Classification(def) => &def.name,
            TypeDef::Relationship(def) => &def.name,
            TypeDef::BusinessMetadata(def) => &def.name,
        }
    }

    /// Get the type category
    pub fn category(&self) -> TypeCategory {
        match self {
            TypeDef::Entity(_) => TypeCategory::Entity,
            TypeDef::Struct(_) => TypeCategory::Struct,
            TypeDef::Enum(_) => TypeCategory::Enum,
            TypeDef::Classification(_) => TypeCategory::Classification,
            TypeDef::Relationship(_) => TypeCategory::Relationship,
            TypeDef::BusinessMetadata(_) => TypeCategory::BusinessMetadata,
        }
    }

    /// Get the GUID
    pub fn guid(&self) -> Option<&str> {
        match self {
            TypeDef::Entity(def) => def.guid.as_deref(),
            TypeDef::Struct(def) => def.guid.as_deref(),
            TypeDef::Enum(def) => def.guid.as_deref(),
            TypeDef::Classification(def) => def.guid.as_deref(),
            TypeDef::Relationship(def) => def.guid.as_deref(),
            TypeDef::BusinessMetadata(def) => def.guid.as_deref(),
        }
    }
}

impl From<EntityDef> for TypeDef {
    fn from(def: EntityDef) -> Self {
        TypeDef::Entity(def)
    }
}

impl From<StructDef> for TypeDef {
    fn from(def: StructDef) -> Self {
        TypeDef::Struct(def)
    }
}

impl From<EnumDef> for TypeDef {
    fn from(def: EnumDef) -> Self {
        TypeDef::Enum(def)
    }
}

impl From<ClassificationDef> for TypeDef {
    fn from(def: ClassificationDef) -> Self {
        TypeDef::Classification(def)
    }
}

impl From<RelationshipDef> for TypeDef {
    fn from(def: RelationshipDef) -> Self {
        TypeDef::Relationship(def)
    }
}

impl From<BusinessMetadataDef> for TypeDef {
    fn from(def: BusinessMetadataDef) -> Self {
        TypeDef::BusinessMetadata(def)
    }
}

/// Container for all type definitions (AtlasTypesDef)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TypesDef {
    /// Enum type definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "enumDefs")]
    pub enum_defs: Vec<EnumDef>,

    /// Struct type definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "structDefs")]
    pub struct_defs: Vec<StructDef>,

    /// Classification type definitions
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        rename = "classificationDefs"
    )]
    pub classification_defs: Vec<ClassificationDef>,

    /// Entity type definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "entityDefs")]
    pub entity_defs: Vec<EntityDef>,

    /// Relationship type definitions
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        rename = "relationshipDefs"
    )]
    pub relationship_defs: Vec<RelationshipDef>,

    /// Business metadata type definitions
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        rename = "businessMetadataDefs"
    )]
    pub business_metadata_defs: Vec<BusinessMetadataDef>,
}

impl TypesDef {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.enum_defs.is_empty()
            && self.struct_defs.is_empty()
            && self.classification_defs.is_empty()
            && self.entity_defs.is_empty()
            && self.relationship_defs.is_empty()
            && self.business_metadata_defs.is_empty()
    }

    pub fn has_entity_def(&self, name: &str) -> bool {
        self.entity_defs.iter().any(|d| d.name == name)
    }

    pub fn has_classification_def(&self, name: &str) -> bool {
        self.classification_defs.iter().any(|d| d.name == name)
    }

    pub fn has_struct_def(&self, name: &str) -> bool {
        self.struct_defs.iter().any(|d| d.name == name)
    }

    pub fn has_enum_def(&self, name: &str) -> bool {
        self.enum_defs.iter().any(|d| d.name == name)
    }

    pub fn has_relationship_def(&self, name: &str) -> bool {
        self.relationship_defs.iter().any(|d| d.name == name)
    }

    pub fn has_business_metadata_def(&self, name: &str) -> bool {
        self.business_metadata_defs.iter().any(|d| d.name == name)
    }
}

/// Type header for list operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeHeader {
    /// Unique identifier
    pub guid: String,

    /// Type name
    pub name: String,

    /// Type category
    pub category: TypeCategory,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_types_def_empty() {
        let types_def = TypesDef::new();
        assert!(types_def.is_empty());
    }

    #[test]
    fn test_types_def_has_entity() {
        let mut types_def = TypesDef::new();
        types_def.entity_defs.push(EntityDef::new("Table"));

        assert!(types_def.has_entity_def("Table"));
        assert!(!types_def.has_entity_def("Column"));
        assert!(!types_def.is_empty());
    }

    #[test]
    fn test_serialization() {
        let mut types_def = TypesDef::new();
        types_def.entity_defs.push(EntityDef::new("Table"));

        let json = serde_json::to_string(&types_def).unwrap();
        let parsed: TypesDef = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.entity_defs.len(), 1);
        assert_eq!(parsed.entity_defs[0].name, "Table");
    }
}
