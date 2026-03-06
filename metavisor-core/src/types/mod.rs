//! Type system module (Atlas API v2 compatible)
//!
//! This module provides type definitions compatible with Apache Atlas API v2.
//!
//! # Structure
//!
//! - `common` - Common types: TypeCategory, Cardinality, IndexType
//! - `struct_def` - StructDef, AttributeDef, ConstraintDef
//! - `enum_def` - EnumDef, EnumElementDef
//! - `classification_def` - ClassificationDef
//! - `entity_def` - EntityDef, RelationshipAttributeDef
//! - `relationship_def` - RelationshipDef, RelationshipEndDef, RelationshipCategory, PropagateTags
//! - `business_metadata_def` - BusinessMetadataDef
//! - `types_def` - TypesDef (container), TypeHeader
//! - `data_type` - DataType utilities

pub mod business_metadata_def;
pub mod classification_def;
mod common;
mod data_type;
pub mod entity_def;
pub mod enum_def;
pub mod relationship_def;
pub mod struct_def;
mod types_def;

// Re-export all public types
pub use business_metadata_def::BusinessMetadataDef;
pub use classification_def::ClassificationDef;
pub use common::*;
pub use data_type::*;
pub use entity_def::{EntityDef, RelationshipAttributeDef};
pub use enum_def::{EnumDef, EnumElementDef};
pub use relationship_def::{
    PropagateTags, RelationshipCategory, RelationshipDef, RelationshipEndDef,
};
pub use struct_def::{AttributeDef, ConstraintDef, StructDef};
pub use types_def::{TypeDef, TypeHeader, TypesDef};
