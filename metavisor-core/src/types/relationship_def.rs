//! Relationship type definition (AtlasRelationshipDef)
//!
//! Contains:
//! - RelationshipDef - relationship type definition
//! - RelationshipEndDef - relationship end definition
//! - RelationshipCategory - relationship category enum
//! - PropagateTags - propagate tags enum

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::common::Cardinality;
use super::struct_def::AttributeDef;

// ============================================================================
// Relationship Category
// ============================================================================

/// Relationship category (AtlasRelationshipCategory)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RelationshipCategory {
    #[default]
    Association,
    Aggregation,
    Composition,
}

impl RelationshipCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationshipCategory::Association => "ASSOCIATION",
            RelationshipCategory::Aggregation => "AGGREGATION",
            RelationshipCategory::Composition => "COMPOSITION",
        }
    }
}

impl std::str::FromStr for RelationshipCategory {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ASSOCIATION" => Ok(RelationshipCategory::Association),
            "AGGREGATION" => Ok(RelationshipCategory::Aggregation),
            "COMPOSITION" => Ok(RelationshipCategory::Composition),
            _ => Err(format!("Unknown relationship category: {}", s)),
        }
    }
}

impl std::fmt::Display for RelationshipCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Propagate Tags
// ============================================================================

/// Propagate tags direction (AtlasPropagateTags)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PropagateTags {
    #[default]
    None,
    OneToTwo,
    TwoToOne,
    Both,
}

impl PropagateTags {
    pub fn as_str(&self) -> &'static str {
        match self {
            PropagateTags::None => "NONE",
            PropagateTags::OneToTwo => "ONE_TO_TWO",
            PropagateTags::TwoToOne => "TWO_TO_ONE",
            PropagateTags::Both => "BOTH",
        }
    }
}

impl std::str::FromStr for PropagateTags {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "NONE" => Ok(PropagateTags::None),
            "ONE_TO_TWO" => Ok(PropagateTags::OneToTwo),
            "TWO_TO_ONE" => Ok(PropagateTags::TwoToOne),
            "BOTH" => Ok(PropagateTags::Both),
            _ => Err(format!("Unknown propagate tags: {}", s)),
        }
    }
}

impl std::fmt::Display for PropagateTags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Relationship End Definition
// ============================================================================

/// Relationship end definition (AtlasRelationshipEndDef)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipEndDef {
    /// Type name of the entity at this end
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub type_name: Option<String>,

    /// Name of this end
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Whether this end is a container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isContainer")]
    pub is_container: Option<bool>,

    /// Cardinality: SINGLE, LIST, SET
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<Cardinality>,

    /// Whether this is a legacy attribute
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isLegacyAttribute")]
    pub is_legacy_attribute: Option<bool>,

    /// Description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl RelationshipEndDef {
    pub fn new(type_name: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            type_name: Some(type_name.into()),
            name: Some(name.into()),
            ..Default::default()
        }
    }

    pub fn container(mut self) -> Self {
        self.is_container = Some(true);
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

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }
}

// ============================================================================
// Relationship Definition
// ============================================================================

/// Relationship type definition (AtlasRelationshipDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipDef {
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

    /// Relationship category: ASSOCIATION, AGGREGATION, COMPOSITION
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "relationshipCategory")]
    pub relationship_category: Option<RelationshipCategory>,

    /// Relationship label
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "relationshipLabel")]
    pub relationship_label: Option<String>,

    /// Propagation tags: NONE, ONE_TO_TWO, TWO_TO_ONE, BOTH
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "propagateTags")]
    pub propagate_tags: Option<PropagateTags>,

    /// Relationship end 1
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "endDef1")]
    pub end_def1: Option<RelationshipEndDef>,

    /// Relationship end 2
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "endDef2")]
    pub end_def2: Option<RelationshipEndDef>,

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

impl RelationshipDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            guid: None,
            description: None,
            type_version: None,
            service_type: None,
            relationship_category: None,
            relationship_label: None,
            propagate_tags: None,
            end_def1: None,
            end_def2: None,
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

    pub fn category(mut self, category: RelationshipCategory) -> Self {
        self.relationship_category = Some(category);
        self
    }

    pub fn propagate_tags(mut self, propagate: PropagateTags) -> Self {
        self.propagate_tags = Some(propagate);
        self
    }

    pub fn end1(mut self, end: RelationshipEndDef) -> Self {
        self.end_def1 = Some(end);
        self
    }

    pub fn end2(mut self, end: RelationshipEndDef) -> Self {
        self.end_def2 = Some(end);
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
    fn test_relationship_category() {
        assert_eq!(RelationshipCategory::Association.to_string(), "ASSOCIATION");
        assert_eq!(RelationshipCategory::Aggregation.to_string(), "AGGREGATION");
        assert_eq!(RelationshipCategory::Composition.to_string(), "COMPOSITION");
    }

    #[test]
    fn test_propagate_tags() {
        assert_eq!(PropagateTags::None.to_string(), "NONE");
        assert_eq!(PropagateTags::OneToTwo.to_string(), "ONE_TO_TWO");
        assert_eq!(PropagateTags::TwoToOne.to_string(), "TWO_TO_ONE");
        assert_eq!(PropagateTags::Both.to_string(), "BOTH");
    }

    #[test]
    fn test_relationship_end_def() {
        let end = RelationshipEndDef::new("Table", "columns")
            .cardinality(Cardinality::Set)
            .container();

        assert_eq!(end.type_name, Some("Table".to_string()));
        assert_eq!(end.name, Some("columns".to_string()));
        assert_eq!(end.cardinality, Some(Cardinality::Set));
        assert_eq!(end.is_container, Some(true));
    }

    #[test]
    fn test_relationship_def() {
        let rel = RelationshipDef::new("table_columns")
            .category(RelationshipCategory::Composition)
            .propagate_tags(PropagateTags::OneToTwo)
            .end1(RelationshipEndDef::new("Table", "columns").cardinality(Cardinality::Set))
            .end2(RelationshipEndDef::new("Column", "table").container());

        assert_eq!(rel.name, "table_columns");
        assert_eq!(
            rel.relationship_category,
            Some(RelationshipCategory::Composition)
        );
        assert_eq!(rel.propagate_tags, Some(PropagateTags::OneToTwo));
        assert!(rel.end_def1.is_some());
        assert!(rel.end_def2.is_some());
    }
}
