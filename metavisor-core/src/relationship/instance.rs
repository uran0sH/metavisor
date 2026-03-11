//! Relationship instance (AtlasRelationship)
//!
//! Full relationship with endpoints, attributes, and metadata

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::entity::{Classification, EntityHeader, ObjectId};
use crate::types::PropagateTags;

/// Relationship status (matches AtlasRelationship.Status)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RelationshipStatus {
    #[default]
    Active,
    Deleted,
}

impl RelationshipStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationshipStatus::Active => "ACTIVE",
            RelationshipStatus::Deleted => "DELETED",
        }
    }
}

impl std::fmt::Display for RelationshipStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for RelationshipStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ACTIVE" => Ok(RelationshipStatus::Active),
            "DELETED" => Ok(RelationshipStatus::Deleted),
            _ => Err(format!("Unknown relationship status: {}", s)),
        }
    }
}

// ============================================================================
// Relationship Instance
// ============================================================================

/// Relationship instance (AtlasRelationship)
///
/// Represents a relationship between two entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// Relationship type name (inherited from AtlasStruct)
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Relationship attributes (inherited from AtlasStruct)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, serde_json::Value>,

    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Home ID (for replicated relationships)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "homeId")]
    pub home_id: Option<String>,

    /// Provenance type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "provenanceType")]
    pub provenance_type: Option<i32>,

    /// End 1 of the relationship (entity reference)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end1: Option<ObjectId>,

    /// End 2 of the relationship (entity reference)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end2: Option<ObjectId>,

    /// Relationship label
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Propagate tags setting
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "propagateTags")]
    pub propagate_tags: Option<PropagateTags>,

    /// Relationship status
    #[serde(default)]
    pub status: RelationshipStatus,

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

    /// Classifications propagated through this relationship
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "propagatedClassifications")]
    pub propagated_classifications: Vec<Classification>,

    /// Classifications that are blocked from propagation
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "blockedPropagatedClassifications")]
    pub blocked_propagated_classifications: Vec<Classification>,
}

impl Relationship {
    /// Create a new relationship with the given type name
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            attributes: HashMap::new(),
            guid: None,
            home_id: None,
            provenance_type: None,
            end1: None,
            end2: None,
            label: None,
            propagate_tags: None,
            status: RelationshipStatus::default(),
            created_by: None,
            updated_by: None,
            create_time: None,
            update_time: None,
            version: None,
            propagated_classifications: Vec::new(),
            blocked_propagated_classifications: Vec::new(),
        }
    }

    /// Create a relationship between two entities
    pub fn between(type_name: impl Into<String>, end1: ObjectId, end2: ObjectId) -> Self {
        Self {
            type_name: type_name.into(),
            end1: Some(end1),
            end2: Some(end2),
            ..Self::new("")
        }
    }

    /// Set the GUID
    pub fn with_guid(mut self, guid: impl Into<String>) -> Self {
        self.guid = Some(guid.into());
        self
    }

    /// Set the type name
    pub fn with_type_name(mut self, type_name: impl Into<String>) -> Self {
        self.type_name = type_name.into();
        self
    }

    /// Set end1
    pub fn with_end1(mut self, end1: ObjectId) -> Self {
        self.end1 = Some(end1);
        self
    }

    /// Set end2
    pub fn with_end2(mut self, end2: ObjectId) -> Self {
        self.end2 = Some(end2);
        self
    }

    /// Set the label
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set propagate tags
    pub fn with_propagate_tags(mut self, propagate_tags: PropagateTags) -> Self {
        self.propagate_tags = Some(propagate_tags);
        self
    }

    /// Set the status
    pub fn with_status(mut self, status: RelationshipStatus) -> Self {
        self.status = status;
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }

    /// Get the relationship's GUID, or return an error if not set
    pub fn require_guid(&self) -> Result<&str, crate::CoreError> {
        self.guid.as_deref().ok_or_else(|| {
            crate::CoreError::Validation("Relationship GUID is required".to_string())
        })
    }

    /// Convert to RelationshipHeader
    pub fn to_header(&self) -> super::RelationshipHeader {
        super::RelationshipHeader {
            type_name: self.type_name.clone(),
            guid: self.guid.clone(),
            status: self.status,
            label: self.label.clone(),
            propagate_tags: self.propagate_tags,
            end1: self.end1.clone(),
            end2: self.end2.clone(),
            attributes: if !self.attributes.is_empty() {
                Some(self.attributes.clone())
            } else {
                None
            },
        }
    }
}

// ============================================================================
// Relationship With ExtInfo
// ============================================================================

/// Relationship with extended info (AtlasRelationshipWithExtInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipWithExtInfo {
    /// The relationship
    pub relationship: Relationship,

    /// Referred entities (entities at the endpoints)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "referredEntities")]
    pub referred_entities: HashMap<String, EntityHeader>,
}

impl RelationshipWithExtInfo {
    /// Create from a relationship
    pub fn new(relationship: Relationship) -> Self {
        Self {
            relationship,
            referred_entities: HashMap::new(),
        }
    }

    /// Add a referred entity
    pub fn add_referred(&mut self, entity: EntityHeader) {
        if let Some(ref guid) = entity.guid {
            self.referred_entities.insert(guid.clone(), entity);
        }
    }

    /// Get a referred entity by GUID
    pub fn get_referred(&self, guid: &str) -> Option<&EntityHeader> {
        self.referred_entities.get(guid)
    }

    /// Check if entity exists
    pub fn has_entity(&self, guid: &str) -> bool {
        self.referred_entities.contains_key(guid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::ObjectId;
    use serde_json::json;

    #[test]
    fn test_relationship_creation() {
        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        let rel = Relationship::between("table_columns", end1.clone(), end2.clone())
            .with_guid("rel-guid-1")
            .with_label("contains")
            .with_propagate_tags(PropagateTags::OneToTwo)
            .with_attribute("since", json!("2024-01-01"));

        assert_eq!(rel.type_name, "table_columns");
        assert_eq!(rel.guid, Some("rel-guid-1".to_string()));
        assert_eq!(rel.end1, Some(end1));
        assert_eq!(rel.end2, Some(end2));
        assert_eq!(rel.label, Some("contains".to_string()));
        assert_eq!(rel.propagate_tags, Some(PropagateTags::OneToTwo));
        assert!(rel.attributes.contains_key("since"));
    }

    #[test]
    fn test_relationship_serialization() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");

        let rel = Relationship::between("table_columns", end1, end2)
            .with_guid("rel-1")
            .with_status(RelationshipStatus::Active);

        let json_str = serde_json::to_string(&rel).unwrap();
        assert!(json_str.contains("\"typeName\":\"table_columns\""));
        assert!(json_str.contains("\"guid\":\"rel-1\""));
        assert!(json_str.contains("\"status\":\"ACTIVE\""));
    }

    #[test]
    fn test_relationship_to_header() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");

        let rel = Relationship::between("table_columns", end1.clone(), end2.clone())
            .with_guid("rel-1")
            .with_label("contains");

        let header = rel.to_header();
        assert_eq!(header.type_name, "table_columns");
        assert_eq!(header.guid, Some("rel-1".to_string()));
        assert_eq!(header.end1, Some(end1));
        assert_eq!(header.end2, Some(end2));
    }

    #[test]
    fn test_relationship_status() {
        assert_eq!(RelationshipStatus::Active.to_string(), "ACTIVE");
        assert_eq!(RelationshipStatus::Deleted.to_string(), "DELETED");
        assert_eq!(
            "ACTIVE".parse::<RelationshipStatus>().unwrap(),
            RelationshipStatus::Active
        );
        assert_eq!(
            "DELETED".parse::<RelationshipStatus>().unwrap(),
            RelationshipStatus::Deleted
        );
    }

    #[test]
    fn test_relationship_with_ext_info() {
        let rel = Relationship::new("table_columns").with_guid("rel-1");

        let mut ext = RelationshipWithExtInfo::new(rel);
        let entity = EntityHeader::new("Table").with_guid("table-1");
        ext.add_referred(entity);

        assert!(ext.has_entity("table-1"));
        assert!(ext.get_referred("table-1").is_some());
    }
}
