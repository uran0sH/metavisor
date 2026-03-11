//! Relationship header (AtlasRelationshipHeader)
//!
//! Minimal relationship info for list operations

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::entity::ObjectId;
use crate::types::PropagateTags;

use super::instance::RelationshipStatus;

/// Relationship header (AtlasRelationshipHeader)
///
/// Minimal information about a relationship for list operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipHeader {
    /// Relationship type name (inherited from AtlasStruct)
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Relationship attributes (optional, inherited from AtlasStruct)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, serde_json::Value>>,

    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Relationship status
    #[serde(default)]
    pub status: RelationshipStatus,

    /// Relationship label
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Propagate tags setting
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "propagateTags")]
    pub propagate_tags: Option<PropagateTags>,

    /// End 1 of the relationship
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end1: Option<ObjectId>,

    /// End 2 of the relationship
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end2: Option<ObjectId>,
}

impl RelationshipHeader {
    /// Create a new relationship header
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            attributes: None,
            guid: None,
            status: RelationshipStatus::default(),
            label: None,
            propagate_tags: None,
            end1: None,
            end2: None,
        }
    }

    /// Create with GUID
    pub fn with_guid(mut self, guid: impl Into<String>) -> Self {
        self.guid = Some(guid.into());
        self
    }

    /// Set the status
    pub fn with_status(mut self, status: RelationshipStatus) -> Self {
        self.status = status;
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

    /// Check if this header has both endpoints set
    pub fn has_endpoints(&self) -> bool {
        self.end1.is_some() && self.end2.is_some()
    }

    /// Get the GUID of end1 entity
    pub fn end1_guid(&self) -> Option<&str> {
        self.end1.as_ref().and_then(|e| e.guid.as_deref())
    }

    /// Get the GUID of end2 entity
    pub fn end2_guid(&self) -> Option<&str> {
        self.end2.as_ref().and_then(|e| e.guid.as_deref())
    }

    /// Check if the given entity GUID is one of the endpoints
    pub fn has_endpoint(&self, entity_guid: &str) -> bool {
        self.end1_guid() == Some(entity_guid) || self.end2_guid() == Some(entity_guid)
    }

    /// Get the other endpoint given one endpoint GUID
    pub fn get_other_endpoint(&self, entity_guid: &str) -> Option<&ObjectId> {
        if self.end1_guid() == Some(entity_guid) {
            self.end2.as_ref()
        } else if self.end2_guid() == Some(entity_guid) {
            self.end1.as_ref()
        } else {
            None
        }
    }
}

/// List of relationship headers (AtlasRelationshipHeader.AtlasRelationshipHeaders)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipHeaders {
    /// List of relationship headers
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub list: Vec<RelationshipHeader>,

    /// Pagination start index
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "startIndex")]
    pub start_index: Option<i64>,

    /// Page size
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pageSize")]
    pub page_size: Option<i32>,

    /// Total count
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "totalCount")]
    pub total_count: Option<i64>,

    /// Sort type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "sortType")]
    pub sort_type: Option<String>,

    /// Sort by field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
}

impl RelationshipHeaders {
    /// Create empty list
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a vector
    pub fn from_vec(list: Vec<RelationshipHeader>) -> Self {
        Self {
            total_count: Some(list.len() as i64),
            list,
            ..Default::default()
        }
    }

    /// Add a header
    pub fn push(&mut self, header: RelationshipHeader) {
        self.list.push(header);
        self.total_count = Some(self.list.len() as i64);
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Get length
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Filter relationships by entity GUID (returns relationships where the entity is an endpoint)
    pub fn filter_by_entity(&self, entity_guid: &str) -> Vec<&RelationshipHeader> {
        self.list
            .iter()
            .filter(|h| h.has_endpoint(entity_guid))
            .collect()
    }

    /// Filter relationships by type name
    pub fn filter_by_type(&self, type_name: &str) -> Vec<&RelationshipHeader> {
        self.list
            .iter()
            .filter(|h| h.type_name == type_name)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relationship_header_creation() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");

        let header = RelationshipHeader::new("table_columns")
            .with_guid("rel-1")
            .with_status(RelationshipStatus::Active)
            .with_end1(end1.clone())
            .with_end2(end2.clone())
            .with_label("contains")
            .with_propagate_tags(PropagateTags::OneToTwo);

        assert_eq!(header.type_name, "table_columns");
        assert_eq!(header.guid, Some("rel-1".to_string()));
        assert!(header.has_endpoints());
        assert_eq!(header.end1_guid(), Some("table-1"));
        assert_eq!(header.end2_guid(), Some("column-1"));
    }

    #[test]
    fn test_get_other_endpoint() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");

        let header = RelationshipHeader::new("table_columns")
            .with_end1(end1.clone())
            .with_end2(end2.clone());

        let other = header.get_other_endpoint("table-1");
        assert!(other.is_some());
        assert_eq!(other.unwrap().guid, Some("column-1".to_string()));

        let other = header.get_other_endpoint("column-1");
        assert!(other.is_some());
        assert_eq!(other.unwrap().guid, Some("table-1".to_string()));

        let other = header.get_other_endpoint("unknown");
        assert!(other.is_none());
    }

    #[test]
    fn test_relationship_headers() {
        let mut headers = RelationshipHeaders::new();
        assert!(headers.is_empty());

        let h1 = RelationshipHeader::new("type1").with_guid("rel-1");
        let h2 = RelationshipHeader::new("type2").with_guid("rel-2");

        headers.push(h1);
        headers.push(h2);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers.total_count, Some(2));
    }

    #[test]
    fn test_filter_by_entity() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");
        let end3 = ObjectId::by_guid("Column", "column-2");

        let h1 = RelationshipHeader::new("table_columns")
            .with_guid("rel-1")
            .with_end1(end1.clone())
            .with_end2(end2.clone());

        let h2 = RelationshipHeader::new("table_columns")
            .with_guid("rel-2")
            .with_end1(end1.clone())
            .with_end2(end3.clone());

        let headers = RelationshipHeaders::from_vec(vec![h1, h2]);

        let filtered = headers.filter_by_entity("table-1");
        assert_eq!(filtered.len(), 2);

        let filtered = headers.filter_by_entity("column-1");
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_serialization() {
        let end1 = ObjectId::by_guid("Table", "table-1");
        let end2 = ObjectId::by_guid("Column", "column-1");

        let header = RelationshipHeader::new("table_columns")
            .with_guid("rel-1")
            .with_end1(end1)
            .with_end2(end2);

        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"typeName\":\"table_columns\""));
        assert!(json.contains("\"guid\":\"rel-1\""));
    }
}
