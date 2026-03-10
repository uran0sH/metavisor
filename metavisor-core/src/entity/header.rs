//! Entity header (AtlasEntityHeader)
//!
//! Minimal entity information used in lists and references
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/AtlasEntityHeader.java

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Classification, EntityStatus, ObjectId};

/// Entity header with minimal information (AtlasEntityHeader)
///
/// Extends AtlasStruct (typeName, attributes) and adds header-specific fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityHeader {
    /// Entity type name (inherited from AtlasStruct)
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Attributes (inherited from AtlasStruct)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, serde_json::Value>,

    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Entity status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<EntityStatus>,

    /// Display text for the entity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "displayText")]
    pub display_text: Option<String>,

    /// Classification names applied to this entity
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "classificationNames")]
    pub classification_names: Vec<String>,

    /// Classifications applied to this entity
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classifications: Vec<Classification>,

    /// Meaning names (glossary term names)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "meaningNames")]
    pub meaning_names: Vec<String>,

    /// Meanings (glossary term references)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub meanings: Vec<ObjectId>,

    /// Whether this entity is incomplete
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isIncomplete")]
    pub is_incomplete: Option<bool>,

    /// Labels/tags on this entity
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

impl EntityHeader {
    /// Create a new entity header
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            attributes: HashMap::new(),
            guid: None,
            status: None,
            display_text: None,
            classification_names: Vec::new(),
            classifications: Vec::new(),
            meaning_names: Vec::new(),
            meanings: Vec::new(),
            is_incomplete: None,
            labels: Vec::new(),
        }
    }

    /// Set the GUID
    pub fn with_guid(mut self, guid: impl Into<String>) -> Self {
        self.guid = Some(guid.into());
        self
    }

    /// Set the display text
    pub fn with_display_text(mut self, text: impl Into<String>) -> Self {
        self.display_text = Some(text.into());
        self
    }

    /// Convert to ObjectId reference
    pub fn to_object_id(&self) -> Option<ObjectId> {
        self.guid
            .as_ref()
            .map(|g| ObjectId::by_guid(&self.type_name, g))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_header() {
        let header = EntityHeader::new("Table")
            .with_guid("guid-123")
            .with_display_text("users");

        assert_eq!(header.type_name, "Table");
        assert_eq!(header.guid, Some("guid-123".to_string()));
        assert_eq!(header.display_text, Some("users".to_string()));
    }
}
