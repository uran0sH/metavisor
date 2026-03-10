//! Classification attached to entities (AtlasClassification)
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/AtlasClassification.java

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::EntityStatus;

/// Classification attached to an entity (AtlasClassification)
///
/// Extends AtlasStruct (typeName, attributes) and adds classification-specific fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Classification {
    /// Classification type name (inherited from AtlasStruct)
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Attributes of the classification (inherited from AtlasStruct)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, serde_json::Value>,

    /// Entity GUID this classification belongs to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "entityGuid")]
    pub entity_guid: Option<String>,

    /// Status of the entity this classification is attached to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "entityStatus")]
    pub entity_status: Option<EntityStatus>,

    /// Whether this classification should propagate
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub propagate: Option<bool>,

    /// Validity periods for this classification
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "validityPeriods")]
    pub validity_periods: Vec<TimeBoundary>,

    /// Whether to remove propagations when entity is deleted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "removePropagationsOnEntityDelete")]
    pub remove_propagations_on_entity_delete: Option<bool>,
}

/// Time boundary for classification validity (AtlasTimeBoundary)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct TimeBoundary {
    /// Start time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startTime: Option<String>,

    /// End time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endTime: Option<String>,

    /// Time zone
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeZone: Option<String>,
}

impl Classification {
    /// Create a new classification
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            attributes: HashMap::new(),
            entity_guid: None,
            entity_status: None,
            propagate: None,
            validity_periods: Vec::new(),
            remove_propagations_on_entity_delete: None,
        }
    }

    /// Set the entity GUID
    pub fn with_entity_guid(mut self, guid: impl Into<String>) -> Self {
        self.entity_guid = Some(guid.into());
        self
    }

    /// Set whether to propagate
    pub fn with_propagate(mut self, propagate: bool) -> Self {
        self.propagate = Some(propagate);
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_classification() {
        let class = Classification::new("PII")
            .with_entity_guid("guid-123")
            .with_propagate(true)
            .with_attribute("sensitivity", json!("HIGH"));

        assert_eq!(class.type_name, "PII");
        assert_eq!(class.entity_guid, Some("guid-123".to_string()));
        assert_eq!(class.propagate, Some(true));
        assert_eq!(class.attributes.get("sensitivity"), Some(&json!("HIGH")));
    }

    #[test]
    fn test_classification_serialization() {
        let class = Classification::new("PII").with_entity_guid("guid-123");
        let json_str = serde_json::to_string(&class).unwrap();
        assert!(json_str.contains("\"typeName\":\"PII\""));
        assert!(json_str.contains("\"entityGuid\":\"guid-123\""));
    }
}
