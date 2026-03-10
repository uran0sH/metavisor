//! Entity reference types (AtlasObjectId)
//!
//! Used to reference entities by GUID or by typeName + uniqueAttributes

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::CoreError;

/// Object ID for entity reference (AtlasObjectId)
///
/// Can reference an entity either by:
/// - GUID (unique identifier)
/// - typeName + uniqueAttributes (for entities with unique keys)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectId {
    /// Entity type name
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Unique identifier (optional if using uniqueAttributes)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Unique attributes for lookup (alternative to guid)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "uniqueAttributes")]
    pub unique_attributes: HashMap<String, serde_json::Value>,
}

impl ObjectId {
    /// Create a new ObjectId by GUID
    pub fn by_guid(type_name: impl Into<String>, guid: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            guid: Some(guid.into()),
            unique_attributes: HashMap::new(),
        }
    }

    /// Create a new ObjectId by unique attributes
    pub fn by_unique_attrs(
        type_name: impl Into<String>,
        unique_attributes: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            guid: None,
            unique_attributes,
        }
    }

    /// Check if this reference is valid (has either guid or unique attributes)
    pub fn is_valid(&self) -> bool {
        self.guid.is_some() || !self.unique_attributes.is_empty()
    }

    /// Get the primary key for lookup (guid or derived from unique attributes)
    pub fn primary_key(&self) -> Result<String, CoreError> {
        if let Some(ref guid) = self.guid {
            Ok(guid.clone())
        } else if !self.unique_attributes.is_empty() {
            // Generate a key from type_name + sorted unique attributes
            let mut attrs: Vec<_> = self.unique_attributes.iter().collect();
            attrs.sort_by_key(|(k, _)| *k);
            let attr_str = attrs
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            Ok(format!("{}:{}", self.type_name, attr_str))
        } else {
            Err(CoreError::Validation(
                "ObjectId must have either guid or uniqueAttributes".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_object_id_by_guid() {
        let id = ObjectId::by_guid("Table", "guid-123");
        assert_eq!(id.type_name, "Table");
        assert_eq!(id.guid, Some("guid-123".to_string()));
        assert!(id.unique_attributes.is_empty());
        assert!(id.is_valid());
    }

    #[test]
    fn test_object_id_by_attrs() {
        let mut attrs = HashMap::new();
        attrs.insert("name".to_string(), json!("users"));

        let id = ObjectId::by_unique_attrs("Table", attrs);
        assert_eq!(id.type_name, "Table");
        assert!(id.guid.is_none());
        assert!(id.is_valid());
    }

    #[test]
    fn test_object_id_serialization() {
        let id = ObjectId::by_guid("Table", "guid-123");
        let json = serde_json::to_string(&id).unwrap();
        assert!(json.contains("\"typeName\":\"Table\""));
        assert!(json.contains("\"guid\":\"guid-123\""));
    }
}
