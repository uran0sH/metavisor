//! Business metadata type definition (AtlasBusinessMetadataDef)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::struct_def::AttributeDef;

/// Business metadata type definition (AtlasBusinessMetadataDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessMetadataDef {
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

impl BusinessMetadataDef {
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
    fn test_business_metadata_def() {
        let bm = BusinessMetadataDef::new("QualityMetrics")
            .description("Data quality metrics")
            .attribute(AttributeDef::new("accuracy", "double"))
            .attribute(AttributeDef::new("completeness", "double"));

        assert_eq!(bm.name, "QualityMetrics");
        assert_eq!(bm.description, Some("Data quality metrics".to_string()));
        assert_eq!(bm.attribute_defs.len(), 2);
    }
}
