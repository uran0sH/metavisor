//! MCP (Model Context Protocol) server implementation using rmcp SDK
//!
//! Provides tools for AI assistants to interact with Metavisor metadata.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars::JsonSchema,
    service::RequestContext,
    tool, tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};

use metavisor_core::{
    AttributeDef, Entity, EntityDef, EntityHeader, EntityStore, ObjectId, Relationship,
    RelationshipHeader, RelationshipStore, TypeDef, TypeStore,
};

use crate::routes::AppCombinedState;

// ============================================================================
// MCP Server Handler
// ============================================================================

#[derive(Clone)]
pub struct MetavisorMcpServer {
    type_store: Arc<dyn TypeStore>,
    entity_store: Arc<dyn EntityStore>,
    relationship_store: Arc<dyn RelationshipStore>,
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

impl MetavisorMcpServer {
    pub fn new(state: AppCombinedState) -> Self {
        Self {
            type_store: state.type_store,
            entity_store: state.entity_store,
            relationship_store: state.relationship_store,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl MetavisorMcpServer {
    /// Search for data entities (tables, datasets, etc.) by their type name.
    #[tool(
        name = "search_entities",
        description = "Search for data entities (tables, datasets, etc.) by their type name. Returns a list of matching entities with their basic information."
    )]
    async fn search_entities(
        &self,
        Parameters(args): Parameters<SearchEntitiesArgs>,
    ) -> Result<CallToolResult, McpError> {
        let headers = self
            .entity_store
            .list_entities_by_type(&args.type_name)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if headers.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No entities found for type '{}'",
                args.type_name
            ))]));
        }

        let result = headers
            .iter()
            .map(format_entity_header)
            .collect::<Vec<_>>()
            .join("\n\n");

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Get detailed information about a specific data entity by its GUID.
    #[tool(
        name = "get_entity",
        description = "Get detailed information about a specific data entity by its GUID (unique identifier). Returns full entity details including attributes, classifications, and labels."
    )]
    async fn get_entity(
        &self,
        Parameters(args): Parameters<GetEntityArgs>,
    ) -> Result<CallToolResult, McpError> {
        let entity = self
            .entity_store
            .get_entity(&args.guid)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format_entity(
            &entity,
        ))]))
    }

    /// List all available type definitions in the metadata repository.
    #[tool(
        name = "list_types",
        description = "List all available type definitions in the metadata repository. Types define the structure of data entities (e.g., DataSet, Table, Column)."
    )]
    async fn list_types(&self) -> Result<CallToolResult, McpError> {
        let types = self
            .type_store
            .list_types()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if types.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No type definitions found in the repository.".to_string(),
            )]));
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Available type definitions ({}):\n\n{}",
            types.len(),
            types.join("\n")
        ))]))
    }

    /// Get detailed information about a specific type definition by name.
    #[tool(
        name = "get_type",
        description = "Get detailed information about a specific type definition by name. Shows the attribute definitions and structure of the type."
    )]
    async fn get_type(
        &self,
        Parameters(args): Parameters<GetTypeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let type_def = self
            .type_store
            .get_type(&args.name)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(
            format_type_def(&type_def),
        )]))
    }

    /// Create a new data entity with the specified type and attributes.
    #[tool(
        name = "create_entity",
        description = "Create a new data entity (table, dataset, etc.) with the specified type and attributes."
    )]
    async fn create_entity(
        &self,
        Parameters(args): Parameters<CreateEntityArgs>,
    ) -> Result<CallToolResult, McpError> {
        let attributes = if let serde_json::Value::Object(map) = args.attributes {
            map.into_iter().collect()
        } else {
            return Err(McpError::invalid_request(
                "attributes must be a JSON object".to_string(),
                None,
            ));
        };

        let mut entity = Entity::new(&args.type_name).with_attributes(attributes);

        if let Some(labels) = args.labels {
            for label in labels {
                entity = entity.with_label(label);
            }
        }

        let guid = self
            .entity_store
            .create_entity(&entity)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Entity created successfully with GUID: {}",
            guid
        ))]))
    }

    /// Update an existing data entity by its GUID.
    #[tool(
        name = "update_entity",
        description = "Update an existing data entity by its GUID. Modifies attributes and/or labels."
    )]
    async fn update_entity(
        &self,
        Parameters(args): Parameters<UpdateEntityArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Get existing entity to preserve status
        let existing = self
            .entity_store
            .get_entity(&args.guid)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        let attributes = if let serde_json::Value::Object(map) = args.attributes {
            map.into_iter().collect()
        } else {
            return Err(McpError::invalid_request(
                "attributes must be a JSON object".to_string(),
                None,
            ));
        };

        let mut entity = Entity::new(&args.type_name)
            .with_guid(&args.guid)
            .with_status(existing.status)
            .with_attributes(attributes);

        if let Some(labels) = args.labels {
            for label in labels {
                entity = entity.with_label(label);
            }
        }

        self.entity_store
            .update_entity(&entity)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Entity '{}' updated successfully",
            args.guid
        ))]))
    }

    /// Delete a data entity by its GUID.
    #[tool(
        name = "delete_entity",
        description = "Delete a data entity by its GUID."
    )]
    async fn delete_entity(
        &self,
        Parameters(args): Parameters<DeleteEntityArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.entity_store
            .delete_entity(&args.guid)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Entity '{}' deleted successfully",
            args.guid
        ))]))
    }

    /// Create a new entity type definition.
    #[tool(
        name = "create_entity_type",
        description = "Create a new entity type definition. Entity types define the structure of data assets like tables, datasets, columns, etc."
    )]
    async fn create_entity_type(
        &self,
        Parameters(args): Parameters<CreateEntityTypeArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Build attribute definitions
        let attribute_defs: Vec<AttributeDef> = args
            .attribute_defs
            .unwrap_or_default()
            .into_iter()
            .map(|attr| {
                let mut def = AttributeDef::new(&attr.name, &attr.type_name);
                if !attr.is_optional.unwrap_or(true) {
                    def = def.required();
                }
                if let Some(default) = attr.default_value {
                    def = def.default(default.to_string());
                }
                def
            })
            .collect();

        // Build entity definition
        let mut entity_def = EntityDef::new(&args.name);
        for super_type in args.super_types.unwrap_or_default() {
            entity_def = entity_def.super_type(super_type);
        }
        for attr in attribute_defs {
            entity_def = entity_def.attribute(attr);
        }

        let type_def = TypeDef::Entity(entity_def);

        self.type_store
            .create_type(&type_def)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Entity type '{}' created successfully",
            args.name
        ))]))
    }

    /// Update an existing entity type definition.
    #[tool(
        name = "update_entity_type",
        description = "Update an existing entity type definition. Can add new attributes or modify super types."
    )]
    async fn update_entity_type(
        &self,
        Parameters(args): Parameters<UpdateEntityTypeArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Get existing type
        let existing = self
            .type_store
            .get_type(&args.name)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        let TypeDef::Entity(mut entity_def) = existing else {
            return Err(McpError::invalid_request(
                format!("Type '{}' is not an entity type", args.name),
                None,
            ));
        };

        // Update super types if provided
        if let Some(super_types) = args.super_types {
            entity_def.super_types = super_types;
        }

        // Add new attributes if provided
        if let Some(attribute_defs) = args.attribute_defs {
            let new_attrs: Vec<AttributeDef> = attribute_defs
                .into_iter()
                .map(|attr| {
                    let mut def = AttributeDef::new(&attr.name, &attr.type_name);
                    if !attr.is_optional.unwrap_or(true) {
                        def = def.required();
                    }
                    def
                })
                .collect();

            // Collect existing names to avoid duplicates
            let existing_names: std::collections::HashSet<_> = entity_def
                .attribute_defs
                .iter()
                .map(|a| a.name.as_str())
                .collect();

            // Add new attributes that don't already exist
            let attrs_to_add: Vec<AttributeDef> = new_attrs
                .into_iter()
                .filter(|attr| !existing_names.contains(attr.name.as_str()))
                .collect();
            entity_def.attribute_defs.extend(attrs_to_add);
        }

        let type_def = TypeDef::Entity(entity_def);

        self.type_store
            .update_type(&type_def)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Entity type '{}' updated successfully",
            args.name
        ))]))
    }

    /// Delete a type definition by name.
    #[tool(
        name = "delete_type",
        description = "Delete a type definition by name. WARNING: This will fail if entities of this type exist."
    )]
    async fn delete_type(
        &self,
        Parameters(args): Parameters<DeleteTypeArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.type_store
            .delete_type(&args.name)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Type '{}' deleted successfully",
            args.name
        ))]))
    }

    // ========================================================================
    // Relationship Tools
    // ========================================================================

    /// Create a new relationship between two entities.
    #[tool(
        name = "create_relationship",
        description = "Create a new relationship between two entities. Relationships define how entities are connected (e.g., table contains columns, process inputs data)."
    )]
    async fn create_relationship(
        &self,
        Parameters(args): Parameters<CreateRelationshipArgs>,
    ) -> Result<CallToolResult, McpError> {
        let end1 = ObjectId::by_guid(&args.end1_type, &args.end1_guid);
        let end2 = ObjectId::by_guid(&args.end2_type, &args.end2_guid);

        let mut relationship = Relationship::between(&args.type_name, end1, end2);

        if let Some(label) = args.label {
            relationship = relationship.with_label(label);
        }

        if let Some(serde_json::Value::Object(map)) = args.attributes {
            for (key, value) in map {
                relationship = relationship.with_attribute(key, value);
            }
        }

        let guid = self
            .relationship_store
            .create_relationship(&relationship)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Relationship created successfully with GUID: {}",
            guid
        ))]))
    }

    /// Get detailed information about a specific relationship by its GUID.
    #[tool(
        name = "get_relationship",
        description = "Get detailed information about a specific relationship by its GUID. Returns full relationship details including endpoints and attributes."
    )]
    async fn get_relationship(
        &self,
        Parameters(args): Parameters<GetRelationshipArgs>,
    ) -> Result<CallToolResult, McpError> {
        let relationship = self
            .relationship_store
            .get_relationship(&args.guid)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(
            format_relationship(&relationship),
        )]))
    }

    /// Update an existing relationship by its GUID.
    #[tool(
        name = "update_relationship",
        description = "Update an existing relationship by its GUID. Can modify attributes and label."
    )]
    async fn update_relationship(
        &self,
        Parameters(args): Parameters<UpdateRelationshipArgs>,
    ) -> Result<CallToolResult, McpError> {
        // Get existing relationship to preserve endpoints and status
        let existing = self
            .relationship_store
            .get_relationship(&args.guid)
            .await
            .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

        let mut relationship = Relationship::new(&existing.type_name)
            .with_guid(&args.guid)
            .with_status(existing.status);

        // Preserve endpoints
        if let Some(end1) = existing.end1 {
            relationship = relationship.with_end1(end1);
        }
        if let Some(end2) = existing.end2 {
            relationship = relationship.with_end2(end2);
        }

        // Update label if provided, otherwise preserve existing
        if let Some(label) = args.label {
            relationship = relationship.with_label(label);
        } else if let Some(label) = existing.label {
            relationship = relationship.with_label(label);
        }

        // Update attributes if provided
        if let Some(attributes) = args.attributes {
            if let serde_json::Value::Object(map) = attributes {
                for (key, value) in map {
                    relationship = relationship.with_attribute(key, value);
                }
            }
        } else {
            // Preserve existing attributes
            for (key, value) in existing.attributes {
                relationship = relationship.with_attribute(key, value);
            }
        }

        self.relationship_store
            .update_relationship(&relationship)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Relationship '{}' updated successfully",
            args.guid
        ))]))
    }

    /// Delete a relationship by its GUID.
    #[tool(
        name = "delete_relationship",
        description = "Delete a relationship by its GUID."
    )]
    async fn delete_relationship(
        &self,
        Parameters(args): Parameters<DeleteRelationshipArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.relationship_store
            .delete_relationship(&args.guid)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Relationship '{}' deleted successfully",
            args.guid
        ))]))
    }

    /// List all relationships for a specific entity.
    #[tool(
        name = "list_relationships_by_entity",
        description = "List all relationships where the specified entity is an endpoint. Useful for finding all connections to a data asset."
    )]
    async fn list_relationships_by_entity(
        &self,
        Parameters(args): Parameters<ListRelationshipsByEntityArgs>,
    ) -> Result<CallToolResult, McpError> {
        let headers = self
            .relationship_store
            .list_relationships_by_entity(&args.entity_guid)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if headers.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No relationships found for entity '{}'",
                args.entity_guid
            ))]));
        }

        let result = headers
            .iter()
            .map(format_relationship_header)
            .collect::<Vec<_>>()
            .join("\n\n");

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// List all relationships of a specific type.
    #[tool(
        name = "list_relationships_by_type",
        description = "List all relationships of a specific type name. Useful for finding all instances of a particular relationship type."
    )]
    async fn list_relationships_by_type(
        &self,
        Parameters(args): Parameters<ListRelationshipsByTypeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let headers = self
            .relationship_store
            .list_relationships_by_type(&args.type_name)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if headers.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "No relationships found for type '{}'",
                args.type_name
            ))]));
        }

        let result = headers
            .iter()
            .map(format_relationship_header)
            .collect::<Vec<_>>()
            .join("\n\n");

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }
}

// ============================================================================
// ServerHandler Implementation
// ============================================================================

#[tool_handler]
impl ServerHandler for MetavisorMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            server_info: Implementation {
                name: "metavisor".to_string(),
                title: Some("Metavisor MCP Server".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Data governance and metadata management".to_string()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Metavisor MCP Server - Query and manage data governance metadata. \
                 Use search_entities to find data assets, get_entity to retrieve details, \
                 list_types to see available types."
                    .to_string(),
            ),
            ..Default::default()
        }
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let headers = self
            .entity_store
            .list_entities()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let resources: Vec<Resource> = headers
            .iter()
            .filter_map(|h| {
                h.guid.as_ref().map(|guid| {
                    RawResource {
                        uri: format!("metavisor://entity/{}", guid),
                        name: h.type_name.clone(),
                        title: None,
                        description: None,
                        mime_type: Some("application/json".to_string()),
                        size: None,
                        icons: None,
                        meta: None,
                    }
                    .no_annotation()
                })
            })
            .collect();

        Ok(ListResourcesResult {
            resources,
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        if let Some(guid) = request.uri.strip_prefix("metavisor://entity/") {
            let entity = self
                .entity_store
                .get_entity(guid)
                .await
                .map_err(|e| McpError::invalid_request(e.to_string(), None))?;

            let json = serde_json::to_string_pretty(&entity)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            Ok(ReadResourceResult {
                contents: vec![ResourceContents::text(json, &request.uri)],
            })
        } else {
            Err(McpError::invalid_request(
                format!("Invalid resource URI: {}", request.uri),
                None,
            ))
        }
    }
}

// ============================================================================
// Tool Argument Types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchEntitiesArgs {
    #[schemars(description = "The entity type name to search for")]
    pub type_name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetEntityArgs {
    #[schemars(description = "The GUID of the entity to retrieve")]
    pub guid: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetTypeArgs {
    #[schemars(description = "The name of the type definition to retrieve")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateEntityArgs {
    #[schemars(description = "The type name of the entity to create")]
    pub type_name: String,
    #[schemars(description = "Entity attributes as a JSON object")]
    pub attributes: serde_json::Value,
    #[schemars(description = "Optional labels/tags for the entity")]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateEntityArgs {
    #[schemars(description = "The GUID of the entity to update")]
    pub guid: String,
    #[schemars(description = "The type name of the entity")]
    pub type_name: String,
    #[schemars(description = "Updated entity attributes as a JSON object")]
    pub attributes: serde_json::Value,
    #[schemars(description = "Optional labels/tags for the entity")]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteEntityArgs {
    #[schemars(description = "The GUID of the entity to delete")]
    pub guid: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateEntityTypeArgs {
    #[schemars(description = "Name of the entity type (e.g., 'Table', 'Dataset')")]
    pub name: String,
    #[schemars(description = "Super types to inherit from (e.g., ['DataSet', 'Asset'])")]
    pub super_types: Option<Vec<String>>,
    #[schemars(description = "Attribute definitions for this type")]
    pub attribute_defs: Option<Vec<AttributeDefArgs>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateEntityTypeArgs {
    #[schemars(description = "Name of the entity type to update")]
    pub name: String,
    #[schemars(description = "Replace super types (optional)")]
    pub super_types: Option<Vec<String>>,
    #[schemars(description = "New attribute definitions to add (optional)")]
    pub attribute_defs: Option<Vec<AttributeDefArgs>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteTypeArgs {
    #[schemars(description = "Name of the type to delete")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AttributeDefArgs {
    #[schemars(description = "Attribute name")]
    pub name: String,
    #[schemars(description = "Attribute type name")]
    pub type_name: String,
    #[schemars(description = "Whether this attribute is optional")]
    pub is_optional: Option<bool>,
    #[schemars(description = "Default value for this attribute")]
    pub default_value: Option<serde_json::Value>,
}

// ============================================================================
// Relationship Tool Argument Types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CreateRelationshipArgs {
    #[schemars(
        description = "The relationship type name (e.g., 'table_columns', 'process_inputs')"
    )]
    pub type_name: String,
    #[schemars(description = "Type name of the first endpoint entity")]
    pub end1_type: String,
    #[schemars(description = "GUID of the first endpoint entity")]
    pub end1_guid: String,
    #[schemars(description = "Type name of the second endpoint entity")]
    pub end2_type: String,
    #[schemars(description = "GUID of the second endpoint entity")]
    pub end2_guid: String,
    #[schemars(description = "Optional label for the relationship")]
    pub label: Option<String>,
    #[schemars(description = "Optional relationship attributes as a JSON object")]
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetRelationshipArgs {
    #[schemars(description = "The GUID of the relationship to retrieve")]
    pub guid: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct UpdateRelationshipArgs {
    #[schemars(description = "The GUID of the relationship to update")]
    pub guid: String,
    #[schemars(description = "Optional new label for the relationship")]
    pub label: Option<String>,
    #[schemars(
        description = "Updated relationship attributes as a JSON object (replaces existing)"
    )]
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteRelationshipArgs {
    #[schemars(description = "The GUID of the relationship to delete")]
    pub guid: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListRelationshipsByEntityArgs {
    #[schemars(description = "The GUID of the entity to find relationships for")]
    pub entity_guid: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListRelationshipsByTypeArgs {
    #[schemars(description = "The relationship type name to filter by")]
    pub type_name: String,
}

// ============================================================================
// Formatting Helpers
// ============================================================================

fn format_entity_header(h: &EntityHeader) -> String {
    let guid = h.guid.as_deref().unwrap_or("unknown");
    let mut result = format!("**{}** (GUID: {})", h.type_name, guid);

    if !h.labels.is_empty() {
        result.push_str(&format!("\n  Labels: {}", h.labels.join(", ")));
    }

    if !h.classification_names.is_empty() {
        result.push_str(&format!(
            "\n  Classifications: {}",
            h.classification_names.join(", ")
        ));
    }

    if !h.attributes.is_empty() {
        result.push_str("\n  Attributes:");
        for (key, value) in &h.attributes {
            result.push_str(&format!("\n    - {}: {}", key, value));
        }
    }

    result
}

fn format_entity(e: &Entity) -> String {
    let mut result = format!(
        "# Entity: {}\n**GUID:** {}\n**Status:** {:?}\n",
        e.type_name,
        e.guid.as_deref().unwrap_or("unknown"),
        e.status
    );

    if !e.labels.is_empty() {
        result.push_str(&format!("\n## Labels\n{}\n", e.labels.join(", ")));
    }

    if !e.classifications.is_empty() {
        result.push_str("\n## Classifications\n");
        for c in &e.classifications {
            result.push_str(&format!("- **{}**\n", c.type_name));
        }
    }

    if !e.attributes.is_empty() {
        result.push_str("\n## Attributes\n");
        for (key, value) in &e.attributes {
            result.push_str(&format!("- **{}:** {}\n", key, value));
        }
    }

    result
}

fn format_type_def(t: &TypeDef) -> String {
    match t {
        TypeDef::Entity(def) => {
            let mut result = format!("# Entity Type: {}\n", def.name);

            if !def.super_types.is_empty() {
                result.push_str(&format!(
                    "\n**Super Types:** {}\n",
                    def.super_types.join(", ")
                ));
            }

            if !def.attribute_defs.is_empty() {
                result.push_str("\n## Attributes\n");
                for attr in &def.attribute_defs {
                    let required = if attr.is_optional { "" } else { " (required)" };
                    result.push_str(&format!(
                        "- **{}:** {}{}\n",
                        attr.name, attr.type_name, required
                    ));
                }
            }

            result
        }
        TypeDef::Classification(def) => {
            format!("# Classification Type: {}\n", def.name)
        }
        TypeDef::Enum(def) => {
            let mut result = format!("# Enum Type: {}\n", def.name);
            if !def.element_defs.is_empty() {
                result.push_str("\n## Values\n");
                for elem in &def.element_defs {
                    result.push_str(&format!("- {}\n", elem.value));
                }
            }
            result
        }
        TypeDef::Struct(def) => {
            let mut result = format!("# Struct Type: {}\n", def.name);
            if !def.attribute_defs.is_empty() {
                result.push_str("\n## Attributes\n");
                for attr in &def.attribute_defs {
                    let required = if attr.is_optional { "" } else { " (required)" };
                    result.push_str(&format!(
                        "- **{}:** {}{}\n",
                        attr.name, attr.type_name, required
                    ));
                }
            }
            result
        }
        TypeDef::Relationship(def) => {
            format!(
                "# Relationship Type: {}\n**Category:** {:?}\n",
                def.name, def.relationship_category
            )
        }
        TypeDef::BusinessMetadata(def) => {
            format!("# Business Metadata: {}\n", def.name)
        }
    }
}

fn format_relationship_header(h: &RelationshipHeader) -> String {
    let guid = h.guid.as_deref().unwrap_or("unknown");
    let mut result = format!("**{}** (GUID: {})", h.type_name, guid);

    if let Some(ref label) = h.label {
        result.push_str(&format!("\n  Label: {}", label));
    }

    result.push_str(&format!("\n  Status: {:?}", h.status));

    if let Some(ref end1) = h.end1 {
        let end1_guid = end1.guid.as_deref().unwrap_or("unknown");
        result.push_str(&format!("\n  End1: {} ({})", end1.type_name, end1_guid));
    }

    if let Some(ref end2) = h.end2 {
        let end2_guid = end2.guid.as_deref().unwrap_or("unknown");
        result.push_str(&format!("\n  End2: {} ({})", end2.type_name, end2_guid));
    }

    if let Some(ref attrs) = h.attributes {
        if !attrs.is_empty() {
            result.push_str("\n  Attributes:");
            for (key, value) in attrs {
                result.push_str(&format!("\n    - {}: {}", key, value));
            }
        }
    }

    result
}

fn format_relationship(r: &Relationship) -> String {
    let guid = r.guid.as_deref().unwrap_or("unknown");
    let mut result = format!(
        "# Relationship: {}\n**GUID:** {}\n**Status:** {:?}\n",
        r.type_name, guid, r.status
    );

    if let Some(ref label) = r.label {
        result.push_str(&format!("\n## Label\n{}\n", label));
    }

    if let Some(ref end1) = r.end1 {
        let end1_guid = end1.guid.as_deref().unwrap_or("unknown");
        result.push_str(&format!(
            "\n## End1\n**Type:** {}\n**GUID:** {}\n",
            end1.type_name, end1_guid
        ));
    }

    if let Some(ref end2) = r.end2 {
        let end2_guid = end2.guid.as_deref().unwrap_or("unknown");
        result.push_str(&format!(
            "\n## End2\n**Type:** {}\n**GUID:** {}\n",
            end2.type_name, end2_guid
        ));
    }

    if !r.attributes.is_empty() {
        result.push_str("\n## Attributes\n");
        for (key, value) in &r.attributes {
            result.push_str(&format!("- **{}:** {}\n", key, value));
        }
    }

    result
}

// ============================================================================
// HTTP Handler (Bridge to Axum)
// ============================================================================

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};

/// Handle MCP JSON-RPC requests over HTTP using rmcp
pub async fn handle_mcp_request(State(state): State<AppCombinedState>, body: String) -> Response {
    use tokio::io::duplex;

    // Create a duplex stream for communication
    let (mut client_tx, server_rx) = duplex(4096);
    let (mut client_rx, server_tx) = duplex(4096);

    // Create the MCP server
    let server = MetavisorMcpServer::new(state);

    // Spawn the server task
    let server_task = tokio::spawn(async move {
        let transport = (server_rx, server_tx);
        let server = server.serve(transport).await;
        if let Ok(server) = server {
            let _ = server.waiting().await;
        }
    });

    // Send the request to the server
    let request_bytes = format!("{}\n", body);
    use tokio::io::AsyncWriteExt;
    if let Err(e) = client_tx.write_all(request_bytes.as_bytes()).await {
        server_task.abort();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32603, "message": format!("Failed to send request: {}", e)}
            })),
        )
            .into_response();
    }
    let _ = client_tx.shutdown().await;

    // Read the response
    use tokio::io::AsyncReadExt;
    let mut response_bytes = Vec::new();
    if let Err(e) = client_rx.read_to_end(&mut response_bytes).await {
        server_task.abort();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32603, "message": format!("Failed to read response: {}", e)}
            })),
        )
            .into_response();
    }

    let _ = server_task.await;

    // Parse and return the response
    match String::from_utf8(response_bytes) {
        Ok(response_str) => match serde_json::from_str::<serde_json::Value>(&response_str) {
            Ok(json_response) => (StatusCode::OK, Json(json_response)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": -32603, "message": format!("Invalid JSON response: {}", e)}
                })),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32603, "message": format!("Invalid UTF-8 response: {}", e)}
            })),
        )
            .into_response(),
    }
}
