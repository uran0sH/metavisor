//! Grafeo-based GraphStore implementation
//!
//! This implementation uses Grafeo graph database for efficient lineage traversal.

use async_trait::async_trait;

use std::path::PathBuf;

use metavisor_core::{CoreError, GraphStore, PropagateTags, Result};

use crate::error::StorageError;

/// Grafeo-based graph store
///
/// Pure graph operations layer — does not depend on KV stores.
pub struct GrafeoGraphStore {
    /// Grafeo database instance
    db: grafeo::GrafeoDB,
}

impl GrafeoGraphStore {
    /// Create a new in-memory GrafeoGraphStore
    pub fn new_in_memory() -> crate::Result<Self> {
        let db = grafeo::GrafeoDB::new_in_memory();
        Ok(Self { db })
    }

    /// Open a persistent GrafeoGraphStore at the given path
    pub fn open<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        // Create config with persistence
        let config = grafeo::Config {
            path: Some(path),
            ..Default::default()
        };

        let db = grafeo::GrafeoDB::with_config(config)
            .map_err(|e| StorageError::Graph(format!("Failed to open Grafeo: {}", e)))?;

        Ok(Self { db })
    }

    /// Convert PropagateTags to string for Grafeo storage
    fn propagate_tags_to_string(tags: PropagateTags) -> &'static str {
        match tags {
            PropagateTags::None => "NONE",
            PropagateTags::OneToTwo => "ONE_TO_TWO",
            PropagateTags::TwoToOne => "TWO_TO_ONE",
            PropagateTags::Both => "BOTH",
        }
    }

    /// Get value from row by column name
    fn get_column_value<'a>(
        result: &'a grafeo_engine::database::QueryResult,
        row_idx: usize,
        col_name: &str,
    ) -> Option<&'a grafeo::Value> {
        let col_idx = result.columns.iter().position(|c| c == col_name)?;
        result.rows.get(row_idx)?.get(col_idx)
    }
}

#[async_trait]
impl GraphStore for GrafeoGraphStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn add_entity_node(
        &self,
        entity_guid: &str,
        entity_type: &str,
        display_name: Option<&str>,
        classifications: Vec<String>,
    ) -> Result<()> {
        let display = display_name.unwrap_or("");
        let classifications_json =
            serde_json::to_string(&classifications).unwrap_or_else(|_| "[]".to_string());

        let cypher = format!(
            "MERGE (e:Entity {{guid: '{}'}}) \
             SET e.type_name = '{}', \
                 e.display_name = '{}', \
                 e.classifications = {}",
            entity_guid,
            entity_type.replace('\'', "\\'"),
            display.replace('\'', "\\'"),
            classifications_json
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        Ok(())
    }

    async fn remove_entity_node(&self, entity_guid: &str) -> Result<()> {
        let cypher = format!(
            "MATCH (e:Entity {{guid: '{}'}}) DETACH DELETE e",
            entity_guid
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        Ok(())
    }

    async fn update_entity_node(
        &self,
        entity_guid: &str,
        entity_type: &str,
        display_name: Option<&str>,
        classifications: Vec<String>,
    ) -> Result<bool> {
        let display = display_name.unwrap_or("");
        let classifications_json =
            serde_json::to_string(&classifications).unwrap_or_else(|_| "[]".to_string());

        let cypher = format!(
            "MATCH (e:Entity {{guid: '{}'}}) \
             SET e.type_name = '{}', \
                 e.display_name = '{}', \
                 e.classifications = {} \
             RETURN count(e) as count",
            entity_guid,
            entity_type.replace('\'', "\\'"),
            display.replace('\'', "\\'"),
            classifications_json
        );

        let result = self
            .db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        // Check if node was found and updated
        let count = Self::get_column_value(&result, 0, "count")
            .and_then(|v| match v {
                grafeo::Value::Int64(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);

        let updated = count > 0;

        Ok(updated)
    }

    async fn add_relationship_edge(
        &self,
        relationship_guid: &str,
        relationship_type: &str,
        from_guid: &str,
        to_guid: &str,
        propagate_tags: PropagateTags,
    ) -> Result<()> {
        let propagate_str = Self::propagate_tags_to_string(propagate_tags);

        // Ensure both nodes exist
        let ensure_nodes = format!(
            "MERGE (a:Entity {{guid: '{}'}}) \
             MERGE (b:Entity {{guid: '{}'}})",
            from_guid, to_guid
        );
        let _ = self.db.execute(&ensure_nodes);

        // Create relationship edge
        let cypher = format!(
            "MATCH (a:Entity {{guid: '{}'}}), (b:Entity {{guid: '{}'}}) \
             MERGE (a)-[r:{} {{rel_guid: '{}', propagate_tags: '{}'}}]->(b)",
            from_guid, to_guid, relationship_type, relationship_guid, propagate_str
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        Ok(())
    }

    async fn remove_relationship_edge(&self, relationship_guid: &str) -> Result<()> {
        let cypher = format!(
            "MATCH ()-[r {{rel_guid: '{}'}}]-() DELETE r",
            relationship_guid
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        Ok(())
    }

    fn node_count(&self) -> usize {
        let cypher = "MATCH (e:Entity) RETURN count(e) as count";
        self.db
            .execute(cypher)
            .ok()
            .and_then(|r| {
                Self::get_column_value(&r, 0, "count").and_then(|v| match v {
                    grafeo::Value::Int64(i) => Some(*i as usize),
                    _ => None,
                })
            })
            .unwrap_or(0)
    }

    fn edge_count(&self) -> usize {
        let cypher = "MATCH ()-[r]->() RETURN count(r) as count";
        self.db
            .execute(cypher)
            .ok()
            .and_then(|r| {
                Self::get_column_value(&r, 0, "count").and_then(|v| match v {
                    grafeo::Value::Int64(i) => Some(*i as usize),
                    _ => None,
                })
            })
            .unwrap_or(0)
    }

    fn list_node_guids(&self) -> Vec<String> {
        let cypher = "MATCH (e:Entity) RETURN e.guid as guid";
        match self.db.execute(cypher) {
            Ok(result) => (0..result.rows.len())
                .filter_map(|i| {
                    Self::get_column_value(&result, i, "guid").and_then(|v| match v {
                        grafeo::Value::String(s) => Some(s.to_string()),
                        _ => None,
                    })
                })
                .collect(),
            Err(e) => {
                tracing::warn!("Failed to list node guids: {}", e);
                Vec::new()
            }
        }
    }

    fn list_edge_guids(&self) -> Vec<String> {
        let cypher = "MATCH ()-[r]->() RETURN r.rel_guid as rel_guid";
        match self.db.execute(cypher) {
            Ok(result) => (0..result.rows.len())
                .filter_map(|i| {
                    Self::get_column_value(&result, i, "rel_guid").and_then(|v| match v {
                        grafeo::Value::String(s) => Some(s.to_string()),
                        _ => None,
                    })
                })
                .collect(),
            Err(e) => {
                tracing::warn!("Failed to list edge guids: {}", e);
                Vec::new()
            }
        }
    }
}

// Tests are in metavisor_store.rs and other integration test files
