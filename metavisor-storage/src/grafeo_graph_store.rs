//! Grafeo-based GraphStore implementation
//!
//! This implementation uses Grafeo graph database for efficient lineage traversal
//! while keeping entity/relationship details in KV store.
//!
//! # Concurrency Notes
//!
//! - Classification propagation is **lazy** - modifications only mark the graph as "dirty"
//! - Call `flush_propagation()` to explicitly trigger propagation when needed
//! - For bulk operations, use the batch methods to minimize propagation overhead
//! - Background propagation task can be spawned with `spawn_background_propagation()`

use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metavisor_core::{
    Classification, CoreError, EntityStore, GraphStore, LineageEdgeInfo,
    LineageNode, LineageQueryOptions, LineageResult, PropagateTags, RelationshipStore, Result,
    TraversalDirection,
};

use crate::error::StorageError;

/// Grafeo-based graph store with lazy classification propagation
///
/// Uses Grafeo for efficient graph traversal while delegating to KV store for entity details.
/// Classification propagation is deferred to avoid O(N²) complexity during bulk operations.
pub struct GrafeoGraphStore {
    /// Grafeo database instance
    db: grafeo::GrafeoDB,
    /// KV store for entity/relationship details
    entity_store: Arc<dyn EntityStore>,
    /// KV store for relationship operations
    relationship_store: Arc<dyn RelationshipStore>,
    /// Graph version incremented after each mutation
    graph_version: AtomicU64,
    /// Last graph version fully propagated
    propagated_version: AtomicU64,
    /// Guards concurrent propagation runs
    propagation_running: AtomicBool,
    /// Cached propagation input graph to avoid full KV reload on each flush
    propagation_graph: RwLock<PropagationGraph>,
}

#[derive(Debug, Clone, Default)]
struct PropagationGraph {
    outgoing: HashMap<String, Vec<(String, PropagateTags)>>,
    incoming: HashMap<String, Vec<(String, PropagateTags)>>,
    direct_classifications: HashMap<String, Vec<String>>,
}

impl PropagationGraph {
    fn merge_edge(&mut self, from_guid: String, to_guid: String, tags: PropagateTags) {
        self.outgoing
            .entry(from_guid.clone())
            .or_default()
            .push((to_guid.clone(), tags));
        self.incoming
            .entry(to_guid)
            .or_default()
            .push((from_guid, tags));
    }

    fn set_direct_classifications(&mut self, guid: String, classifications: Vec<String>) {
        if !classifications.is_empty() {
            self.direct_classifications.insert(guid, classifications);
        } else {
            self.direct_classifications.remove(&guid);
        }
    }

    fn remove_entity(&mut self, guid: &str) {
        self.direct_classifications.remove(guid);

        if let Some(outgoing_edges) = self.outgoing.remove(guid) {
            for (to_guid, _) in outgoing_edges {
                if let Some(entries) = self.incoming.get_mut(&to_guid) {
                    entries.retain(|(from_guid, _)| from_guid != guid);
                    if entries.is_empty() {
                        self.incoming.remove(&to_guid);
                    }
                }
            }
        }

        if let Some(incoming_edges) = self.incoming.remove(guid) {
            for (from_guid, _) in incoming_edges {
                if let Some(entries) = self.outgoing.get_mut(&from_guid) {
                    entries.retain(|(to_guid, _)| to_guid != guid);
                    if entries.is_empty() {
                        self.outgoing.remove(&from_guid);
                    }
                }
            }
        }
    }

    fn remove_relationship_edge(&mut self, relationship: &(String, String)) {
        let (from_guid, to_guid) = relationship;
        if let Some(entries) = self.outgoing.get_mut(from_guid) {
            entries.retain(|(to, _)| to != to_guid);
            if entries.is_empty() {
                self.outgoing.remove(from_guid);
            }
        }
        if let Some(entries) = self.incoming.get_mut(to_guid) {
            entries.retain(|(from, _)| from != from_guid);
            if entries.is_empty() {
                self.incoming.remove(to_guid);
            }
        }
    }
}

impl GrafeoGraphStore {
    /// Create a new in-memory GrafeoGraphStore
    pub fn new_in_memory(
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
    ) -> crate::Result<Self> {
        let db = grafeo::GrafeoDB::new_in_memory();
        Ok(Self {
            db,
            entity_store,
            relationship_store,
            graph_version: AtomicU64::new(0),
            propagated_version: AtomicU64::new(0),
            propagation_running: AtomicBool::new(false),
            propagation_graph: RwLock::new(PropagationGraph::default()),
        })
    }

    /// Open a persistent GrafeoGraphStore at the given path
    pub fn open<P: Into<PathBuf>>(
        path: P,
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
    ) -> crate::Result<Self> {
        let path = path.into();
        
        // Create config with persistence
        let config = grafeo::Config {
            path: Some(path),
            ..Default::default()
        };
        
        let db = grafeo::GrafeoDB::with_config(config)
            .map_err(|e| StorageError::Graph(format!("Failed to open Grafeo: {}", e)))?;
        
        Ok(Self {
            db,
            entity_store,
            relationship_store,
            graph_version: AtomicU64::new(0),
            propagated_version: AtomicU64::new(0),
            propagation_running: AtomicBool::new(false),
            propagation_graph: RwLock::new(PropagationGraph::default()),
        })
    }

    /// Check if classification propagation is pending
    pub fn is_propagation_pending(&self) -> bool {
        self.graph_version.load(Ordering::Acquire) > self.propagated_version.load(Ordering::Acquire)
    }

    /// Explicitly trigger classification propagation
    ///
    /// This is a potentially expensive O(N) operation that traverses the entire graph.
    /// It's called automatically during `get_all_classifications` if pending, but you can
    /// call it explicitly after bulk operations to ensure consistency.
    ///
    /// If propagation is not pending (no modifications since last propagation), this is a no-op.
    pub async fn flush_propagation(&self) -> Result<()> {
        loop {
            let target_version = self.graph_version.load(Ordering::Acquire);
            let current_version = self.propagated_version.load(Ordering::Acquire);

            if current_version >= target_version {
                return Ok(());
            }

            if self
                .propagation_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                tokio::task::yield_now().await;
                continue;
            }

            let result = self.propagate_classifications().await;
            self.propagated_version
                .store(target_version, Ordering::Release);
            self.propagation_running.store(false, Ordering::Release);
            result?;
        }
    }

    /// Mark propagation as pending (called after modifications)
    /// 
    /// This is safe to call at any time - propagation is lazy and will be
    /// executed by background task or next explicit flush.
    pub fn mark_propagation_pending(&self) {
        self.graph_version.fetch_add(1, Ordering::AcqRel);
    }

    /// Spawn a background task for periodic classification propagation
    ///
    /// This creates a tokio task that periodically checks if propagation is pending
    /// and flushes it. This is useful for ensuring classification propagation stays
    /// up-to-date without requiring explicit calls.
    ///
    /// # Arguments
    ///
    /// * `interval` - How often to check for pending propagation (default: 30 seconds)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use metavisor_storage::GrafeoGraphStore;
    ///
    /// let graph_store = Arc::new(GrafeoGraphStore::open(...)?);
    /// graph_store.spawn_background_propagation(Some(Duration::from_secs(30)));
    /// ```
    pub fn spawn_background_propagation(self: &Arc<Self>, interval: Option<Duration>) {
        let interval = interval.unwrap_or(Duration::from_secs(30));
        let store = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // Skip first tick

            loop {
                ticker.tick().await;

                if store.is_propagation_pending() {
                    tracing::debug!("Background propagation: flushing pending classifications");
                    if let Err(e) = store.flush_propagation().await {
                        tracing::error!("Background propagation failed: {}", e);
                    } else {
                        tracing::debug!("Background propagation completed successfully");
                    }
                }
            }
        });

        tracing::info!(
            "Started background classification propagation task (interval: {:?})",
            interval
        );
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
    fn get_column_value<'a>(result: &'a grafeo_engine::database::QueryResult, row_idx: usize, col_name: &str) -> Option<&'a grafeo::Value> {
        let col_idx = result.columns.iter().position(|c| c == col_name)?;
        result.rows.get(row_idx)?.get(col_idx)
    }

    /// Convert Value to String
    fn value_to_string(value: Option<&grafeo::Value>) -> String {
        match value {
            Some(grafeo::Value::String(s)) => s.to_string(),
            Some(grafeo::Value::Int64(i)) => i.to_string(),
            Some(grafeo::Value::Float64(f)) => f.to_string(),
            Some(grafeo::Value::Bool(b)) => b.to_string(),
            Some(grafeo::Value::List(list)) => {
                // Try to convert list of strings
                let strings: Vec<String> = list
                    .iter()
                    .map(|v| match v {
                        grafeo::Value::String(s) => s.to_string(),
                        _ => format!("{:?}", v),
                    })
                    .collect();
                serde_json::to_string(&strings).unwrap_or_default()
            }
            _ => String::new(),
        }
    }

    /// Convert Value to list of strings
    fn value_to_string_list(value: Option<&grafeo::Value>) -> Vec<String> {
        match value {
            Some(grafeo::Value::List(list)) => list
                .iter()
                .filter_map(|v| match v {
                    grafeo::Value::String(s) => Some(s.to_string()),
                    _ => None,
                })
                .collect(),
            Some(grafeo::Value::String(s)) => {
                // Try to parse as JSON array
                serde_json::from_str(s).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    #[allow(dead_code)]
    async fn load_full_propagation_graph(&self) -> Result<PropagationGraph> {
        let all_entities = self
            .entity_store
            .list_entities()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;
        let all_relationships = self
            .relationship_store
            .list_relationships()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut graph = PropagationGraph::default();

        for rel_header in all_relationships {
            let Some(rel_guid) = rel_header.guid else {
                continue;
            };
            let rel = match self.relationship_store.get_relationship(&rel_guid).await {
                Ok(rel) => rel,
                Err(_) => continue,
            };
            let (Some(end1), Some(end2)) = (&rel.end1, &rel.end2) else {
                continue;
            };
            let (Some(from_guid), Some(to_guid)) = (&end1.guid, &end2.guid) else {
                continue;
            };
            let tags = rel.propagate_tags.unwrap_or(PropagateTags::None);
            graph.merge_edge(from_guid.clone(), to_guid.clone(), tags);
        }

        for header in all_entities {
            let Some(source_guid) = header.guid else {
                continue;
            };
            let entity = match self.entity_store.get_entity(&source_guid).await {
                Ok(entity) => entity,
                Err(_) => continue,
            };
            let classes: Vec<String> = entity
                .classifications
                .iter()
                .map(|c| c.type_name.clone())
                .collect();
            graph.set_direct_classifications(source_guid, classes);
        }

        Ok(graph)
    }

    fn cached_propagation_graph(&self) -> PropagationGraph {
        self.propagation_graph
            .read()
            .expect("propagation graph lock poisoned")
            .clone()
    }

    fn replace_cached_propagation_graph(&self, graph: PropagationGraph) {
        *self
            .propagation_graph
            .write()
            .expect("propagation graph lock poisoned") = graph;
    }

    fn compute_propagated_classifications(
        &self,
        graph: &PropagationGraph,
        max_depth: usize,
    ) -> HashMap<String, HashSet<String>> {
        let mut propagated: HashMap<String, HashSet<String>> = HashMap::new();

        for (source_guid, classes) in &graph.direct_classifications {
            for class in classes {
                let mut queue: VecDeque<(String, usize)> = VecDeque::new();
                let mut seen: HashSet<String> = HashSet::new();
                queue.push_back((source_guid.clone(), 0));
                seen.insert(source_guid.clone());

                while let Some((current, depth)) = queue.pop_front() {
                    if depth >= max_depth {
                        continue;
                    }

                    if let Some(next_nodes) = graph.outgoing.get(&current) {
                        for (next_guid, tags) in next_nodes {
                            if matches!(tags, PropagateTags::OneToTwo | PropagateTags::Both)
                                && seen.insert(next_guid.clone())
                            {
                                propagated
                                    .entry(next_guid.clone())
                                    .or_default()
                                    .insert(class.clone());
                                queue.push_back((next_guid.clone(), depth + 1));
                            }
                        }
                    }

                    if let Some(prev_nodes) = graph.incoming.get(&current) {
                        for (prev_guid, tags) in prev_nodes {
                            if matches!(tags, PropagateTags::TwoToOne | PropagateTags::Both)
                                && seen.insert(prev_guid.clone())
                            {
                                propagated
                                    .entry(prev_guid.clone())
                                    .or_default()
                                    .insert(class.clone());
                                queue.push_back((prev_guid.clone(), depth + 1));
                            }
                        }
                    }
                }
            }
        }

        propagated
    }

    fn apply_propagated_classifications(
        &self,
        propagated: HashMap<String, HashSet<String>>,
    ) -> Result<()> {
        let clear_cypher = "MATCH (e:Entity) SET e.propagated_classifications = []";
        self.db
            .execute(clear_cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        for (guid, classes) in propagated {
            let mut class_list: Vec<String> = classes.into_iter().collect();
            class_list.sort();
            let classifications_json =
                serde_json::to_string(&class_list).unwrap_or_else(|_| "[]".to_string());
            let cypher = format!(
                "MATCH (e:Entity {{guid: '{}'}}) SET e.propagated_classifications = {}",
                guid, classifications_json
            );
            self.db
                .execute(&cypher)
                .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;
        }

        Ok(())
    }

    /// Propagate classifications through the graph using an application-level propagation graph
    async fn propagate_classifications(&self) -> Result<()> {
        let graph = self.cached_propagation_graph();
        let propagated = self.compute_propagated_classifications(&graph, 10);
        self.apply_propagated_classifications(propagated)?;

        Ok(())
    }
}

#[async_trait]
impl GraphStore for GrafeoGraphStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn rebuild_graph(&self) -> Result<()> {
        // Load all relationships from KV
        let relationships = self
            .relationship_store
            .list_relationships()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Load all entities
        let all_entities = self
            .entity_store
            .list_entities()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut propagation_graph = PropagationGraph::default();

        // Create all entity nodes first
        for header in all_entities {
            if let Some(ref guid) = header.guid {
                let entity = self.entity_store.get_entity(guid).await.ok();
                let (type_name, display_name, classifications) = match entity {
                    Some(e) => {
                        let display = e
                            .attributes
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                            .unwrap_or_default();

                        let classes: Vec<String> = e
                            .classifications
                            .iter()
                            .map(|c| c.type_name.clone())
                            .collect();

                        (e.type_name, display, classes)
                    }
                    None => ("Unknown".to_string(), String::new(), Vec::new()),
                };

                propagation_graph.set_direct_classifications(guid.clone(), classifications.clone());

                let classifications_json = serde_json::to_string(&classifications)
                    .unwrap_or_else(|_| "[]".to_string());

                let cypher = format!(
                    "MERGE (e:Entity {{guid: '{}'}}) \
                     SET e.type_name = '{}', \
                         e.display_name = '{}', \
                         e.classifications = {}",
                    guid,
                    type_name.replace('\'', "\\'"),
                    display_name.replace('\'', "\\'"),
                    classifications_json
                );

                if let Err(e) = self.db.execute(&cypher) {
                    tracing::warn!("Failed to insert node {}: {}", guid, e);
                }
            }
        }

        // Create relationship edges
        for rel_header in relationships {
            if let Some(ref guid) = rel_header.guid {
                if let Ok(rel) = self.relationship_store.get_relationship(guid).await {
                    if let (Some(end1), Some(end2)) = (&rel.end1, &rel.end2) {
                        if let (Some(from_guid), Some(to_guid)) =
                            (end1.guid.as_ref(), end2.guid.as_ref())
                        {
                            let tags = rel.propagate_tags.unwrap_or_default();
                            let propagate_str = Self::propagate_tags_to_string(tags);
                            propagation_graph
                                .merge_edge(from_guid.clone(), to_guid.clone(), tags);

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
                                from_guid,
                                to_guid,
                                rel.type_name,
                                guid,
                                propagate_str
                            );

                            if let Err(e) = self.db.execute(&cypher) {
                                tracing::warn!("Failed to create edge {}: {}", guid, e);
                            }
                        }
                    }
                }
            }
        }

        self.replace_cached_propagation_graph(propagation_graph);

        // Mark propagation as pending since we rebuilt the graph
        self.mark_propagation_pending();

        Ok(())
    }

    async fn add_entity_node(&self, entity_guid: &str, entity_type: &str) -> Result<()> {
        // Try to get entity details
        let entity = self.entity_store.get_entity(entity_guid).await.ok();

        let (type_name, display_name, classifications) = match entity {
            Some(e) => {
                let display = e
                    .attributes
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_default();

                let classes: Vec<String> = e
                    .classifications
                    .iter()
                    .map(|c| c.type_name.clone())
                    .collect();

                (e.type_name, display, classes)
            }
            None => (entity_type.to_string(), String::new(), Vec::new()),
        };

        let classifications_json = serde_json::to_string(&classifications)
            .unwrap_or_else(|_| "[]".to_string());

        let cypher = format!(
            "MERGE (e:Entity {{guid: '{}'}}) \
             SET e.type_name = '{}', \
                 e.display_name = '{}', \
                 e.classifications = {}",
            entity_guid,
            type_name.replace('\'', "\\'"),
            display_name.replace('\'', "\\'"),
            classifications_json
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        self.propagation_graph
            .write()
            .expect("propagation graph lock poisoned")
            .set_direct_classifications(entity_guid.to_string(), classifications);

        // Mark propagation as pending (lazy propagation)
        self.mark_propagation_pending();

        Ok(())
    }

    async fn remove_entity_node(&self, entity_guid: &str) -> Result<()> {
        let cypher = format!("MATCH (e:Entity {{guid: '{}'}}) DETACH DELETE e", entity_guid);

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        self.propagation_graph
            .write()
            .expect("propagation graph lock poisoned")
            .remove_entity(entity_guid);

        // Mark propagation as pending (lazy propagation)
        self.mark_propagation_pending();

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

        if updated {
            self.propagation_graph
                .write()
                .expect("propagation graph lock poisoned")
                .set_direct_classifications(entity_guid.to_string(), classifications);
            // Mark propagation as pending (lazy propagation)
            self.mark_propagation_pending();
        }

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
            from_guid,
            to_guid,
            relationship_type,
            relationship_guid,
            propagate_str
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        self.propagation_graph
            .write()
            .expect("propagation graph lock poisoned")
            .merge_edge(from_guid.to_string(), to_guid.to_string(), propagate_tags);

        // Mark propagation as pending (lazy propagation)
        self.mark_propagation_pending();

        Ok(())
    }

    async fn remove_relationship_edge(&self, relationship_guid: &str) -> Result<()> {
        let lookup_cypher = format!(
            "MATCH (a:Entity)-[r {{rel_guid: '{}'}}]->(b:Entity) \
             RETURN a.guid as from_guid, b.guid as to_guid",
            relationship_guid
        );
        let endpoints = self
            .db
            .execute(&lookup_cypher)
            .ok()
            .and_then(|result| {
                let from_guid = Self::value_to_string(Self::get_column_value(&result, 0, "from_guid"));
                let to_guid = Self::value_to_string(Self::get_column_value(&result, 0, "to_guid"));
                if from_guid.is_empty() || to_guid.is_empty() {
                    None
                } else {
                    Some((from_guid, to_guid))
                }
            });

        let cypher = format!(
            "MATCH ()-[r {{rel_guid: '{}'}}]-() DELETE r",
            relationship_guid
        );

        self.db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        if let Some(endpoints) = endpoints {
            self.propagation_graph
                .write()
                .expect("propagation graph lock poisoned")
                .remove_relationship_edge(&endpoints);
        }

        // Mark propagation as pending (lazy propagation)
        self.mark_propagation_pending();

        Ok(())
    }

    async fn get_lineage(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
        options: LineageQueryOptions,
    ) -> Result<LineageResult> {
        // Build relationship type filter if specified
        let type_filter = if options.relationship_types.as_ref().map(|v| v.is_empty()).unwrap_or(true) {
            String::new()
        } else {
            let types: Vec<String> = options
                .relationship_types
                .as_ref()
                .unwrap_or(&vec![])
                .iter()
                .map(|t| format!("'{}'", t))
                .collect();
            format!("WHERE type(r) IN [{}]", types.join(","))
        };

        // Use different query patterns based on direction
        // Optimized: Single query to get nodes and edges together
        let cypher = match direction {
            TraversalDirection::Input => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})<-[r*1..{}]-(related) \
                     {} \
                     RETURN DISTINCT \
                         related.guid as node_guid, \
                         related.type_name as node_type, \
                         related.display_name as node_display, \
                         related.classifications as node_classifications, \
                         type(r) as rel_type, \
                         r.rel_guid as rel_guid",
                    entity_guid,
                    options.depth,
                    type_filter
                )
            }
            TraversalDirection::Output => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})-[r*1..{}]->(related) \
                     {} \
                     RETURN DISTINCT \
                         related.guid as node_guid, \
                         related.type_name as node_type, \
                         related.display_name as node_display, \
                         related.classifications as node_classifications, \
                         type(r) as rel_type, \
                         r.rel_guid as rel_guid",
                    entity_guid,
                    options.depth,
                    type_filter
                )
            }
            TraversalDirection::Both => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})-[r*1..{}]-(related) \
                     {} \
                     RETURN DISTINCT \
                         related.guid as node_guid, \
                         related.type_name as node_type, \
                         related.display_name as node_display, \
                         related.classifications as node_classifications, \
                         type(r) as rel_type, \
                         r.rel_guid as rel_guid",
                    entity_guid,
                    options.depth,
                    type_filter
                )
            }
        };

        let result = self
            .db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        let mut nodes_map: HashMap<String, LineageNode> = HashMap::new();
        let mut edges: Vec<LineageEdgeInfo> = Vec::new();
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        // Add root node
        if let Ok(root_entity) = self.entity_store.get_entity(entity_guid).await {
            let root_node = LineageNode::new(entity_guid, &root_entity.type_name)
                .with_display_name(
                    root_entity
                        .attributes
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                )
                .with_classifications(
                    root_entity
                        .classifications
                        .iter()
                        .map(|c| c.type_name.clone())
                        .collect(),
                );
            nodes_map.insert(entity_guid.to_string(), root_node);
        }

        // Collect relationship GUIDs for batch endpoint lookup
        let mut rel_guids: Vec<String> = Vec::new();
        for (row_idx, _row) in result.rows.iter().enumerate() {
            let rel_guid = Self::value_to_string(Self::get_column_value(&result, row_idx, "rel_guid"));
            if !rel_guid.is_empty() {
                rel_guids.push(rel_guid);
            }
        }

        // Batch fetch relationship endpoints from KV store
        let mut rel_endpoints: HashMap<String, (String, String)> = HashMap::new();
        for rel_guid in &rel_guids {
            if let Ok(rel) = self.relationship_store.get_relationship(rel_guid).await {
                if let (Some(end1), Some(end2)) = (&rel.end1, &rel.end2) {
                    if let (Some(from), Some(to)) = (&end1.guid, &end2.guid) {
                        rel_endpoints.insert(rel_guid.clone(), (from.clone(), to.clone()));
                    }
                }
            }
        }

        // Process results
        for (row_idx, _row) in result.rows.iter().enumerate() {
            let node_guid = Self::value_to_string(Self::get_column_value(&result, row_idx, "node_guid"));
            let node_type = Self::value_to_string(Self::get_column_value(&result, row_idx, "node_type"));
            let node_display = Self::value_to_string(Self::get_column_value(&result, row_idx, "node_display"));
            let node_classifications = Self::value_to_string_list(Self::get_column_value(&result, row_idx, "node_classifications"));

            let rel_guid = Self::value_to_string(Self::get_column_value(&result, row_idx, "rel_guid"));
            let rel_type = Self::value_to_string(Self::get_column_value(&result, row_idx, "rel_type"));

            // Build node
            let node = LineageNode::new(&node_guid, &node_type)
                .with_display_name(node_display)
                .with_classifications(node_classifications);

            nodes_map.entry(node_guid.clone()).or_insert(node);

            // Build edge info from cached endpoints
            if let Some((from_guid, to_guid)) = rel_endpoints.get(&rel_guid) {
                edges.push(
                    LineageEdgeInfo::new(from_guid, to_guid, &rel_guid, &rel_type)
                        .with_label(rel_type.clone()),
                );

                // Build adjacency
                adjacency
                    .entry(from_guid.clone())
                    .or_default()
                    .push(to_guid.clone());
            }
        }

        // Apply entity type filter if specified
        let empty_vec: Vec<String> = Vec::new();
        let nodes: Vec<LineageNode> = if options.entity_types.as_ref().map(|v| v.is_empty()).unwrap_or(true) {
            nodes_map.into_values().collect()
        } else {
            let entity_types = options.entity_types.as_ref().unwrap_or(&empty_vec);
            nodes_map
                .into_values()
                .filter(|n| entity_types.contains(&n.entity_type))
                .collect()
        };

        Ok(LineageResult {
            root_guid: entity_guid.to_string(),
            direction,
            depth: options.depth,
            nodes,
            edges,
            adjacency,
        })
    }

    async fn get_all_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        // Auto-flush pending propagation before querying
        self.flush_propagation().await?;

        // Get direct classifications from entity store
        let entity = self
            .entity_store
            .get_entity(entity_guid)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut all_classifications: Vec<Classification> = entity.classifications.clone();

        // Get propagated classifications from Grafeo
        let cypher = format!(
            "MATCH (e:Entity {{guid: '{}'}}) \
             RETURN e.propagated_classifications as propagated",
            entity_guid
        );

        if let Ok(result) = self.db.execute(&cypher) {
            if let Some(value) = Self::get_column_value(&result, 0, "propagated") {
                let propagated_list = Self::value_to_string_list(Some(value));
                for class_name in propagated_list {
                    if !all_classifications.iter().any(|c| c.type_name == class_name) {
                        all_classifications.push(Classification::new(class_name));
                    }
                }
            }
        }

        Ok(all_classifications)
    }

    async fn get_neighbors(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
    ) -> Result<Vec<LineageNode>> {
        let cypher = match direction {
            TraversalDirection::Input => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})<-[r]-(n) \
                     RETURN n.guid as guid, n.type_name as type_name, n.display_name as display_name, n.classifications as classifications",
                    entity_guid
                )
            }
            TraversalDirection::Output => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})-[r]->(n) \
                     RETURN n.guid as guid, n.type_name as type_name, n.display_name as display_name, n.classifications as classifications",
                    entity_guid
                )
            }
            TraversalDirection::Both => {
                format!(
                    "MATCH (e:Entity {{guid: '{}'}})-[r]-(n) \
                     RETURN n.guid as guid, n.type_name as type_name, n.display_name as display_name, n.classifications as classifications",
                    entity_guid
                )
            }
        };

        let result = self
            .db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        let mut neighbors = Vec::new();
        for (row_idx, _row) in result.rows.iter().enumerate() {
            let guid = Self::value_to_string(Self::get_column_value(&result, row_idx, "guid"));
            let node_type = Self::value_to_string(Self::get_column_value(&result, row_idx, "type_name"));
            let display_name = Self::value_to_string(Self::get_column_value(&result, row_idx, "display_name"));
            let classifications = Self::value_to_string_list(Self::get_column_value(&result, row_idx, "classifications"));

            let node = LineageNode::new(&guid, &node_type)
                .with_display_name(display_name)
                .with_classifications(classifications);

            neighbors.push(node);
        }

        Ok(neighbors)
    }

    async fn path_exists(&self, from_guid: &str, to_guid: &str, max_depth: usize) -> Result<bool> {
        let cypher = format!(
            "MATCH path = (a:Entity {{guid: '{}'}})-[*1..{}]->(b:Entity {{guid: '{}'}}) \
             RETURN count(path) as path_count",
            from_guid, max_depth, to_guid
        );

        let result = self
            .db
            .execute(&cypher)
            .map_err(|e| CoreError::Storage(format!("Grafeo error: {}", e)))?;

        let exists = Self::get_column_value(&result, 0, "path_count")
            .and_then(|v| match v {
                grafeo::Value::Int64(i) => Some(*i > 0),
                _ => None,
            })
            .unwrap_or(false);

        Ok(exists)
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
}

// Tests are in metavisor_store.rs and other integration test files
