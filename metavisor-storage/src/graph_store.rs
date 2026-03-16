//! InMemoryGraphStore implementation using petgraph
//!
//! Provides graph-based operations for lineage tracking and classification propagation

use async_trait::async_trait;
use petgraph::stable_graph::{EdgeIndex, NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use petgraph::Direction as PetDirection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use metavisor_core::{
    Classification, CoreError, EntityStore, GraphStore, LineageEdge, LineageEdgeInfo, LineageNode,
    LineageQueryOptions, LineageResult, PropagateTags, RelationshipStore, Result,
    TraversalDirection,
};

/// Mutable state for BFS traversal
struct BfsState<'a> {
    visited_nodes: &'a mut HashMap<NodeIndex, LineageNode>,
    visited_edges: &'a mut Vec<LineageEdgeInfo>,
    adjacency: &'a mut HashMap<String, Vec<String>>,
}

/// In-memory graph store using petgraph
///
/// This implementation maintains an in-memory directed graph for efficient
/// lineage traversal and classification propagation.
pub struct InMemoryGraphStore {
    /// The petgraph DiGraph
    graph: RwLock<StableDiGraph<LineageNode, LineageEdge>>,
    /// Map entity GUID to NodeIndex for quick lookup
    node_index: RwLock<HashMap<String, NodeIndex>>,
    /// Map relationship GUID to EdgeIndex for quick lookup
    edge_index: RwLock<HashMap<String, EdgeIndex>>,
    /// Entity store for fetching entity details
    entity_store: Arc<dyn EntityStore>,
    /// Relationship store for rebuilding graph
    relationship_store: Arc<dyn RelationshipStore>,
}

impl InMemoryGraphStore {
    /// Create a new InMemoryGraphStore
    pub fn new(
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
    ) -> Self {
        Self {
            // StableDiGraph keeps NodeIndex/EdgeIndex stable across removals.
            graph: RwLock::new(StableDiGraph::new()),
            node_index: RwLock::new(HashMap::new()),
            edge_index: RwLock::new(HashMap::new()),
            entity_store,
            relationship_store,
        }
    }

    /// Get node index by GUID
    fn get_node_index(&self, guid: &str) -> Option<NodeIndex> {
        self.node_index.read().unwrap().get(guid).copied()
    }

    /// Check if a node exists
    #[allow(dead_code)]
    fn node_exists(&self, guid: &str) -> bool {
        self.node_index.read().unwrap().contains_key(guid)
    }

    /// BFS traversal helper
    fn bfs_traverse(
        graph: &StableDiGraph<LineageNode, LineageEdge>,
        start: NodeIndex,
        pet_direction: PetDirection,
        max_depth: usize,
        state: &mut BfsState<'_>,
        options: &LineageQueryOptions,
    ) {
        let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::new();
        queue.push_back((start, 0));
        let mut seen: HashSet<NodeIndex> = HashSet::new();
        seen.insert(start);

        while let Some((current_idx, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            // Get edges in the specified direction
            for edge_ref in graph.edges_directed(current_idx, pet_direction) {
                let edge_weight = edge_ref.weight();

                // Filter by relationship type if specified
                if !options.matches_relationship_type(&edge_weight.relationship_type) {
                    continue;
                }

                // For outgoing edges, target() is the neighbor
                // For incoming edges, source() is the neighbor
                let (neighbor_idx, source_idx) = match pet_direction {
                    PetDirection::Outgoing => (edge_ref.target(), edge_ref.source()),
                    PetDirection::Incoming => (edge_ref.source(), edge_ref.target()),
                };

                // Determine from/to based on direction (for edge info)
                let (from_idx, to_idx) = match pet_direction {
                    PetDirection::Outgoing => (source_idx, neighbor_idx),
                    PetDirection::Incoming => (neighbor_idx, source_idx),
                };

                // Apply entity-type filter before recording edges/adjacency so results stay
                // internally consistent (no dangling edge endpoints).
                if !seen.contains(&neighbor_idx) {
                    if let Some(node) = graph.node_weight(neighbor_idx) {
                        if !options.matches_entity_type(&node.entity_type) {
                            continue;
                        }
                    }
                }

                let from_guid = match graph.node_weight(from_idx) {
                    Some(n) => n.id.0.clone(),
                    None => continue,
                };
                let to_guid = match graph.node_weight(to_idx) {
                    Some(n) => n.id.0.clone(),
                    None => continue,
                };

                state.visited_edges.push(
                    LineageEdgeInfo::new(
                        &from_guid,
                        &to_guid,
                        &edge_weight.id,
                        &edge_weight.relationship_type,
                    )
                    .with_label(edge_weight.label.clone().unwrap_or_default()),
                );

                state
                    .adjacency
                    .entry(from_guid.clone())
                    .or_default()
                    .push(to_guid.clone());

                // Add neighbor node if not seen
                if !seen.contains(&neighbor_idx) {
                    seen.insert(neighbor_idx);

                    if let Some(node) = graph.node_weight(neighbor_idx).cloned() {
                        state.visited_nodes.insert(neighbor_idx, node);
                    }
                    queue.push_back((neighbor_idx, depth + 1));
                }
            }
        }
    }

    /// Propagate classifications through the graph
    fn propagate_classifications(graph: &mut StableDiGraph<LineageNode, LineageEdge>) {
        // Reset previous propagation results; otherwise incremental mutations can leave stale
        // propagated_classifications hanging around.
        let node_indices: Vec<NodeIndex> = graph.node_indices().collect();
        for node_idx in &node_indices {
            if let Some(node) = graph.node_weight_mut(*node_idx) {
                node.propagated_classifications.clear();
            }
        }

        // Collect all classification sources and propagation rules
        let mut propagation_queue: VecDeque<(NodeIndex, String)> = VecDeque::new();

        // Find all nodes with classifications
        for node_idx in node_indices {
            if let Some(node) = graph.node_weight(node_idx) {
                for classification in &node.classifications {
                    propagation_queue.push_back((node_idx, classification.clone()));
                }
            }
        }

        // Build adjacency information first (to avoid borrow issues)
        let forward_edges: HashMap<NodeIndex, Vec<(NodeIndex, bool)>> = {
            let mut edges: HashMap<NodeIndex, Vec<(NodeIndex, bool)>> = HashMap::new();
            for node_idx in graph.node_indices() {
                for edge_ref in graph.edges_directed(node_idx, PetDirection::Outgoing) {
                    let target = edge_ref.target();
                    let propagates = edge_ref.weight().propagates_forward();
                    edges
                        .entry(node_idx)
                        .or_default()
                        .push((target, propagates));
                }
            }
            edges
        };

        let backward_edges: HashMap<NodeIndex, Vec<(NodeIndex, bool)>> = {
            let mut edges: HashMap<NodeIndex, Vec<(NodeIndex, bool)>> = HashMap::new();
            for node_idx in graph.node_indices() {
                for edge_ref in graph.edges_directed(node_idx, PetDirection::Incoming) {
                    let source = edge_ref.source();
                    let propagates = edge_ref.weight().propagates_backward();
                    edges
                        .entry(node_idx)
                        .or_default()
                        .push((source, propagates));
                }
            }
            edges
        };

        // BFS propagation
        let mut propagated: HashSet<(NodeIndex, String)> = HashSet::new();
        while let Some((source_idx, classification)) = propagation_queue.pop_front() {
            // Propagate forward (outgoing edges)
            if let Some(neighbors) = forward_edges.get(&source_idx) {
                for &(target_idx, should_propagate) in neighbors {
                    if should_propagate && propagated.insert((target_idx, classification.clone())) {
                        if let Some(target_node) = graph.node_weight_mut(target_idx) {
                            target_node.add_propagated_classification(&classification);
                        }
                        propagation_queue.push_back((target_idx, classification.clone()));
                    }
                }
            }

            // Propagate backward (incoming edges)
            if let Some(neighbors) = backward_edges.get(&source_idx) {
                for &(neighbor_idx, should_propagate) in neighbors {
                    if should_propagate && propagated.insert((neighbor_idx, classification.clone()))
                    {
                        if let Some(neighbor_node) = graph.node_weight_mut(neighbor_idx) {
                            neighbor_node.add_propagated_classification(&classification);
                        }
                        propagation_queue.push_back((neighbor_idx, classification.clone()));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl GraphStore for InMemoryGraphStore {
    async fn rebuild_graph(&self) -> Result<()> {
        // Load all relationships first (outside of lock)
        let relationships = self
            .relationship_store
            .list_relationships()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Load all entities (including those without relationships)
        let all_entities = self
            .entity_store
            .list_entities()
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Build entity map from headers
        let mut entity_guids: HashSet<String> = HashSet::new();
        for header in &all_entities {
            if let Some(ref guid) = header.guid {
                entity_guids.insert(guid.clone());
            }
        }

        // Also add entity GUIDs from relationships (in case they were deleted)
        for rel in &relationships {
            if let Some(ref end1) = rel.end1 {
                if let Some(ref guid) = end1.guid {
                    entity_guids.insert(guid.clone());
                }
            }
            if let Some(ref end2) = rel.end2 {
                if let Some(ref guid) = end2.guid {
                    entity_guids.insert(guid.clone());
                }
            }
        }

        // Fetch all entities (outside of lock)
        let mut entity_map: HashMap<String, metavisor_core::Entity> = HashMap::new();
        for guid in &entity_guids {
            if let Ok(entity) = self.entity_store.get_entity(guid).await {
                entity_map.insert(guid.clone(), entity);
            }
        }

        // Now acquire locks and build the graph
        let mut graph = self.graph.write().unwrap();
        let mut node_index = self.node_index.write().unwrap();
        let mut edge_index = self.edge_index.write().unwrap();

        // Clear existing graph
        *graph = StableDiGraph::new();
        node_index.clear();
        edge_index.clear();

        // Create nodes for all entities
        for guid in entity_guids {
            let node = match entity_map.get(&guid) {
                Some(entity) => {
                    let display_name = entity
                        .attributes
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(String::from);

                    let classifications = entity
                        .classifications
                        .iter()
                        .map(|c| c.type_name.clone())
                        .collect();

                    LineageNode::new(&guid, &entity.type_name)
                        .with_display_name(display_name.unwrap_or_default())
                        .with_classifications(classifications)
                }
                None => LineageNode::new(&guid, "Unknown"),
            };

            let idx = graph.add_node(node);
            node_index.insert(guid, idx);
        }

        // Create edges
        for rel in relationships {
            let from_guid = rel.end1.as_ref().and_then(|e| e.guid.as_ref());
            let to_guid = rel.end2.as_ref().and_then(|e| e.guid.as_ref());
            let rel_guid = rel.guid.as_ref();

            if let (Some(from), Some(to), Some(guid)) = (from_guid, to_guid, rel_guid) {
                if let (Some(&from_idx), Some(&to_idx)) = (node_index.get(from), node_index.get(to))
                {
                    let edge = LineageEdge::new(
                        guid,
                        &rel.type_name,
                        rel.propagate_tags.unwrap_or(PropagateTags::None),
                    )
                    .with_label(rel.label.clone().unwrap_or_default());

                    let idx = graph.add_edge(from_idx, to_idx, edge);
                    edge_index.insert(guid.clone(), idx);
                }
            }
        }

        // Propagate classifications
        Self::propagate_classifications(&mut graph);

        Ok(())
    }

    async fn add_entity_node(&self, entity_guid: &str, entity_type: &str) -> Result<()> {
        // Check if node already exists (quick check with read lock)
        {
            let node_index = self.node_index.read().unwrap();
            if node_index.contains_key(entity_guid) {
                return Ok(());
            }
        }

        // Try to get entity details (outside of lock)
        let entity_result = self.entity_store.get_entity(entity_guid).await;

        // Now acquire write locks
        let mut graph = self.graph.write().unwrap();
        let mut node_index = self.node_index.write().unwrap();

        // Double-check after acquiring write lock
        if node_index.contains_key(entity_guid) {
            return Ok(());
        }

        let node = match entity_result {
            Ok(entity) => {
                let display_name = entity
                    .attributes
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let classifications = entity
                    .classifications
                    .iter()
                    .map(|c| c.type_name.clone())
                    .collect();

                LineageNode::new(entity_guid, &entity.type_name)
                    .with_display_name(display_name.unwrap_or_default())
                    .with_classifications(classifications)
            }
            Err(_) => LineageNode::new(entity_guid, entity_type),
        };

        let idx = graph.add_node(node);
        node_index.insert(entity_guid.to_string(), idx);

        // Keep propagated classifications fresh after incremental mutations.
        Self::propagate_classifications(&mut graph);

        Ok(())
    }

    async fn remove_entity_node(&self, entity_guid: &str) -> Result<()> {
        let mut graph = self.graph.write().unwrap();
        let mut node_index = self.node_index.write().unwrap();
        let mut edge_index = self.edge_index.write().unwrap();

        if let Some(node_idx) = node_index.remove(entity_guid) {
            // Remove all edges connected to this node
            let edges_to_remove: Vec<EdgeIndex> = graph.edges(node_idx).map(|e| e.id()).collect();

            for edge_idx in edges_to_remove {
                // Find and remove from edge_index
                edge_index.retain(|_, &mut idx| idx != edge_idx);
                graph.remove_edge(edge_idx);
            }

            // Remove the node
            graph.remove_node(node_idx);
        }

        // Keep propagated classifications fresh after incremental mutations.
        Self::propagate_classifications(&mut graph);

        Ok(())
    }

    async fn update_entity_node(
        &self,
        entity_guid: &str,
        entity_type: &str,
        display_name: Option<&str>,
        classifications: Vec<String>,
    ) -> Result<bool> {
        let mut graph = self.graph.write().unwrap();

        if let Some(node_idx) = self.get_node_index(entity_guid) {
            if let Some(node) = graph.node_weight_mut(node_idx) {
                // Update the node's properties
                node.entity_type = entity_type.to_string();
                node.display_name = display_name.map(String::from);
                node.classifications = classifications;

                // Re-propagate classifications since direct classifications may have changed
                Self::propagate_classifications(&mut graph);

                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn add_relationship_edge(
        &self,
        relationship_guid: &str,
        relationship_type: &str,
        from_guid: &str,
        to_guid: &str,
        propagate_tags: PropagateTags,
    ) -> Result<()> {
        let mut graph = self.graph.write().unwrap();
        let mut node_index = self.node_index.write().unwrap();
        let mut edge_index = self.edge_index.write().unwrap();

        // Ensure both nodes exist
        let from_idx = match node_index.get(from_guid) {
            Some(&idx) => idx,
            None => {
                let node = LineageNode::new(from_guid, "Unknown");
                let idx = graph.add_node(node);
                node_index.insert(from_guid.to_string(), idx);
                idx
            }
        };

        let to_idx = match node_index.get(to_guid) {
            Some(&idx) => idx,
            None => {
                let node = LineageNode::new(to_guid, "Unknown");
                let idx = graph.add_node(node);
                node_index.insert(to_guid.to_string(), idx);
                idx
            }
        };

        // Create the edge
        let edge = LineageEdge::new(relationship_guid, relationship_type, propagate_tags);
        let idx = graph.add_edge(from_idx, to_idx, edge);
        edge_index.insert(relationship_guid.to_string(), idx);

        // Keep propagated classifications fresh after incremental mutations.
        Self::propagate_classifications(&mut graph);

        Ok(())
    }

    async fn remove_relationship_edge(&self, relationship_guid: &str) -> Result<()> {
        let mut graph = self.graph.write().unwrap();
        let mut edge_index = self.edge_index.write().unwrap();

        if let Some(edge_idx) = edge_index.remove(relationship_guid) {
            graph.remove_edge(edge_idx);
        }

        // Keep propagated classifications fresh after incremental mutations.
        Self::propagate_classifications(&mut graph);

        Ok(())
    }

    async fn get_lineage(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
        options: LineageQueryOptions,
    ) -> Result<LineageResult> {
        let graph = self.graph.read().unwrap();
        let start_idx = self
            .get_node_index(entity_guid)
            .ok_or_else(|| CoreError::EntityNotFound(entity_guid.to_string()))?;

        let mut visited_nodes: HashMap<NodeIndex, LineageNode> = HashMap::new();
        let mut visited_edges: Vec<LineageEdgeInfo> = Vec::new();
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        // Add root node
        if let Some(root_node) = graph.node_weight(start_idx).cloned() {
            visited_nodes.insert(start_idx, root_node);
        }

        let max_depth = options.depth;

        let mut state = BfsState {
            visited_nodes: &mut visited_nodes,
            visited_edges: &mut visited_edges,
            adjacency: &mut adjacency,
        };

        match direction {
            TraversalDirection::Input => {
                // Follow edges in reverse (to -> from)
                Self::bfs_traverse(
                    &graph,
                    start_idx,
                    PetDirection::Incoming,
                    max_depth,
                    &mut state,
                    &options,
                );
            }
            TraversalDirection::Output => {
                // Follow edges forward (from -> to)
                Self::bfs_traverse(
                    &graph,
                    start_idx,
                    PetDirection::Outgoing,
                    max_depth,
                    &mut state,
                    &options,
                );
            }
            TraversalDirection::Both => {
                // Both directions
                Self::bfs_traverse(
                    &graph,
                    start_idx,
                    PetDirection::Incoming,
                    max_depth,
                    &mut state,
                    &options,
                );
                Self::bfs_traverse(
                    &graph,
                    start_idx,
                    PetDirection::Outgoing,
                    max_depth,
                    &mut state,
                    &options,
                );
            }
        }

        // Filter propagated classifications if not requested
        let nodes: Vec<LineageNode> = visited_nodes
            .into_values()
            .map(|mut node| {
                if !options.include_propagated_classifications {
                    node.propagated_classifications.clear();
                }
                node
            })
            .collect();

        Ok(LineageResult {
            root_guid: entity_guid.to_string(),
            direction,
            depth: max_depth,
            nodes,
            edges: visited_edges,
            adjacency,
        })
    }

    async fn get_all_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        // First, try to get direct classifications from entity store
        let entity = self
            .entity_store
            .get_entity(entity_guid)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let direct_classifications = entity.classifications.clone();

        // Then, get propagated classifications from graph
        let graph = self.graph.read().unwrap();
        if let Some(node_idx) = self.get_node_index(entity_guid) {
            if let Some(node) = graph.node_weight(node_idx) {
                // Merge direct and propagated classifications
                let mut all_classifications = direct_classifications;

                // For propagated classifications, we only have type names
                // Create simple Classification objects for them
                for prop_class_name in &node.propagated_classifications {
                    // Check if this classification is already in direct list
                    if !all_classifications
                        .iter()
                        .any(|c| &c.type_name == prop_class_name)
                    {
                        all_classifications.push(Classification::new(prop_class_name.clone()));
                    }
                }

                return Ok(all_classifications);
            }
        }

        Ok(direct_classifications)
    }

    async fn get_neighbors(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
    ) -> Result<Vec<LineageNode>> {
        let graph = self.graph.read().unwrap();
        let start_idx = self
            .get_node_index(entity_guid)
            .ok_or_else(|| CoreError::EntityNotFound(entity_guid.to_string()))?;

        let mut neighbors: Vec<LineageNode> = Vec::new();
        let mut seen: HashSet<NodeIndex> = HashSet::new();

        let directions: Vec<PetDirection> = match direction {
            TraversalDirection::Input => vec![PetDirection::Incoming],
            TraversalDirection::Output => vec![PetDirection::Outgoing],
            TraversalDirection::Both => vec![PetDirection::Incoming, PetDirection::Outgoing],
        };

        for pet_dir in directions {
            for edge_ref in graph.edges_directed(start_idx, pet_dir) {
                // For outgoing edges, target() is the neighbor
                // For incoming edges, source() is the neighbor
                let neighbor_idx = if pet_dir == PetDirection::Outgoing {
                    edge_ref.target()
                } else {
                    edge_ref.source()
                };
                if seen.insert(neighbor_idx) {
                    if let Some(node) = graph.node_weight(neighbor_idx).cloned() {
                        neighbors.push(node);
                    }
                }
            }
        }

        Ok(neighbors)
    }

    async fn path_exists(&self, from_guid: &str, to_guid: &str, max_depth: usize) -> Result<bool> {
        let graph = self.graph.read().unwrap();

        let start_idx = self
            .get_node_index(from_guid)
            .ok_or_else(|| CoreError::EntityNotFound(from_guid.to_string()))?;

        let target_idx = match self.get_node_index(to_guid) {
            Some(idx) => idx,
            None => return Ok(false),
        };

        // BFS to find path
        let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::new();
        queue.push_back((start_idx, 0));
        let mut visited: HashSet<NodeIndex> = HashSet::new();
        visited.insert(start_idx);

        while let Some((current_idx, depth)) = queue.pop_front() {
            if current_idx == target_idx {
                return Ok(true);
            }

            if depth >= max_depth {
                continue;
            }

            for neighbor_idx in graph.neighbors(current_idx) {
                if visited.insert(neighbor_idx) {
                    queue.push_back((neighbor_idx, depth + 1));
                }
            }
        }

        Ok(false)
    }

    fn node_count(&self) -> usize {
        self.graph.read().unwrap().node_count()
    }

    fn edge_count(&self) -> usize {
        self.graph.read().unwrap().edge_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{Entity, EntityHeader, Relationship, RelationshipHeader};

    // Mock EntityStore for testing
    struct MockEntityStore {
        entities: RwLock<HashMap<String, Entity>>,
    }

    impl MockEntityStore {
        fn new() -> Self {
            Self {
                entities: RwLock::new(HashMap::new()),
            }
        }

        fn add_entity(&self, entity: Entity) {
            if let Some(ref guid) = entity.guid {
                self.entities.write().unwrap().insert(guid.clone(), entity);
            }
        }
    }

    #[async_trait]
    impl EntityStore for MockEntityStore {
        async fn create_entity(&self, entity: &Entity) -> Result<String> {
            let guid = entity.guid.clone().unwrap_or_default();
            self.add_entity(entity.clone());
            Ok(guid)
        }

        async fn get_entity(&self, guid: &str) -> Result<Entity> {
            self.entities
                .read()
                .unwrap()
                .get(guid)
                .cloned()
                .ok_or_else(|| CoreError::EntityNotFound(guid.to_string()))
        }

        async fn get_entity_by_unique_attrs(
            &self,
            _type_name: &str,
            _unique_attrs: &HashMap<String, serde_json::Value>,
        ) -> Result<Entity> {
            Err(CoreError::EntityNotFound("not found".to_string()))
        }

        async fn update_entity(&self, _entity: &Entity) -> Result<()> {
            Ok(())
        }

        async fn delete_entity(&self, guid: &str) -> Result<()> {
            self.entities.write().unwrap().remove(guid);
            Ok(())
        }

        async fn entity_exists(&self, guid: &str) -> Result<bool> {
            Ok(self.entities.read().unwrap().contains_key(guid))
        }

        async fn list_entities_by_type(&self, _type_name: &str) -> Result<Vec<EntityHeader>> {
            Ok(Vec::new())
        }

        async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
            Ok(Vec::new())
        }
    }

    // Mock RelationshipStore for testing
    struct MockRelationshipStore {
        relationships: RwLock<HashMap<String, Relationship>>,
    }

    impl MockRelationshipStore {
        fn new() -> Self {
            Self {
                relationships: RwLock::new(HashMap::new()),
            }
        }

        fn add_relationship(&self, rel: Relationship) {
            if let Some(ref guid) = rel.guid {
                self.relationships
                    .write()
                    .unwrap()
                    .insert(guid.clone(), rel);
            }
        }
    }

    #[async_trait]
    impl RelationshipStore for MockRelationshipStore {
        async fn create_relationship(&self, relationship: &Relationship) -> Result<String> {
            let guid = relationship.guid.clone().unwrap_or_default();
            self.add_relationship(relationship.clone());
            Ok(guid)
        }

        async fn get_relationship(&self, guid: &str) -> Result<Relationship> {
            self.relationships
                .read()
                .unwrap()
                .get(guid)
                .cloned()
                .ok_or_else(|| CoreError::RelationshipNotFound(guid.to_string()))
        }

        async fn update_relationship(&self, _relationship: &Relationship) -> Result<()> {
            Ok(())
        }

        async fn delete_relationship(&self, guid: &str) -> Result<()> {
            self.relationships.write().unwrap().remove(guid);
            Ok(())
        }

        async fn relationship_exists(&self, guid: &str) -> Result<bool> {
            Ok(self.relationships.read().unwrap().contains_key(guid))
        }

        async fn list_relationships_by_entity(
            &self,
            _entity_guid: &str,
        ) -> Result<Vec<RelationshipHeader>> {
            Ok(Vec::new())
        }

        async fn list_relationships_by_type(
            &self,
            _type_name: &str,
        ) -> Result<Vec<RelationshipHeader>> {
            Ok(Vec::new())
        }

        async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>> {
            Ok(self
                .relationships
                .read()
                .unwrap()
                .values()
                .map(|r| r.to_header())
                .collect())
        }
    }

    fn create_test_stores() -> (Arc<MockEntityStore>, Arc<MockRelationshipStore>) {
        (
            Arc::new(MockEntityStore::new()),
            Arc::new(MockRelationshipStore::new()),
        )
    }

    #[tokio::test]
    async fn test_empty_graph() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        assert_eq!(graph_store.node_count(), 0);
        assert_eq!(graph_store.edge_count(), 0);
        assert!(graph_store.is_empty());
    }

    #[tokio::test]
    async fn test_add_entity_node() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        graph_store
            .add_entity_node("guid-1", "Table")
            .await
            .unwrap();

        assert_eq!(graph_store.node_count(), 1);

        // Adding same node again should not duplicate
        graph_store
            .add_entity_node("guid-1", "Table")
            .await
            .unwrap();

        assert_eq!(graph_store.node_count(), 1);
    }

    #[tokio::test]
    async fn test_add_relationship_edge() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        graph_store
            .add_relationship_edge(
                "rel-1",
                "process_inputs",
                "source",
                "target",
                PropagateTags::OneToTwo,
            )
            .await
            .unwrap();

        assert_eq!(graph_store.node_count(), 2); // source and target nodes
        assert_eq!(graph_store.edge_count(), 1);
    }

    #[tokio::test]
    async fn test_remove_entity_node() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        graph_store
            .add_entity_node("guid-1", "Table")
            .await
            .unwrap();

        assert_eq!(graph_store.node_count(), 1);

        graph_store.remove_entity_node("guid-1").await.unwrap();

        assert_eq!(graph_store.node_count(), 0);
    }

    #[tokio::test]
    async fn test_get_lineage_not_found() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        let result = graph_store
            .get_lineage(
                "nonexistent",
                TraversalDirection::Input,
                LineageQueryOptions::new(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::EntityNotFound(_)));
    }

    #[tokio::test]
    async fn test_get_neighbors() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create a simple graph: A -> B -> C
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::OneToTwo)
            .await
            .unwrap();
        graph_store
            .add_relationship_edge("rel-2", "flow", "B", "C", PropagateTags::OneToTwo)
            .await
            .unwrap();

        // Get output neighbors of B
        let neighbors = graph_store
            .get_neighbors("B", TraversalDirection::Output)
            .await
            .unwrap();

        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "C");

        // Get input neighbors of B
        let neighbors = graph_store
            .get_neighbors("B", TraversalDirection::Input)
            .await
            .unwrap();

        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "A");
    }

    #[tokio::test]
    async fn test_path_exists() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create a simple graph: A -> B -> C
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::None)
            .await
            .unwrap();
        graph_store
            .add_relationship_edge("rel-2", "flow", "B", "C", PropagateTags::None)
            .await
            .unwrap();

        // Path exists from A to C
        assert!(graph_store.path_exists("A", "C", 10).await.unwrap());

        // Path does not exist from C to A (directed graph)
        assert!(!graph_store.path_exists("C", "A", 10).await.unwrap());

        // Path with depth limit
        assert!(graph_store.path_exists("A", "C", 2).await.unwrap());
        assert!(!graph_store.path_exists("A", "C", 1).await.unwrap());
    }

    #[tokio::test]
    async fn test_classification_propagation_one_to_two() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create a chain: A -(ONE_TO_TWO)-> B -(ONE_TO_TWO)-> C
        graph_store.add_entity_node("A", "Table").await.unwrap();
        graph_store.add_entity_node("B", "Table").await.unwrap();
        graph_store.add_entity_node("C", "Table").await.unwrap();

        // Add edges with ONE_TO_TWO propagation
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::OneToTwo)
            .await
            .unwrap();
        graph_store
            .add_relationship_edge("rel-2", "flow", "B", "C", PropagateTags::OneToTwo)
            .await
            .unwrap();

        // Verify graph structure
        assert_eq!(graph_store.node_count(), 3);
        assert_eq!(graph_store.edge_count(), 2);

        // Verify neighbors in forward direction
        let neighbors = graph_store
            .get_neighbors("A", TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "B");

        let neighbors = graph_store
            .get_neighbors("B", TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "C");
    }

    #[tokio::test]
    async fn test_classification_propagation_two_to_one() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create a chain: A <-(TWO_TO_ONE)- B
        graph_store.add_entity_node("A", "Table").await.unwrap();
        graph_store.add_entity_node("B", "Table").await.unwrap();

        // Add edge with TWO_TO_ONE propagation (propagates backward)
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::TwoToOne)
            .await
            .unwrap();

        // Verify graph structure
        assert_eq!(graph_store.node_count(), 2);
        assert_eq!(graph_store.edge_count(), 1);

        // Verify edge direction: A -> B (edge is still from A to B)
        let neighbors = graph_store
            .get_neighbors("A", TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "B");
    }

    #[tokio::test]
    async fn test_classification_propagation_none() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create a chain: A -(NONE)-> B
        graph_store.add_entity_node("A", "Table").await.unwrap();
        graph_store.add_entity_node("B", "Table").await.unwrap();

        // Add edge with NONE propagation
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::None)
            .await
            .unwrap();

        // Verify graph structure
        assert_eq!(graph_store.node_count(), 2);
        assert_eq!(graph_store.edge_count(), 1);
    }

    #[tokio::test]
    async fn test_classification_propagation_both() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store, rel_store);

        // Create nodes A and B
        graph_store.add_entity_node("A", "Table").await.unwrap();
        graph_store.add_entity_node("B", "Table").await.unwrap();

        // Add edge with BOTH propagation
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::Both)
            .await
            .unwrap();

        // Verify graph structure
        assert_eq!(graph_store.node_count(), 2);
        assert_eq!(graph_store.edge_count(), 1);

        // Verify neighbors in both directions
        let neighbors = graph_store
            .get_neighbors("A", TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "B");

        let neighbors = graph_store
            .get_neighbors("B", TraversalDirection::Input)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id.as_str(), "A");
    }

    #[tokio::test]
    async fn test_get_all_classifications() {
        let (entity_store, rel_store) = create_test_stores();
        let graph_store = InMemoryGraphStore::new(entity_store.clone(), rel_store);

        // Create entities in the mock store first (required for get_all_classifications)
        let entity_a = Entity::new("Table").with_guid("A");
        let entity_b = Entity::new("Table").with_guid("B");
        entity_store.create_entity(&entity_a).await.unwrap();
        entity_store.create_entity(&entity_b).await.unwrap();

        // Add nodes to graph
        graph_store.add_entity_node("A", "Table").await.unwrap();
        graph_store.add_entity_node("B", "Table").await.unwrap();

        // Add edge with ONE_TO_TWO propagation
        graph_store
            .add_relationship_edge("rel-1", "flow", "A", "B", PropagateTags::OneToTwo)
            .await
            .unwrap();

        // Get all classifications for A (should return empty since no classifications)
        let all_classifications = graph_store.get_all_classifications("A").await.unwrap();
        assert!(all_classifications.is_empty());

        // Get all classifications for non-existent entity should fail
        let result = graph_store.get_all_classifications("nonexistent").await;
        assert!(result.is_err());
    }
}
