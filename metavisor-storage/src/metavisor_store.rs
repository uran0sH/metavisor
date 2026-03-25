//! MetavisorStore implementation - Unified abstraction layer
//!
//! Coordinates KV storage (entities, relationships, types) and Graph storage
//! (lineage, classification propagation) with transactional guarantees.
//!
//! # Transaction Support
//!
//! This implementation uses Write Ahead Logging (WAL) to ensure cross-storage
//! atomicity. Operations are logged before execution and can be recovered
//! after crashes.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use metavisor_core::{
    Classification, Entity, EntityDef, EntityHeader, EntityStore, GraphStats, GraphStore,
    LineageNode, LineageQueryOptions, LineageResult, MetavisorStore, ObjectId, PropagateTags,
    Relationship, RelationshipDef, RelationshipEndDef, RelationshipHeader, RelationshipStore,
    Result, TraversalDirection, TypeDef, TypeStore,
};

use crate::transaction::{
    InitializationResult, TransactionManager, TransactionalEntityBuilder, TransactionalRelationshipBuilder,
};
use crate::wal::{Transaction, WriteAheadLog};

/// DefaultMetavisorStore - Default implementation of MetavisorStore
///
/// This implementation provides:
/// - Transactional operations across KV and Graph storage using WAL
/// - Automatic graph synchronization on entity/relationship changes
/// - Unified interface for all metadata operations
/// - Crash recovery support
pub struct DefaultMetavisorStore {
    type_store: Arc<dyn TypeStore>,
    entity_store: Arc<dyn EntityStore>,
    relationship_store: Arc<dyn RelationshipStore>,
    graph_store: Arc<dyn GraphStore>,
    /// WAL for durability
    wal: Option<Arc<WriteAheadLog>>,
    /// Transaction manager for recovery
    tx_manager: Option<Arc<TransactionManager>>,
    /// Blocks writes while startup recovery is running
    write_barrier: Arc<RwLock<()>>,
}

impl DefaultMetavisorStore {
    /// Create a new DefaultMetavisorStore (legacy, without WAL)
    pub fn new(
        type_store: Arc<dyn TypeStore>,
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
        graph_store: Arc<dyn GraphStore>,
    ) -> Self {
        Self {
            type_store,
            entity_store,
            relationship_store,
            graph_store,
            wal: None,
            tx_manager: None,
            write_barrier: Arc::new(RwLock::new(())),
        }
    }

    /// Create a new DefaultMetavisorStore with WAL support
    ///
    /// This version accepts a concrete GrafeoGraphStore for transaction recovery support.
    pub fn with_wal(
        type_store: Arc<dyn TypeStore>,
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
        graph_store: Arc<crate::GrafeoGraphStore>,
        wal: Arc<WriteAheadLog>,
    ) -> Self {
        let tx_manager = Some(Arc::new(TransactionManager::new(
            wal.clone(),
            entity_store.clone(),
            relationship_store.clone(),
            graph_store.clone(),
        )));

        // Convert to trait object for storage
        let graph_store_trait: Arc<dyn GraphStore> = graph_store;

        Self {
            type_store,
            entity_store,
            relationship_store,
            graph_store: graph_store_trait,
            wal: Some(wal),
            tx_manager,
            write_barrier: Arc::new(RwLock::new(())),
        }
    }

    /// Initialize the graph from persisted data with optional consistency repair
    ///
    /// This method rebuilds the graph from KV data. If `repair_consistency` is true,
    /// it will also check and repair any inconsistencies between KV and graph storage.
    pub async fn initialize(&self) -> Result<()> {
        self.graph_store.rebuild_graph().await
    }

    /// Initialize with WAL recovery and consistency check
    ///
    /// This method should be called on startup to:
    /// 1. Recover any pending transactions from WAL (repair data consistency)
    /// 2. Rebuild the graph from KV data
    /// 3. Run consistency check to ensure KV and Graph are in sync
    /// 4. Clean up old committed WAL records
    /// 5. Mark propagation as pending if any data was recovered
    ///
    /// The recovery order is important:
    /// - First WAL recovery ensures core data consistency
    /// - Then consistency check repairs any remaining divergence
    /// - Cleanup prevents WAL from growing indefinitely
    /// - Finally, propagation is marked pending to update derived data
    pub async fn initialize_with_recovery(&self) -> Result<InitializationResult> {
        let _recovery_guard = self.write_barrier.write().await;

        // Step 1: Run WAL recovery if available
        // This repairs any incomplete transactions from previous crashes
        let wal_recovery = if let Some(ref manager) = self.tx_manager {
            manager.recover_all().await
        } else {
            vec![]
        };
        
        let wal_recovered = wal_recovery.iter().any(|r| r.operations_recovered > 0);

        // Step 2: Rebuild graph from KV data
        // This ensures graph is in sync with KV store
        self.graph_store.rebuild_graph().await?;

        // Step 3: Run consistency check to detect and repair any remaining issues
        // This catches cases where WAL wasn't available or failed to recover everything
        let (consistency_report, repair_result) = self.repair_consistency().await?;
        
        // Step 4: Clean up old committed WAL records (keep 7 days)
        if let Some(ref manager) = self.tx_manager {
            match manager.cleanup_old_transactions(24 * 7).await {
                Ok(cleaned) => {
                    if cleaned > 0 {
                        tracing::info!("WAL cleanup: removed {} old committed records", cleaned);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to cleanup old WAL records: {}", e);
                    // Non-fatal error, continue
                }
            }
        }
        
        // Step 5: Mark propagation as pending if any data was changed
        // This ensures derived data (propagated classifications) will be updated
        if wal_recovered || repair_result.total_repaired() > 0 {
            tracing::info!(
                "Data was recovered (WAL: {} ops, Repair: {} items), marking propagation pending",
                wal_recovery.iter().map(|r| r.operations_recovered).sum::<usize>(),
                repair_result.total_repaired()
            );
            self.mark_propagation_pending();
        }

        Ok(InitializationResult {
            wal_recovery,
            consistency_report,
            repair_result,
        })
    }

    /// Spawn background maintenance tasks
    /// 
    /// This spawns:
    /// - WAL cleanup task (if WAL is enabled)
    /// 
    /// # Arguments
    /// * `wal_cleanup_interval_hours` - How often to clean WAL (default: 24 hours)
    /// * `wal_retention_hours` - How long to keep WAL records (default: 168 hours = 7 days)
    pub fn spawn_maintenance_tasks(&self, wal_cleanup_interval_hours: Option<u64>, wal_retention_hours: Option<u64>) {
        if let Some(ref manager) = self.tx_manager {
            Arc::clone(manager).spawn_cleanup_task(wal_cleanup_interval_hours, wal_retention_hours);
            tracing::info!("WAL cleanup task spawned");
        }
    }

    /// Check consistency between KV and graph storage
    ///
    /// Returns a report detailing any inconsistencies found.
    pub async fn check_consistency(&self) -> Result<crate::ConsistencyReport> {
        crate::ConsistencyChecker::check_consistency(
            self.entity_store.as_ref(),
            self.relationship_store.as_ref(),
            self.graph_store.as_ref(),
        )
        .await
    }

    /// Repair inconsistencies between KV and graph storage
    ///
    /// This method checks for inconsistencies and repairs them by syncing
    /// missing data from KV to the graph.
    pub async fn repair_consistency(&self) -> Result<(crate::ConsistencyReport, crate::RepairResult)> {
        crate::ConsistencyChecker::check_and_repair(
            self.entity_store.as_ref(),
            self.relationship_store.as_ref(),
            self.graph_store.as_ref(),
        )
        .await
    }

    /// Explicitly flush pending classification propagation
    ///
    /// This is useful after bulk operations to ensure classification
    /// propagation is up-to-date before querying.
    pub async fn flush_classification_propagation(&self) -> Result<()> {
        use crate::GrafeoGraphStore;
        if let Some(grafeo_store) = self.graph_store.as_any().downcast_ref::<GrafeoGraphStore>() {
            grafeo_store.flush_propagation().await?;
        }
        Ok(())
    }

    /// Check if classification propagation is pending
    pub fn is_classification_propagation_pending(&self) -> bool {
        use crate::GrafeoGraphStore;
        if let Some(grafeo_store) = self.graph_store.as_any().downcast_ref::<GrafeoGraphStore>() {
            grafeo_store.is_propagation_pending()
        } else {
            false
        }
    }

    /// Get a reference to the underlying graph store for advanced operations
    ///
    /// This can be used to spawn background propagation tasks or access
    /// Grafeo-specific features.
    pub fn graph_store(&self) -> &Arc<dyn GraphStore> {
        &self.graph_store
    }

    /// Get WAL if configured
    pub fn wal(&self) -> Option<&Arc<WriteAheadLog>> {
        self.wal.as_ref()
    }

    /// Get transaction manager if configured
    pub fn tx_manager(&self) -> Option<&Arc<TransactionManager>> {
        self.tx_manager.as_ref()
    }

    /// Begin a new transaction
    ///
    /// Returns None if WAL is not configured.
    /// Remember to call commit() or rollback() on the returned Transaction.
    pub async fn begin_transaction(&self) -> Option<Transaction> {
        if let Some(ref wal) = self.wal {
            Transaction::begin(wal.clone()).await.ok()
        } else {
            None
        }
    }

    async fn acquire_write_guard(&self) -> Option<OwnedRwLockReadGuard<()>> {
        if self.wal.is_some() {
            Some(Arc::clone(&self.write_barrier).read_owned().await)
        } else {
            None
        }
    }

    /// Bulk create entities with WAL support and optimized graph updates
    ///
    /// This method creates multiple entities transactionally (if WAL is configured)
    /// and only triggers classification propagation once at the end.
    pub async fn bulk_create_entities(&self, entities: &[Entity]) -> Result<Vec<String>> {
        // If WAL is configured, use transaction
        if self.wal.is_some() {
            self.bulk_create_entities_with_wal(entities).await
        } else {
            self.bulk_create_entities_without_wal(entities).await
        }
    }

    /// Bulk create entities with WAL transaction
    async fn bulk_create_entities_with_wal(&self, entities: &[Entity]) -> Result<Vec<String>> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;
        
        let mut guids = Vec::with_capacity(entities.len());

        for entity in entities {
            // Log KV operation
            let builder = TransactionalEntityBuilder::new(entity.clone());
            tx.log_operation(builder.build_create_op().map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?)
                .await
                .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

            // Log graph operation
            tx.log_operation(builder.build_graph_node_op())
                .await
                .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

            // Execute KV operation
            let guid = match self.entity_store.create_entity(entity).await {
                Ok(g) => g,
                Err(e) => {
                    let _ = tx.abort().await;
                    return Err(e);
                }
            };
            guids.push(guid.clone());
        }

        if let Err(e) = tx.mark_kv_applied().await {
            let _ = tx.abort().await;
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark KV applied: {}",
                e
            )));
        }

        // Commit the transaction
        if let Err(e) = tx.commit().await {
            return Err(metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)));
        }

        for (entity, guid) in entities.iter().zip(&guids) {
            if let Err(e) = self.graph_store.add_entity_node(guid, &entity.type_name).await {
                return Err(metavisor_core::CoreError::Storage(format!(
                    "Committed KV write but failed to project graph: {}",
                    e
                )));
            }
        }

        if let Err(e) = tx.mark_graph_applied().await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark graph applied: {}",
                e
            )));
        }

        // Mark propagation as pending AFTER successful commit
        // This ensures data consistency even if propagation fails
        self.mark_propagation_pending();

        Ok(guids)
    }

    /// Helper method to mark propagation as pending (with downcast)
    fn mark_propagation_pending(&self) {
        use crate::GrafeoGraphStore;
        if let Some(grafeo_store) = self.graph_store.as_any().downcast_ref::<GrafeoGraphStore>() {
            grafeo_store.mark_propagation_pending();
        }
    }

    async fn update_entity_with_wal(&self, entity: &Entity) -> Result<()> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;

        let builder = TransactionalEntityBuilder::new(entity.clone());
        tx.log_operation(
            builder
                .build_update_op()
                .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?,
        )
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        tx.log_operation(builder.build_graph_node_op())
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        self.entity_store.update_entity(entity).await?;
        tx.mark_kv_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark KV applied: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)))?;

        let guid = entity.guid.as_deref().unwrap_or("");
        self.graph_store
            .add_entity_node(guid, &entity.type_name)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )))?;

        tx.mark_graph_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark graph applied: {}", e)))?;
        self.mark_propagation_pending();
        Ok(())
    }

    async fn delete_entity_with_wal(&self, guid: &str) -> Result<()> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;

        tx.log_operation(crate::wal::OpType::DeleteEntity {
            guid: guid.to_string(),
        })
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        tx.log_operation(crate::wal::OpType::RemoveGraphNode {
            entity_guid: guid.to_string(),
        })
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        self.entity_store.delete_entity(guid).await?;
        tx.mark_kv_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark KV applied: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)))?;

        self.graph_store
            .remove_entity_node(guid)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )))?;
        tx.mark_graph_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark graph applied: {}", e)))?;
        self.mark_propagation_pending();
        Ok(())
    }

    async fn update_relationship_with_wal(&self, relationship: &Relationship) -> Result<()> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;

        let guid = relationship.guid.clone().ok_or_else(|| {
            metavisor_core::CoreError::Validation("Relationship GUID is required for update".to_string())
        })?;
        let end1_guid = relationship
            .end1
            .as_ref()
            .and_then(|end| end.guid.clone())
            .ok_or_else(|| {
                metavisor_core::CoreError::Validation(
                    "Relationship endpoints must have GUIDs".to_string(),
                )
            })?;
        let end2_guid = relationship
            .end2
            .as_ref()
            .and_then(|end| end.guid.clone())
            .ok_or_else(|| {
                metavisor_core::CoreError::Validation(
                    "Relationship endpoints must have GUIDs".to_string(),
                )
            })?;
        let propagate_tags = relationship.propagate_tags.unwrap_or(PropagateTags::None);

        let builder = TransactionalRelationshipBuilder::new(relationship.clone());
        tx.log_operation(
            builder
                .build_update_op()
                .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?,
        )
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        tx.log_operation(crate::wal::OpType::RemoveGraphEdge {
            relationship_guid: guid.clone(),
        })
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        tx.log_operation(crate::wal::OpType::AddGraphEdge {
            relationship_guid: guid.clone(),
            relationship_type: relationship.type_name.clone(),
            from_guid: end1_guid.clone(),
            to_guid: end2_guid.clone(),
            propagate_tags: Some(
                serde_json::to_string(&propagate_tags)
                    .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?,
            ),
        })
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        self.relationship_store.update_relationship(relationship).await?;
        tx.mark_kv_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark KV applied: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)))?;

        self.graph_store
            .remove_relationship_edge(&guid)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )))?;
        self.graph_store
            .add_relationship_edge(
                &guid,
                &relationship.type_name,
                &end1_guid,
                &end2_guid,
                propagate_tags,
            )
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )))?;
        tx.mark_graph_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark graph applied: {}", e)))?;
        self.mark_propagation_pending();
        Ok(())
    }

    async fn delete_relationship_with_wal(&self, guid: &str) -> Result<()> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;

        tx.log_operation(crate::wal::OpType::DeleteRelationship {
            guid: guid.to_string(),
        })
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        tx.log_operation(crate::wal::OpType::RemoveGraphEdge {
            relationship_guid: guid.to_string(),
        })
        .await
        .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        self.relationship_store.delete_relationship(guid).await?;
        tx.mark_kv_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark KV applied: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)))?;

        self.graph_store
            .remove_relationship_edge(guid)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )))?;
        tx.mark_graph_applied()
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(format!("Failed to mark graph applied: {}", e)))?;
        self.mark_propagation_pending();
        Ok(())
    }

    /// Bulk create entities without WAL (legacy behavior)
    async fn bulk_create_entities_without_wal(&self, entities: &[Entity]) -> Result<Vec<String>> {
        let mut guids = Vec::with_capacity(entities.len());

        // Step 1: Create all entities in KV store
        for entity in entities {
            let guid = self.entity_store.create_entity(entity).await?;
            guids.push(guid);
        }

        // Step 2: Add all nodes to graph (without triggering propagation each time)
        for (entity, guid) in entities.iter().zip(&guids) {
            if let Err(e) = self
                .graph_store
                .add_entity_node(guid, &entity.type_name)
                .await
            {
                tracing::error!(
                    "Failed to add entity {} to graph after KV creation: {}",
                    guid, e
                );
            }
        }

        // Step 3: Create lineage relationships
        for (entity, guid) in entities.iter().zip(&guids) {
            let mut created_entity = entity.clone();
            created_entity.guid = Some(guid.clone());
            if let Err(e) = self.create_lineage_relationships_for_entity(&created_entity, guid).await {
                tracing::error!(
                    "Failed to create lineage relationships for entity {}: {}",
                    guid, e
                );
            }
        }

        Ok(guids)
    }

    /// Helper method to update entity and sync classifications to graph
    async fn update_entity_classifications(&self, entity: &Entity) -> Result<()> {
        // Update the entity in KV store
        self.entity_store.update_entity(entity).await?;

        // Extract classification names for graph update
        let classification_names: Vec<String> = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        // Update the graph node to trigger re-propagation
        self.graph_store
            .update_entity_node(
                entity.guid.as_deref().unwrap_or(""),
                &entity.type_name,
                None,
                classification_names,
            )
            .await?;

        Ok(())
    }

    async fn ensure_lineage_relationship_types(&self, process_type: &str) -> Result<()> {
        let process_entity_type = match self.type_store.get_type(process_type).await? {
            TypeDef::Entity(def) => def,
            _ => {
                return Err(metavisor_core::CoreError::Validation(format!(
                    "Type '{}' is not an entity type",
                    process_type
                )))
            }
        };

        let input_type = self
            .extract_lineage_endpoint_type(&process_entity_type, "inputs")
            .unwrap_or_else(|| "column_meta".to_string());
        let output_type = self
            .extract_lineage_endpoint_type(&process_entity_type, "outputs")
            .unwrap_or_else(|| "column_meta".to_string());

        self.ensure_relationship_type_exists(
            RelationshipDef::new("process_inputs")
                .end1(
                    RelationshipEndDef::new(&input_type, "input")
                        .cardinality(metavisor_core::Cardinality::Set),
                )
                .end2(
                    RelationshipEndDef::new(process_type, "process")
                        .cardinality(metavisor_core::Cardinality::Single),
                ),
        )
        .await?;

        self.ensure_relationship_type_exists(
            RelationshipDef::new("process_outputs")
                .end1(
                    RelationshipEndDef::new(process_type, "process")
                        .cardinality(metavisor_core::Cardinality::Single),
                )
                .end2(
                    RelationshipEndDef::new(&output_type, "output")
                        .cardinality(metavisor_core::Cardinality::Set),
                ),
        )
        .await?;

        Ok(())
    }

    async fn ensure_relationship_type_exists(
        &self,
        relationship_def: RelationshipDef,
    ) -> Result<()> {
        let type_name = relationship_def.name.clone();
        if self.type_store.type_exists(&type_name).await? {
            return Ok(());
        }
        self.type_store
            .create_type(&TypeDef::Relationship(relationship_def))
            .await
    }

    fn extract_lineage_endpoint_type(
        &self,
        entity_def: &EntityDef,
        attr_name: &str,
    ) -> Option<String> {
        entity_def
            .attribute_defs
            .iter()
            .find(|attr| attr.name == attr_name)
            .and_then(|attr| Self::parse_array_object_type(&attr.type_name))
    }

    fn parse_array_object_type(type_name: &str) -> Option<String> {
        let inner = type_name
            .strip_prefix("array<")
            .and_then(|s| s.strip_suffix('>'))?;
        inner
            .strip_prefix("objectid<")
            .and_then(|s| s.strip_suffix('>'))
            .map(ToString::to_string)
    }

    fn parse_object_ids(value: Option<&serde_json::Value>) -> Vec<ObjectId> {
        value
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|item| serde_json::from_value::<ObjectId>(item.clone()).ok())
            .collect()
    }

    async fn create_lineage_relationships_for_entity(
        &self,
        entity: &Entity,
        entity_guid: &str,
    ) -> Result<()> {
        let inputs = Self::parse_object_ids(entity.attributes.get("inputs"));
        let outputs = Self::parse_object_ids(entity.attributes.get("outputs"));

        if inputs.is_empty() && outputs.is_empty() {
            return Ok(());
        }

        self.ensure_lineage_relationship_types(&entity.type_name)
            .await?;

        let mut created_relationship_guids = Vec::new();

        for input in inputs {
            let relationship = Relationship::new("process_inputs")
                .with_end1(self.resolve_object_id(input).await?)
                .with_end2(ObjectId::by_guid(&entity.type_name, entity_guid))
                .with_propagate_tags(PropagateTags::OneToTwo);
            match self.create_relationship(&relationship).await {
                Ok(guid) => created_relationship_guids.push(guid),
                Err(err) => {
                    self.rollback_created_relationships(&created_relationship_guids)
                        .await;
                    return Err(err);
                }
            }
        }

        for output in outputs {
            let relationship = Relationship::new("process_outputs")
                .with_end1(ObjectId::by_guid(&entity.type_name, entity_guid))
                .with_end2(self.resolve_object_id(output).await?)
                .with_propagate_tags(PropagateTags::OneToTwo);
            match self.create_relationship(&relationship).await {
                Ok(guid) => created_relationship_guids.push(guid),
                Err(err) => {
                    self.rollback_created_relationships(&created_relationship_guids)
                        .await;
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    async fn rollback_created_relationships(&self, relationship_guids: &[String]) {
        for guid in relationship_guids.iter().rev() {
            let _ = self.delete_relationship(guid).await;
        }
    }

    async fn resolve_object_id(&self, object_id: ObjectId) -> Result<ObjectId> {
        if object_id.guid.is_some() {
            return Ok(object_id);
        }

        let entity = self
            .entity_store
            .get_entity_by_unique_attrs(&object_id.type_name, &object_id.unique_attributes)
            .await?;

        let guid = entity.guid.ok_or_else(|| {
            metavisor_core::CoreError::Validation(format!(
                "Entity '{}' resolved from unique attributes has no GUID",
                object_id.type_name
            ))
        })?;

        Ok(ObjectId::by_guid(object_id.type_name, guid))
    }

    /// Create entity with WAL transaction support
    ///
    /// If WAL is configured, this operation is atomic and can be recovered
    /// after crashes.
    async fn create_entity_with_wal(&self, entity: &Entity) -> Result<String> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;
        
        // Log operations
        let builder = TransactionalEntityBuilder::new(entity.clone());
        
        tx.log_operation(builder.build_create_op().map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        
        tx.log_operation(builder.build_graph_node_op())
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        // Execute operations
        let guid = match self.entity_store.create_entity(entity).await {
            Ok(g) => g,
            Err(e) => {
                let _ = tx.abort().await;
                return Err(e);
            }
        };

        if let Err(e) = tx.mark_kv_applied().await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark KV applied: {}",
                e
            )));
        }

        // Commit the transaction
        if let Err(e) = tx.commit().await {
            return Err(metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)));
        }

        if let Err(e) = self.graph_store.add_entity_node(&guid, &entity.type_name).await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )));
        }

        if let Err(e) = tx.mark_graph_applied().await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark graph applied: {}",
                e
            )));
        }

        // Mark propagation as pending AFTER successful commit
        // This ensures data consistency even if propagation fails
        self.mark_propagation_pending();

        Ok(guid)
    }

    /// Create relationship with WAL transaction support
    async fn create_relationship_with_wal(&self, relationship: &Relationship) -> Result<String> {
        let _write_guard = self.acquire_write_guard().await;
        let mut tx = self.begin_transaction().await.ok_or_else(|| {
            metavisor_core::CoreError::Storage("WAL not configured".to_string())
        })?;
        
        let mut resolved_relationship = relationship.clone();
        if let Some(end1) = resolved_relationship.end1.take() {
            match self.resolve_object_id(end1).await {
                Ok(obj) => resolved_relationship.end1 = Some(obj),
                Err(e) => {
                    let _ = tx.abort().await;
                    return Err(e);
                }
            }
        }
        if let Some(end2) = resolved_relationship.end2.take() {
            match self.resolve_object_id(end2).await {
                Ok(obj) => resolved_relationship.end2 = Some(obj),
                Err(e) => {
                    let _ = tx.abort().await;
                    return Err(e);
                }
            }
        }

        // Log operations
        let builder = TransactionalRelationshipBuilder::new(resolved_relationship.clone());
        
        tx.log_operation(builder.build_create_op().map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;
        
        tx.log_operation(builder.build_graph_edge_op().map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?)
            .await
            .map_err(|e| metavisor_core::CoreError::Storage(e.to_string()))?;

        // Execute operations
        let guid = match self.relationship_store.create_relationship(&resolved_relationship).await {
            Ok(g) => g,
            Err(e) => {
                let _ = tx.abort().await;
                return Err(e);
            }
        };

        let (end1_guid, end2_guid) = match (&resolved_relationship.end1, &resolved_relationship.end2) {
            (Some(e1), Some(e2)) => match (&e1.guid, &e2.guid) {
                (Some(g1), Some(g2)) => (g1.clone(), g2.clone()),
                _ => {
                    let _ = tx.abort().await;
                    return Err(metavisor_core::CoreError::Validation(
                        "Relationship endpoints must have GUIDs".to_string(),
                    ));
                }
            },
            _ => {
                let _ = tx.abort().await;
                return Err(metavisor_core::CoreError::Validation(
                    "Relationship must have both endpoints".to_string(),
                ));
            }
        };

        let propagate_tags = resolved_relationship
            .propagate_tags
            .unwrap_or(PropagateTags::None);

        if let Err(e) = tx.mark_kv_applied().await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark KV applied: {}",
                e
            )));
        }

        if let Err(e) = tx.commit().await {
            return Err(metavisor_core::CoreError::Storage(format!("Failed to commit: {}", e)));
        }

        if let Err(e) = self.graph_store
            .add_relationship_edge(
                &guid,
                &resolved_relationship.type_name,
                &end1_guid,
                &end2_guid,
                propagate_tags,
            )
            .await
        {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Committed KV write but failed to project graph: {}",
                e
            )));
        }

        if let Err(e) = tx.mark_graph_applied().await {
            return Err(metavisor_core::CoreError::Storage(format!(
                "Failed to mark graph applied: {}",
                e
            )));
        }

        // Mark propagation as pending AFTER successful commit
        // Relationship changes may affect tag propagation
        self.mark_propagation_pending();

        Ok(guid)
    }
}

#[async_trait]
impl MetavisorStore for DefaultMetavisorStore {
    // ========================================================================
    // Type Operations
    // ========================================================================

    async fn create_type(&self, type_def: &TypeDef) -> Result<()> {
        self.type_store.create_type(type_def).await
    }

    async fn get_type(&self, name: &str) -> Result<TypeDef> {
        self.type_store.get_type(name).await
    }

    async fn update_type(&self, type_def: &TypeDef) -> Result<()> {
        self.type_store.update_type(type_def).await
    }

    async fn delete_type(&self, name: &str) -> Result<()> {
        self.type_store.delete_type(name).await
    }

    async fn type_exists(&self, name: &str) -> Result<bool> {
        self.type_store.type_exists(name).await
    }

    async fn list_types(&self) -> Result<Vec<String>> {
        self.type_store.list_types().await
    }

    // ========================================================================
    // Entity Operations (with Graph sync)
    // ========================================================================

    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        // If WAL is configured, use transactional version
        if self.wal.is_some() {
            match self.create_entity_with_wal(entity).await {
                Ok(guid) => return Ok(guid),
                Err(e) => {
                    tracing::warn!("WAL transaction failed, falling back to legacy: {}", e);
                    // Fall through to legacy implementation
                }
            }
        }

        // Legacy implementation with manual rollback
        // Step 1: Create entity in KV store
        let guid = self.entity_store.create_entity(entity).await?;

        // Step 2: Add node to graph
        if let Err(e) = self
            .graph_store
            .add_entity_node(&guid, &entity.type_name)
            .await
        {
            tracing::error!(
                "Failed to add entity {} to graph, attempting rollback: {}",
                guid, e
            );
            if let Err(rollback_err) = self.entity_store.delete_entity(&guid).await {
                tracing::error!(
                    "CRITICAL: Rollback failed for entity {}. Manual repair needed: {}",
                    guid, rollback_err
                );
            }
            return Err(e);
        }

        let mut created_entity = entity.clone();
        created_entity.guid = Some(guid.clone());

        // Step 3: Create lineage relationships
        if let Err(e) = self
            .create_lineage_relationships_for_entity(&created_entity, &guid)
            .await
        {
            tracing::error!(
                "Failed to create lineage for entity {}. Attempting rollback: {}",
                guid, e
            );
            if let Err(graph_err) = self.graph_store.remove_entity_node(&guid).await {
                tracing::warn!("Rollback: failed to remove entity {} from graph: {}", guid, graph_err);
            }
            if let Err(kv_err) = self.entity_store.delete_entity(&guid).await {
                tracing::error!("CRITICAL: Rollback failed to delete entity {} from KV: {}", guid, kv_err);
            }
            return Err(e);
        }

        Ok(guid)
    }

    async fn get_entity(&self, guid: &str) -> Result<Entity> {
        self.entity_store.get_entity(guid).await
    }

    async fn get_entity_by_unique_attrs(
        &self,
        type_name: &str,
        unique_attrs: &HashMap<String, serde_json::Value>,
    ) -> Result<Entity> {
        self.entity_store
            .get_entity_by_unique_attrs(type_name, unique_attrs)
            .await
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        if self.wal.is_some() {
            return self.update_entity_with_wal(entity).await;
        }

        self.entity_store.update_entity(entity).await?;

        // Update the node in the graph incrementally (much faster than rebuild_graph)
        let display_name = entity.attributes.get("name").and_then(|v| v.as_str());
        let classifications = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        let updated = self
            .graph_store
            .update_entity_node(
                entity.guid.as_deref().unwrap_or(""),
                &entity.type_name,
                display_name,
                classifications,
            )
            .await?;

        // If the node wasn't found in the graph, add it
        if !updated {
            if let Some(ref guid) = entity.guid {
                self.graph_store
                    .add_entity_node(guid, &entity.type_name)
                    .await?;
            }
        }

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        if self.wal.is_some() {
            return self.delete_entity_with_wal(guid).await;
        }

        // Step 0: Get entity info first (needed for potential rollback)
        let entity = self.entity_store.get_entity(guid).await?;

        // Step 1: Remove from graph first
        self.graph_store.remove_entity_node(guid).await?;

        // Step 2: Delete from KV store
        if let Err(e) = self.entity_store.delete_entity(guid).await {
            // Rollback: re-add node to graph
            if let Err(rollback_err) = self
                .graph_store
                .add_entity_node(guid, &entity.type_name)
                .await
            {
                tracing::error!(
                    "Failed to rollback graph node after KV delete failure: {}",
                    rollback_err
                );
            }
            return Err(e);
        }

        Ok(())
    }

    async fn entity_exists(&self, guid: &str) -> Result<bool> {
        self.entity_store.entity_exists(guid).await
    }

    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>> {
        self.entity_store.list_entities_by_type(type_name).await
    }

    async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
        self.entity_store.list_entities().await
    }

    // ========================================================================
    // Relationship Operations (with Graph sync)
    // ========================================================================

    async fn create_relationship(&self, relationship: &Relationship) -> Result<String> {
        // If WAL is configured, use transactional version
        if self.wal.is_some() {
            match self.create_relationship_with_wal(relationship).await {
                Ok(guid) => return Ok(guid),
                Err(e) => {
                    tracing::warn!("WAL transaction failed, falling back to legacy: {}", e);
                }
            }
        }

        // Legacy implementation
        let mut resolved_relationship = relationship.clone();
        if let Some(end1) = resolved_relationship.end1.take() {
            resolved_relationship.end1 = Some(self.resolve_object_id(end1).await?);
        }
        if let Some(end2) = resolved_relationship.end2.take() {
            resolved_relationship.end2 = Some(self.resolve_object_id(end2).await?);
        }

        // Step 1: Create relationship in KV store
        let guid = self
            .relationship_store
            .create_relationship(&resolved_relationship)
            .await?;

        // Step 2: Add edge to graph
        let (end1_guid, end2_guid) =
            match (&resolved_relationship.end1, &resolved_relationship.end2) {
                (Some(e1), Some(e2)) => match (&e1.guid, &e2.guid) {
                    (Some(g1), Some(g2)) => (g1.clone(), g2.clone()),
                    _ => {
                        let _ = self.relationship_store.delete_relationship(&guid).await;
                        return Err(metavisor_core::CoreError::Validation(
                            "Relationship endpoints must have GUIDs".to_string(),
                        ));
                    }
                },
                _ => {
                    let _ = self.relationship_store.delete_relationship(&guid).await;
                    return Err(metavisor_core::CoreError::Validation(
                        "Relationship must have both endpoints".to_string(),
                    ));
                }
            };

        let propagate_tags = resolved_relationship
            .propagate_tags
            .unwrap_or(PropagateTags::None);

        if let Err(e) = self
            .graph_store
            .add_relationship_edge(
                &guid,
                &resolved_relationship.type_name,
                &end1_guid,
                &end2_guid,
                propagate_tags,
            )
            .await
        {
            tracing::error!(
                "Failed to add relationship {} to graph, attempting rollback: {}",
                guid, e
            );
            if let Err(rollback_err) = self.relationship_store.delete_relationship(&guid).await {
                tracing::error!(
                    "CRITICAL: Rollback failed for relationship {}. Manual repair needed: {}",
                    guid, rollback_err
                );
            }
            return Err(e);
        }

        Ok(guid)
    }

    async fn get_relationship(&self, guid: &str) -> Result<Relationship> {
        self.relationship_store.get_relationship(guid).await
    }

    async fn update_relationship(&self, relationship: &Relationship) -> Result<()> {
        if self.wal.is_some() {
            return self.update_relationship_with_wal(relationship).await;
        }

        self.relationship_store
            .update_relationship(relationship)
            .await?;

        self.graph_store.rebuild_graph().await?;

        Ok(())
    }

    async fn delete_relationship(&self, guid: &str) -> Result<()> {
        if self.wal.is_some() {
            return self.delete_relationship_with_wal(guid).await;
        }

        // Step 0: Get relationship info first (needed for potential rollback)
        let rel = self.relationship_store.get_relationship(guid).await?;

        // Extract endpoint GUIDs for graph operations
        let (end1_guid, end2_guid) = match (&rel.end1, &rel.end2) {
            (Some(e1), Some(e2)) => match (&e1.guid, &e2.guid) {
                (Some(g1), Some(g2)) => (g1.clone(), g2.clone()),
                _ => {
                    return Err(metavisor_core::CoreError::Validation(
                        "Relationship endpoints must have GUIDs".to_string(),
                    ));
                }
            },
            _ => {
                return Err(metavisor_core::CoreError::Validation(
                    "Relationship must have both endpoints".to_string(),
                ));
            }
        };

        let propagate_tags = rel.propagate_tags.unwrap_or(PropagateTags::None);

        // Step 1: Remove from graph first
        self.graph_store.remove_relationship_edge(guid).await?;

        // Step 2: Delete from KV store
        if let Err(e) = self.relationship_store.delete_relationship(guid).await {
            // Rollback: re-add edge to graph
            if let Err(rollback_err) = self
                .graph_store
                .add_relationship_edge(guid, &rel.type_name, &end1_guid, &end2_guid, propagate_tags)
                .await
            {
                tracing::error!(
                    "Failed to rollback graph edge after KV delete failure: {}",
                    rollback_err
                );
            }
            return Err(e);
        }

        Ok(())
    }

    async fn relationship_exists(&self, guid: &str) -> Result<bool> {
        self.relationship_store.relationship_exists(guid).await
    }

    async fn list_relationships_by_entity(
        &self,
        entity_guid: &str,
    ) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store
            .list_relationships_by_entity(entity_guid)
            .await
    }

    async fn list_relationships_by_type(&self, type_name: &str) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store
            .list_relationships_by_type(type_name)
            .await
    }

    async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store.list_relationships().await
    }

    // ========================================================================
    // Graph / Lineage Operations
    // ========================================================================

    async fn rebuild_graph(&self) -> Result<()> {
        self.graph_store.rebuild_graph().await
    }

    async fn get_lineage(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
        options: LineageQueryOptions,
    ) -> Result<LineageResult> {
        self.graph_store
            .get_lineage(entity_guid, direction, options)
            .await
    }

    async fn get_all_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        self.graph_store.get_all_classifications(entity_guid).await
    }

    async fn add_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Add new classifications (avoid duplicates)
        for classification in classifications {
            if !entity
                .classifications
                .iter()
                .any(|c| c.type_name == classification.type_name)
            {
                entity.classifications.push(classification.clone());
            }
        }

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn get_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        let entity = self.entity_store.get_entity(entity_guid).await?;
        Ok(entity.classifications)
    }

    async fn update_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Replace all classifications
        entity.classifications = classifications.to_vec();

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn remove_classification(
        &self,
        entity_guid: &str,
        classification_name: &str,
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Remove the classification
        entity
            .classifications
            .retain(|c| c.type_name != classification_name);

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn get_neighbors(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
    ) -> Result<Vec<LineageNode>> {
        self.graph_store.get_neighbors(entity_guid, direction).await
    }

    async fn path_exists(&self, from_guid: &str, to_guid: &str, max_depth: usize) -> Result<bool> {
        self.graph_store
            .path_exists(from_guid, to_guid, max_depth)
            .await
    }

    fn graph_stats(&self) -> GraphStats {
        GraphStats {
            node_count: self.graph_store.node_count(),
            edge_count: self.graph_store.edge_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{AttributeDef, EntityDef, ObjectId, RelationshipDef, RelationshipEndDef};
    use serde_json::json;
    use tempfile::TempDir;

    use crate::entity_store::KvEntityStore;
    use crate::grafeo_graph_store::GrafeoGraphStore;
    use crate::kv::KvStore;
    use crate::relationship_store::KvRelationshipStore;
    use crate::wal::OpType;
    use crate::type_store::KvTypeStore;

    async fn create_test_store() -> (Arc<DefaultMetavisorStore>, Arc<KvTypeStore>) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store = Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));

        // Create Grafeo graph store
        let graph_store: Arc<dyn GraphStore> = Arc::new(
            GrafeoGraphStore::new_in_memory(
                entity_store.clone(),
                relationship_store.clone(),
            )
            .unwrap(),
        );

        let store = Arc::new(DefaultMetavisorStore::new(
            type_store.clone(),
            entity_store,
            relationship_store,
            graph_store,
        ));

        (store, type_store)
    }

    async fn create_test_store_with_wal() -> (Arc<DefaultMetavisorStore>, Arc<KvTypeStore>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let wal = Arc::new(WriteAheadLog::new(kv.clone()));

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store = Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));

        // Create Grafeo graph store (concrete type for with_wal)
        let graph_store = Arc::new(
            GrafeoGraphStore::new_in_memory(
                entity_store.clone(),
                relationship_store.clone(),
            )
            .unwrap(),
        );

        let store = Arc::new(DefaultMetavisorStore::with_wal(
            type_store.clone(),
            entity_store,
            relationship_store,
            graph_store,
            wal,
        ));

        (store, type_store, tempdir)
    }

    async fn create_test_types(type_store: &KvTypeStore) {
        // Create entity types
        let table_def =
            EntityDef::new("Table").attribute(AttributeDef::new("name", "string").required());
        let column_def =
            EntityDef::new("Column").attribute(AttributeDef::new("name", "string").required());

        type_store
            .create_type(&TypeDef::from(table_def))
            .await
            .unwrap();
        type_store
            .create_type(&TypeDef::from(column_def))
            .await
            .unwrap();

        // Create relationship type
        let rel_def = RelationshipDef::new("table_columns")
            .category(metavisor_core::RelationshipCategory::Composition)
            .propagate_tags(PropagateTags::OneToTwo)
            .end1(RelationshipEndDef::new("Table", "columns"))
            .end2(RelationshipEndDef::new("Column", "table"));

        type_store
            .create_type(&TypeDef::from(rel_def))
            .await
            .unwrap();

        let chain_rel_def = RelationshipDef::new("column_chain")
            .category(metavisor_core::RelationshipCategory::Association)
            .propagate_tags(PropagateTags::OneToTwo)
            .end1(RelationshipEndDef::new("Column", "from"))
            .end2(RelationshipEndDef::new("Column", "to"));

        type_store
            .create_type(&TypeDef::from(chain_rel_def))
            .await
            .unwrap();
    }


    #[tokio::test]
    async fn test_create_entity_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Initially graph should be empty
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 0);

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify entity exists in KV
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "Table");

        // Verify graph was updated
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 1);
    }

    #[tokio::test]
    async fn test_create_entity_with_wal() {
        let (store, type_store, _temp) = create_test_store_with_wal().await;
        create_test_types(&type_store).await;

        // Initially graph should be empty
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 0);

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify entity exists in KV
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "Table");

        // Verify graph was updated
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 1);
    }

    #[tokio::test]
    async fn test_transaction_support() {
        let (store, type_store, _temp) = create_test_store_with_wal().await;
        create_test_types(&type_store).await;

        // Begin a transaction
        let mut tx = store.begin_transaction().await.expect("WAL should be configured");
        
        // Log an operation
        let op = OpType::CreateEntity {
            guid: "test-guid".to_string(),
            entity_type: "Table".to_string(),
            serialized_data: vec![1, 2, 3],
        };
        let seq = tx.log_operation(op).await.unwrap();
        assert_eq!(seq, 0);

        // Commit the transaction
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_bulk_create_entities_with_wal() {
        let (store, type_store, _temp) = create_test_store_with_wal().await;
        create_test_types(&type_store).await;

        let entities: Vec<Entity> = (0..5)
            .map(|i| Entity::new("Table").with_attribute("name", json!(format!("table_{}", i))))
            .collect();

        let guids = store.bulk_create_entities(&entities).await.unwrap();
        assert_eq!(guids.len(), 5);

        // Verify all entities exist
        for guid in &guids {
            assert!(store.entity_exists(guid).await.unwrap());
        }

        // Verify graph was updated
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 5);
    }

    #[tokio::test]
    async fn test_delete_entity_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        assert_eq!(store.graph_stats().node_count, 1);

        // Delete entity
        store.delete_entity(&guid).await.unwrap();

        // Verify entity is deleted from KV
        assert!(!store.entity_exists(&guid).await.unwrap());

        // Verify graph was updated
        assert_eq!(store.graph_stats().node_count, 0);
    }

    #[tokio::test]
    async fn test_create_relationship_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        // Initially graph should have 2 nodes, 0 edges
        assert_eq!(store.graph_stats().node_count, 2);
        assert_eq!(store.graph_stats().edge_count, 0);

        // Create relationship
        let end1 = ObjectId::by_guid("Table", &table_guid);
        let end2 = ObjectId::by_guid("Column", &column_guid);
        let rel = Relationship::between("table_columns", end1, end2);

        let rel_guid = store.create_relationship(&rel).await.unwrap();

        // Verify relationship exists in KV
        let retrieved = store.get_relationship(&rel_guid).await.unwrap();
        assert_eq!(retrieved.type_name, "table_columns");

        // Verify graph was updated
        assert_eq!(store.graph_stats().edge_count, 1);
    }

    #[tokio::test]
    async fn test_delete_relationship_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities and relationship
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        let end1 = ObjectId::by_guid("Table", &table_guid);
        let end2 = ObjectId::by_guid("Column", &column_guid);
        let rel = Relationship::between("table_columns", end1, end2);
        let rel_guid = store.create_relationship(&rel).await.unwrap();

        assert_eq!(store.graph_stats().edge_count, 1);

        // Delete relationship
        store.delete_relationship(&rel_guid).await.unwrap();

        // Verify relationship is deleted from KV
        assert!(!store.relationship_exists(&rel_guid).await.unwrap());

        // Verify graph was updated
        assert_eq!(store.graph_stats().edge_count, 0);
    }

    #[tokio::test]
    async fn test_update_relationship_with_wal_preserves_graph_edge() {
        let (store, type_store, _temp) = create_test_store_with_wal().await;
        create_test_types(&type_store).await;

        let source = Entity::new("Table").with_attribute("name", json!("users"));
        let source_guid = store.create_entity(&source).await.unwrap();

        let target = Entity::new("Column").with_attribute("name", json!("id"));
        let target_guid = store.create_entity(&target).await.unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &source_guid),
            ObjectId::by_guid("Column", &target_guid),
        )
        .with_propagate_tags(PropagateTags::None);
        let rel_guid = store.create_relationship(&rel).await.unwrap();

        assert_eq!(store.graph_stats().edge_count, 1);

        let mut updated = store.get_relationship(&rel_guid).await.unwrap();
        updated.label = Some("updated".to_string());
        updated.propagate_tags = Some(PropagateTags::OneToTwo);
        store.update_relationship(&updated).await.unwrap();

        let stored = store.get_relationship(&rel_guid).await.unwrap();
        assert_eq!(stored.label.as_deref(), Some("updated"));
        assert_eq!(stored.propagate_tags, Some(PropagateTags::OneToTwo));
        let lineage = store
            .get_lineage(&source_guid, TraversalDirection::Output, LineageQueryOptions::new())
            .await
            .unwrap();
        assert!(!lineage.edges.is_empty());
        assert_eq!(store.graph_stats().edge_count, 1);
    }

    #[tokio::test]
    async fn test_wal_recovery_projects_committed_entity_to_graph() {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let wal = Arc::new(WriteAheadLog::new(kv.clone()));

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store: Arc<dyn EntityStore> =
            Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store: Arc<dyn RelationshipStore> =
            Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));
        let graph_store = Arc::new(
            GrafeoGraphStore::new_in_memory(entity_store.clone(), relationship_store.clone())
                .unwrap(),
        );
        let store = DefaultMetavisorStore::with_wal(
            type_store.clone(),
            entity_store.clone(),
            relationship_store.clone(),
            graph_store.clone(),
            wal.clone(),
        );

        create_test_types(&type_store).await;

        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let mut tx = Transaction::begin(wal.clone()).await.unwrap();
        let builder = TransactionalEntityBuilder::new(entity.clone());
        tx.log_operation(builder.build_create_op().unwrap()).await.unwrap();
        tx.log_operation(builder.build_graph_node_op()).await.unwrap();

        let guid = entity_store.create_entity(&entity).await.unwrap();
        tx.mark_kv_applied().await.unwrap();
        tx.commit().await.unwrap();

        assert_eq!(graph_store.node_count(), 0);
        let meta_before = wal.get_transaction_meta(tx.id()).await.unwrap().unwrap();
        assert!(meta_before.kv_applied);
        assert!(!meta_before.graph_applied);

        let results = store.tx_manager().unwrap().recover_all().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());

        let meta_after = wal.get_transaction_meta(tx.id()).await.unwrap().unwrap();
        assert!(meta_after.graph_applied);
        assert_eq!(graph_store.node_count(), 1);
        assert!(store.entity_exists(&guid).await.unwrap());
    }

    #[tokio::test]
    async fn test_lineage_operations() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column1 = Entity::new("Column").with_attribute("name", json!("id"));
        let column1_guid = store.create_entity(&column1).await.unwrap();

        let column2 = Entity::new("Column").with_attribute("name", json!("name"));
        let column2_guid = store.create_entity(&column2).await.unwrap();

        // Create relationships: table -> column1, table -> column2
        let rel1 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column1_guid),
        );
        store.create_relationship(&rel1).await.unwrap();

        let rel2 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column2_guid),
        );
        store.create_relationship(&rel2).await.unwrap();

        // Test get_neighbors
        let neighbors = store
            .get_neighbors(&table_guid, TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 2);

        // Test get_lineage
        let lineage = store
            .get_lineage(
                &table_guid,
                TraversalDirection::Output,
                LineageQueryOptions::new().with_depth(3),
            )
            .await
            .unwrap();
        assert_eq!(lineage.node_count(), 3); // table + 2 columns
        assert_eq!(lineage.edge_count(), 2);
    }

    #[tokio::test]
    async fn test_path_exists() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        let other = Entity::new("Column").with_attribute("name", json!("other"));
        let other_guid = store.create_entity(&other).await.unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        );
        store.create_relationship(&rel).await.unwrap();

        assert!(store.path_exists(&table_guid, &column_guid, 1).await.unwrap());
        assert!(!store.path_exists(&table_guid, &other_guid, 1).await.unwrap());
    }

    #[tokio::test]
    async fn test_type_operations() {
        let (store, _type_store) = create_test_store().await;

        // Create type
        let entity_def =
            EntityDef::new("TestType").attribute(AttributeDef::new("name", "string").required());
        store.create_type(&TypeDef::from(entity_def)).await.unwrap();

        // Verify type exists
        assert!(store.type_exists("TestType").await.unwrap());

        // Get type
        let retrieved = store.get_type("TestType").await.unwrap();
        assert!(matches!(retrieved, TypeDef::Entity(_)));

        // List types
        let types = store.list_types().await.unwrap();
        assert!(types.contains(&"TestType".to_string()));

        // Delete type
        store.delete_type("TestType").await.unwrap();
        assert!(!store.type_exists("TestType").await.unwrap());
    }

    #[tokio::test]
    async fn test_add_classifications() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity without classifications
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify no classifications initially
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert!(classifications.is_empty());

        // Add classification
        let classification = Classification::new("PII").with_propagate(true);
        store
            .add_classifications(&guid, std::slice::from_ref(&classification))
            .await
            .unwrap();

        // Verify classification was added
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
        assert_eq!(classifications[0].type_name, "PII");

        // Add duplicate classification (should be ignored)
        store
            .add_classifications(&guid, &[classification])
            .await
            .unwrap();

        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1); // Still only 1
    }

    #[tokio::test]
    async fn test_classification_propagates_one_to_two() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        store.create_relationship(&rel).await.unwrap();

        store
            .add_classifications(&table_guid, &[Classification::new("PII")])
            .await
            .unwrap();

        let classifications = store.get_all_classifications(&column_guid).await.unwrap();
        assert!(classifications.iter().any(|c| c.type_name == "PII"));
    }

    #[tokio::test]
    async fn test_classification_propagates_two_to_one() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::TwoToOne);
        store.create_relationship(&rel).await.unwrap();

        store
            .add_classifications(&column_guid, &[Classification::new("SENSITIVE")])
            .await
            .unwrap();

        let classifications = store.get_all_classifications(&table_guid).await.unwrap();
        assert!(classifications.iter().any(|c| c.type_name == "SENSITIVE"));
    }

    #[tokio::test]
    async fn test_propagation_converges_after_repeated_updates_and_flushes() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table_guid = store
            .create_entity(&Entity::new("Table").with_attribute("name", json!("users")))
            .await
            .unwrap();
        let col1_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("id")))
            .await
            .unwrap();
        let col2_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("email")))
            .await
            .unwrap();

        let rel1 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &col1_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        let rel2 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &col2_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        store.create_relationship(&rel1).await.unwrap();
        store.create_relationship(&rel2).await.unwrap();

        store
            .add_classifications(&table_guid, &[Classification::new("PII")])
            .await
            .unwrap();
        store.flush_classification_propagation().await.unwrap();

        let col1_initial = store.get_all_classifications(&col1_guid).await.unwrap();
        let col2_initial = store.get_all_classifications(&col2_guid).await.unwrap();
        assert!(col1_initial.iter().any(|c| c.type_name == "PII"));
        assert!(col2_initial.iter().any(|c| c.type_name == "PII"));

        store
            .update_classifications(&table_guid, &[Classification::new("CONFIDENTIAL")])
            .await
            .unwrap();
        assert!(store.is_classification_propagation_pending());

        store.flush_classification_propagation().await.unwrap();

        let col1_updated = store.get_all_classifications(&col1_guid).await.unwrap();
        let col2_updated = store.get_all_classifications(&col2_guid).await.unwrap();
        assert!(col1_updated.iter().any(|c| c.type_name == "CONFIDENTIAL"));
        assert!(col2_updated.iter().any(|c| c.type_name == "CONFIDENTIAL"));
        assert!(!col1_updated.iter().any(|c| c.type_name == "PII"));
        assert!(!col2_updated.iter().any(|c| c.type_name == "PII"));
    }

    #[tokio::test]
    async fn test_concurrent_updates_and_flushes_converge() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table_guid = store
            .create_entity(&Entity::new("Table").with_attribute("name", json!("users")))
            .await
            .unwrap();
        let column_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("id")))
            .await
            .unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        store.create_relationship(&rel).await.unwrap();

        let store_updates = Arc::clone(&store);
        let table_for_updates = table_guid.clone();
        let update_task = async move {
            store_updates
                .update_classifications(&table_for_updates, &[Classification::new("FIRST")])
                .await
                .unwrap();
            store_updates
                .update_classifications(&table_for_updates, &[Classification::new("SECOND")])
                .await
                .unwrap();
        };

        let store_flushes = Arc::clone(&store);
        let flush_task = async move {
            store_flushes.flush_classification_propagation().await.unwrap();
            store_flushes.flush_classification_propagation().await.unwrap();
        };

        tokio::join!(update_task, flush_task);

        store.flush_classification_propagation().await.unwrap();

        let final_classes = store.get_all_classifications(&column_guid).await.unwrap();
        assert!(final_classes.iter().any(|c| c.type_name == "SECOND"));
        assert!(!final_classes.iter().any(|c| c.type_name == "FIRST"));
    }

    #[tokio::test]
    async fn test_cache_stays_consistent_after_relationship_delete_and_recreate() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table_guid = store
            .create_entity(&Entity::new("Table").with_attribute("name", json!("users")))
            .await
            .unwrap();
        let column_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("id")))
            .await
            .unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        let rel_guid = store.create_relationship(&rel).await.unwrap();

        store
            .add_classifications(&table_guid, &[Classification::new("PII")])
            .await
            .unwrap();
        store.flush_classification_propagation().await.unwrap();
        assert!(store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));

        store.delete_relationship(&rel_guid).await.unwrap();
        store.flush_classification_propagation().await.unwrap();
        assert!(!store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));

        let rel2 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        store.create_relationship(&rel2).await.unwrap();
        store.flush_classification_propagation().await.unwrap();
        assert!(store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));
    }

    #[tokio::test]
    async fn test_cache_stays_consistent_after_relationship_tag_update() {
        let (store, type_store, _temp) = create_test_store_with_wal().await;
        create_test_types(&type_store).await;

        let table_guid = store
            .create_entity(&Entity::new("Table").with_attribute("name", json!("users")))
            .await
            .unwrap();
        let column_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("id")))
            .await
            .unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::None);
        let rel_guid = store.create_relationship(&rel).await.unwrap();

        store
            .add_classifications(&table_guid, &[Classification::new("PII")])
            .await
            .unwrap();
        store.flush_classification_propagation().await.unwrap();
        assert!(!store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));

        let mut updated = store.get_relationship(&rel_guid).await.unwrap();
        updated.propagate_tags = Some(PropagateTags::OneToTwo);
        store.update_relationship(&updated).await.unwrap();
        store.flush_classification_propagation().await.unwrap();
        assert!(store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));
    }

    #[tokio::test]
    async fn test_cache_stays_consistent_after_source_entity_delete() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let table_guid = store
            .create_entity(
                &Entity::new("Table")
                    .with_attribute("name", json!("users"))
                    .with_classification(Classification::new("PII")),
            )
            .await
            .unwrap();
        let column_guid = store
            .create_entity(&Entity::new("Column").with_attribute("name", json!("id")))
            .await
            .unwrap();

        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column_guid),
        )
        .with_propagate_tags(PropagateTags::OneToTwo);
        store.create_relationship(&rel).await.unwrap();

        store.flush_classification_propagation().await.unwrap();
        assert!(store
            .get_all_classifications(&column_guid)
            .await
            .unwrap()
            .iter()
            .any(|c| c.type_name == "PII"));

        store.delete_entity(&table_guid).await.unwrap();
        store.flush_classification_propagation().await.unwrap();
        let classes_after_delete = store.get_all_classifications(&column_guid).await.unwrap();
        assert!(!classes_after_delete.iter().any(|c| c.type_name == "PII"));
    }

    #[tokio::test]
    async fn test_update_classifications() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Add initial classification
        let classification1 = Classification::new("PII");
        store
            .add_classifications(&guid, &[classification1])
            .await
            .unwrap();

        // Update with different classifications
        let classification2 = Classification::new("SENSITIVE");
        let classification3 = Classification::new("CONFIDENTIAL");
        store
            .update_classifications(&guid, &[classification2, classification3])
            .await
            .unwrap();

        // Verify classifications were replaced
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 2);
        let names: Vec<&str> = classifications
            .iter()
            .map(|c| c.type_name.as_str())
            .collect();
        assert!(names.contains(&"SENSITIVE"));
        assert!(names.contains(&"CONFIDENTIAL"));
        assert!(!names.contains(&"PII")); // Old one should be gone
    }

    #[tokio::test]
    async fn test_remove_classification() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity with classifications
        let classification1 = Classification::new("PII");
        let classification2 = Classification::new("SENSITIVE");
        let entity = Entity::new("Table")
            .with_attribute("name", json!("users"))
            .with_classification(classification1)
            .with_classification(classification2);
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify initial classifications
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 2);

        // Remove one classification
        store.remove_classification(&guid, "PII").await.unwrap();

        // Verify classification was removed
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
        assert_eq!(classifications[0].type_name, "SENSITIVE");

        // Remove non-existent classification (should succeed silently)
        store
            .remove_classification(&guid, "NONEXISTENT")
            .await
            .unwrap();

        // Verify nothing changed
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
    }
}
