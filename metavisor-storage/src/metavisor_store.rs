//! MetavisorStore implementation - Unified abstraction layer.
//!
//! KV is the source of truth for metadata. The graph store is a derived
//! projection that is rebuilt or repaired from KV data as needed.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use metavisor_core::{
    Classification, Entity, EntityHeader, GraphStats, GraphStore, MetavisorStore, ObjectId,
    PropagateTags, Relationship, RelationshipHeader, Result, TypeDef, TypeStore,
};

/// Startup initialization summary.
#[derive(Debug, Clone)]
pub struct InitializationResult {
    pub consistency_report: crate::ConsistencyReport,
    pub repair_result: crate::RepairResult,
}

impl InitializationResult {
    pub fn had_changes(&self) -> bool {
        self.repair_result.total_repaired() > 0
    }

    pub fn total_changes(&self) -> usize {
        self.repair_result.total_repaired()
    }
}

struct MaintenanceTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// DefaultMetavisorStore - Default implementation of MetavisorStore.
///
/// Main data is stored in KV-backed stores. Graph updates happen as a
/// best-effort projection and are repaired from KV on startup.
pub struct DefaultMetavisorStore {
    type_store: Arc<dyn TypeStore>,
    entity_store: Arc<dyn metavisor_core::store::EntityStore>,
    relationship_store: Arc<dyn metavisor_core::store::RelationshipStore>,
    graph_store: Arc<dyn GraphStore>,
    /// Blocks writes while startup rebuild/repair is running.
    write_barrier: Arc<RwLock<()>>,
    /// Number of known graph projection failures awaiting repair.
    pending_projection_repairs: Arc<AtomicUsize>,
    /// Background repair task, if started.
    maintenance_task: Mutex<Option<MaintenanceTask>>,
}

impl DefaultMetavisorStore {
    pub fn new(
        type_store: Arc<dyn TypeStore>,
        entity_store: Arc<dyn metavisor_core::store::EntityStore>,
        relationship_store: Arc<dyn metavisor_core::store::RelationshipStore>,
        graph_store: Arc<dyn GraphStore>,
    ) -> Self {
        Self {
            type_store,
            entity_store,
            relationship_store,
            graph_store,
            write_barrier: Arc::new(RwLock::new(())),
            pending_projection_repairs: Arc::new(AtomicUsize::new(0)),
            maintenance_task: Mutex::new(None),
        }
    }

    /// Rebuild the graph projection from persisted KV data.
    pub async fn initialize(&self) -> Result<()> {
        self.graph_store.rebuild_graph().await
    }

    /// Rebuild and repair the graph projection from KV data on startup.
    pub async fn initialize_with_recovery(&self) -> Result<InitializationResult> {
        let _recovery_guard = self.write_barrier.write().await;

        self.graph_store.rebuild_graph().await?;
        let (consistency_report, repair_result) = self.repair_consistency().await?;
        self.pending_projection_repairs
            .store(repair_result.total_failed(), Ordering::Relaxed);

        Ok(InitializationResult {
            consistency_report,
            repair_result,
        })
    }

    /// Spawn background maintenance tasks.
    ///
    /// # Arguments
    /// * `repair_interval_minutes` - Interval in minutes between repair runs. Defaults to 5 minutes.
    pub fn spawn_maintenance_tasks(&self, repair_interval_minutes: Option<u64>) {
        let mut maintenance_task = self
            .maintenance_task
            .lock()
            .expect("maintenance task mutex poisoned");
        if maintenance_task.is_some() {
            tracing::debug!("Projection repair maintenance task already running");
            return;
        }

        let interval_secs = repair_interval_minutes.unwrap_or(5) * 60;
        let write_barrier = Arc::clone(&self.write_barrier);
        let pending_projection_repairs = Arc::clone(&self.pending_projection_repairs);
        let entity_store = Arc::clone(&self.entity_store);
        let relationship_store = Arc::clone(&self.relationship_store);
        let graph_store = Arc::clone(&self.graph_store);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = cancel_task.cancelled() => {
                        tracing::info!("Projection repair maintenance task stopped");
                        break;
                    }
                    _ = ticker.tick() => {}
                }

                if pending_projection_repairs.load(Ordering::Relaxed) == 0 {
                    continue;
                }

                let _repair_guard = write_barrier.write().await;
                match crate::ConsistencyChecker::check_and_repair(
                    entity_store.as_ref(),
                    relationship_store.as_ref(),
                    graph_store.as_ref(),
                )
                .await
                {
                    Ok((_report, repair_result)) => {
                        pending_projection_repairs
                            .store(repair_result.total_failed(), Ordering::Relaxed);
                        tracing::info!(
                            "Projection repair run completed: repaired={}, failed={}",
                            repair_result.total_repaired(),
                            repair_result.total_failed()
                        );
                    }
                    Err(err) => {
                        tracing::error!("Projection repair run failed: {}", err);
                    }
                }
            }
        });

        *maintenance_task = Some(MaintenanceTask { cancel, handle });
    }

    pub async fn shutdown_maintenance_tasks(&self) {
        let task = {
            let mut maintenance_task = self
                .maintenance_task
                .lock()
                .expect("maintenance task mutex poisoned");
            maintenance_task.take()
        };

        if let Some(task) = task {
            task.cancel.cancel();
            if let Err(err) = task.handle.await {
                tracing::warn!("Projection repair maintenance task join failed: {}", err);
            }
        }
    }

    pub async fn check_consistency(&self) -> Result<crate::ConsistencyReport> {
        crate::ConsistencyChecker::check_consistency(
            self.entity_store.as_ref(),
            self.relationship_store.as_ref(),
            self.graph_store.as_ref(),
        )
        .await
    }

    pub async fn repair_consistency(
        &self,
    ) -> Result<(crate::ConsistencyReport, crate::RepairResult)> {
        let result = crate::ConsistencyChecker::check_and_repair(
            self.entity_store.as_ref(),
            self.relationship_store.as_ref(),
            self.graph_store.as_ref(),
        )
        .await?;
        self.pending_projection_repairs
            .store(result.1.total_failed(), Ordering::Relaxed);
        Ok(result)
    }

    pub fn graph_store(&self) -> &Arc<dyn GraphStore> {
        &self.graph_store
    }

    pub fn pending_projection_repairs(&self) -> usize {
        self.pending_projection_repairs.load(Ordering::Relaxed)
    }

    fn record_projection_failure(
        &self,
        operation: &'static str,
        subject_type: &'static str,
        subject_id: &str,
        error: &metavisor_core::CoreError,
    ) {
        let queued = self
            .pending_projection_repairs
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        tracing::error!(
            operation,
            subject_type,
            subject_id,
            queued_projection_repairs = queued,
            error = %error,
            "Graph projection failed; queued for later repair"
        );
    }

    pub async fn bulk_create_entities(&self, entities: &[Entity]) -> Result<Vec<String>> {
        let _write_guard = self.write_barrier.read().await;
        let mut guids = Vec::with_capacity(entities.len());

        for entity in entities {
            let guid = self.entity_store.create_entity(entity).await?;
            guids.push(guid);
        }

        for (entity, guid) in entities.iter().zip(&guids) {
            if let Err(err) = self
                .graph_store
                .add_entity_node(guid, &entity.type_name)
                .await
            {
                self.record_projection_failure("bulk_create", "entity", guid, &err);
            }
        }

        Ok(guids)
    }

    async fn update_entity_classifications(&self, entity: &Entity) -> Result<()> {
        self.entity_store.update_entity(entity).await?;

        let classification_names: Vec<String> = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        let guid = entity.guid.as_deref().unwrap_or("");
        if let Err(err) = self
            .graph_store
            .update_entity_node(guid, &entity.type_name, None, classification_names)
            .await
        {
            self.record_projection_failure("update_classifications", "entity", guid, &err);
        }

        Ok(())
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
}

#[async_trait]
impl MetavisorStore for DefaultMetavisorStore {
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

    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        let _write_guard = self.write_barrier.read().await;
        let guid = self.entity_store.create_entity(entity).await?;

        if let Err(err) = self
            .graph_store
            .add_entity_node(&guid, &entity.type_name)
            .await
        {
            self.record_projection_failure("create", "entity", &guid, &err);
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
        let _write_guard = self.write_barrier.read().await;
        self.entity_store.update_entity(entity).await?;

        let display_name = entity.attributes.get("name").and_then(|v| v.as_str());
        let classifications = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        let guid = entity.guid.as_deref().unwrap_or("");
        let updated = match self
            .graph_store
            .update_entity_node(guid, &entity.type_name, display_name, classifications)
            .await
        {
            Ok(updated) => updated,
            Err(err) => {
                self.record_projection_failure("update", "entity", guid, &err);
                return Ok(());
            }
        };

        if !updated {
            if let Some(ref entity_guid) = entity.guid {
                if let Err(err) = self
                    .graph_store
                    .add_entity_node(entity_guid, &entity.type_name)
                    .await
                {
                    self.record_projection_failure("update", "entity", entity_guid, &err);
                }
            }
        }

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        let _write_guard = self.write_barrier.read().await;
        let entity = self.entity_store.get_entity(guid).await?;

        let graph_err = self.graph_store.remove_entity_node(guid).await.err();
        self.entity_store.delete_entity(guid).await?;

        if let Some(err) = graph_err {
            self.record_projection_failure("delete", "entity", guid, &err);
            tracing::info!(
                entity_type = %entity.type_name,
                subject_id = guid,
                "Entity deleted from KV; graph projection will be repaired later"
            );
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

    async fn create_relationship(&self, relationship: &Relationship) -> Result<String> {
        let _write_guard = self.write_barrier.read().await;
        let mut resolved_relationship = relationship.clone();
        if let Some(end1) = resolved_relationship.end1.take() {
            resolved_relationship.end1 = Some(self.resolve_object_id(end1).await?);
        }
        if let Some(end2) = resolved_relationship.end2.take() {
            resolved_relationship.end2 = Some(self.resolve_object_id(end2).await?);
        }

        let guid = self
            .relationship_store
            .create_relationship(&resolved_relationship)
            .await?;

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

        if let Err(err) = self
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
            self.record_projection_failure("create", "relationship", &guid, &err);
        }

        Ok(guid)
    }

    async fn get_relationship(&self, guid: &str) -> Result<Relationship> {
        self.relationship_store.get_relationship(guid).await
    }

    async fn update_relationship(&self, relationship: &Relationship) -> Result<()> {
        let _write_guard = self.write_barrier.read().await;
        self.relationship_store
            .update_relationship(relationship)
            .await?;
        if let Err(err) = self.graph_store.rebuild_graph().await {
            let guid = relationship.guid.as_deref().unwrap_or("");
            self.record_projection_failure("update", "relationship", guid, &err);
        }
        Ok(())
    }

    async fn delete_relationship(&self, guid: &str) -> Result<()> {
        let _write_guard = self.write_barrier.read().await;
        let rel = self.relationship_store.get_relationship(guid).await?;

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

        let graph_err = self.graph_store.remove_relationship_edge(guid).await.err();
        self.relationship_store.delete_relationship(guid).await?;

        if let Some(err) = graph_err {
            self.record_projection_failure("delete", "relationship", guid, &err);
            tracing::info!(
                relationship_type = %rel.type_name,
                end1_guid = %end1_guid,
                end2_guid = %end2_guid,
                propagate_tags = ?propagate_tags,
                subject_id = guid,
                "Relationship deleted from KV; graph projection will be repaired later"
            );
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

    async fn add_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()> {
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        for classification in classifications {
            if !entity
                .classifications
                .iter()
                .any(|c| c.type_name == classification.type_name)
            {
                entity.classifications.push(classification.clone());
            }
        }

        self.update_entity_classifications(&entity).await
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
        let mut entity = self.entity_store.get_entity(entity_guid).await?;
        entity.classifications = classifications.to_vec();
        self.update_entity_classifications(&entity).await
    }

    async fn remove_classification(
        &self,
        entity_guid: &str,
        classification_name: &str,
    ) -> Result<()> {
        let mut entity = self.entity_store.get_entity(entity_guid).await?;
        entity
            .classifications
            .retain(|c| c.type_name != classification_name);
        self.update_entity_classifications(&entity).await
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
    use metavisor_core::{AttributeDef, EntityDef, RelationshipDef, RelationshipEndDef};
    use serde_json::json;

    use crate::entity_store::KvEntityStore;
    use crate::grafeo_graph_store::GrafeoGraphStore;
    use crate::kv::KvStore;
    use crate::relationship_store::KvRelationshipStore;
    use crate::type_store::KvTypeStore;

    async fn create_test_store() -> (Arc<DefaultMetavisorStore>, Arc<KvTypeStore>) {
        let tempdir = tempfile::TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store = Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));
        let graph_store: Arc<dyn GraphStore> = Arc::new(
            GrafeoGraphStore::new_in_memory(entity_store.clone(), relationship_store.clone())
                .unwrap(),
        );

        (
            Arc::new(DefaultMetavisorStore::new(
                type_store.clone(),
                entity_store,
                relationship_store,
                graph_store,
            )),
            type_store,
        )
    }

    async fn create_test_types(type_store: &KvTypeStore) {
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

        let rel_def = RelationshipDef::new("table_columns")
            .category(metavisor_core::RelationshipCategory::Composition)
            .propagate_tags(PropagateTags::OneToTwo)
            .end1(RelationshipEndDef::new("Table", "columns"))
            .end2(RelationshipEndDef::new("Column", "table"));

        type_store
            .create_type(&TypeDef::from(rel_def))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_entity_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "Table");
        assert_eq!(store.graph_stats().node_count, 1);
    }

    #[tokio::test]
    async fn test_bulk_create_entities_projects_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let entities: Vec<Entity> = (0..3)
            .map(|i| Entity::new("Table").with_attribute("name", json!(format!("table_{}", i))))
            .collect();

        let guids = store.bulk_create_entities(&entities).await.unwrap();
        assert_eq!(guids.len(), 3);
        assert_eq!(store.graph_stats().node_count, 3);
    }

    #[tokio::test]
    async fn test_initialize_with_recovery_repairs_graph_from_kv() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.entity_store.create_entity(&entity).await.unwrap();
        assert_eq!(store.graph_stats().node_count, 0);

        let result = store.initialize_with_recovery().await.unwrap();

        assert!(result.consistency_report.is_consistent());
        assert!(store.entity_exists(&guid).await.unwrap());
        assert_eq!(store.graph_stats().node_count, 1);
    }

    #[tokio::test]
    async fn test_create_relationship_syncs_to_graph() {
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
        );

        let rel_guid = store.create_relationship(&rel).await.unwrap();
        let retrieved = store.get_relationship(&rel_guid).await.unwrap();
        assert_eq!(retrieved.type_name, "table_columns");
        assert_eq!(store.graph_stats().edge_count, 1);
    }

    #[tokio::test]
    async fn test_maintenance_task_can_start_once_and_shutdown() {
        let (store, _type_store) = create_test_store().await;

        store.spawn_maintenance_tasks(Some(1));
        store.spawn_maintenance_tasks(Some(1));

        store.shutdown_maintenance_tasks().await;
        store.shutdown_maintenance_tasks().await;
    }
}
