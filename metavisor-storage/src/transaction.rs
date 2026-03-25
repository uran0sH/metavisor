//! Transaction manager for WAL-backed KV writes and redo-only graph recovery

use std::sync::Arc;

use crate::error::{Result, StorageError};
use crate::grafeo_graph_store::GrafeoGraphStore;
use crate::wal::{OpType, Transaction, TxMeta, TxState, WriteAheadLog};
use metavisor_core::{
    store::{EntityStore, RelationshipStore},
    GraphStore,
};

/// Recovery action for a transaction
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryAction {
    CommitAndProject,
    ProjectOnly,
    Abort,
    NoAction,
}

/// Result of transaction recovery
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub tx_id: String,
    pub action: RecoveryAction,
    pub operations_recovered: usize,
    pub errors: Vec<String>,
}

impl RecoveryResult {
    fn success(tx_id: String, action: RecoveryAction, ops: usize) -> Self {
        Self {
            tx_id,
            action,
            operations_recovered: ops,
            errors: vec![],
        }
    }

    fn failed(tx_id: String, action: RecoveryAction, errors: Vec<String>) -> Self {
        Self {
            tx_id,
            action,
            operations_recovered: 0,
            errors,
        }
    }

    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Transaction manager for cross-storage operations
pub struct TransactionManager {
    wal: Arc<WriteAheadLog>,
    entity_store: Arc<dyn EntityStore>,
    relationship_store: Arc<dyn RelationshipStore>,
    graph_store: Arc<GrafeoGraphStore>,
}

impl TransactionManager {
    pub fn new(
        wal: Arc<WriteAheadLog>,
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
        graph_store: Arc<GrafeoGraphStore>,
    ) -> Self {
        Self {
            wal,
            entity_store,
            relationship_store,
            graph_store,
        }
    }

    pub async fn begin_transaction(&self) -> Result<Transaction> {
        Transaction::begin(self.wal.clone()).await
    }

    async fn execute_kv_operation(&self, op: &OpType) -> Result<()> {
        match op {
            OpType::CreateEntity {
                serialized_data, ..
            } => {
                let entity: metavisor_core::Entity =
                    serde_json::from_slice(serialized_data).map_err(StorageError::Serialization)?;
                self.entity_store
                    .create_entity(&entity)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            OpType::DeleteEntity { guid } => {
                self.entity_store
                    .delete_entity(guid)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            OpType::UpdateEntity {
                serialized_data, ..
            } => {
                let entity: metavisor_core::Entity =
                    serde_json::from_slice(serialized_data).map_err(StorageError::Serialization)?;
                self.entity_store
                    .update_entity(&entity)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            OpType::UpdateRelationship {
                serialized_data, ..
            } => {
                let rel: metavisor_core::Relationship =
                    serde_json::from_slice(serialized_data).map_err(StorageError::Serialization)?;
                self.relationship_store
                    .update_relationship(&rel)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            OpType::CreateRelationship {
                serialized_data, ..
            } => {
                let rel: metavisor_core::Relationship =
                    serde_json::from_slice(serialized_data).map_err(StorageError::Serialization)?;
                self.relationship_store
                    .create_relationship(&rel)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            OpType::DeleteRelationship { guid } => {
                self.relationship_store
                    .delete_relationship(guid)
                    .await
                    .map_err(|e| StorageError::Kv(e.to_string()))?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn execute_graph_operation(&self, op: &OpType) -> Result<()> {
        match op {
            OpType::AddGraphNode {
                entity_guid,
                entity_type,
            } => {
                GraphStore::add_entity_node(self.graph_store.as_ref(), entity_guid, entity_type)
                    .await
                    .map_err(|e| StorageError::Graph(e.to_string()))?;
            }
            OpType::RemoveGraphNode { entity_guid } => {
                GraphStore::remove_entity_node(self.graph_store.as_ref(), entity_guid)
                    .await
                    .map_err(|e| StorageError::Graph(e.to_string()))?;
            }
            OpType::AddGraphEdge {
                relationship_guid,
                relationship_type,
                from_guid,
                to_guid,
                propagate_tags,
            } => {
                let tags = propagate_tags
                    .as_ref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(metavisor_core::PropagateTags::None);
                GraphStore::add_relationship_edge(
                    self.graph_store.as_ref(),
                    relationship_guid,
                    relationship_type,
                    from_guid,
                    to_guid,
                    tags,
                )
                .await
                .map_err(|e| StorageError::Graph(e.to_string()))?;
            }
            OpType::RemoveGraphEdge { relationship_guid } => {
                GraphStore::remove_relationship_edge(self.graph_store.as_ref(), relationship_guid)
                    .await
                    .map_err(|e| StorageError::Graph(e.to_string()))?;
            }
            OpType::RebuildGraph => {
                GraphStore::rebuild_graph(self.graph_store.as_ref())
                    .await
                    .map_err(|e| StorageError::Graph(e.to_string()))?;
            }
            _ => {}
        }
        Ok(())
    }

    fn determine_recovery_action(&self, meta: &TxMeta) -> RecoveryAction {
        match meta.state {
            TxState::Aborted => RecoveryAction::NoAction,
            TxState::Preparing => {
                if meta.kv_applied {
                    RecoveryAction::CommitAndProject
                } else {
                    RecoveryAction::Abort
                }
            }
            TxState::Committed => {
                if meta.graph_applied {
                    RecoveryAction::NoAction
                } else {
                    RecoveryAction::ProjectOnly
                }
            }
        }
    }

    async fn apply_ops(
        &self,
        tx_id: &str,
        run_kv: bool,
        run_graph: bool,
    ) -> std::result::Result<usize, Vec<String>> {
        let records = match self.wal.get_transaction_ops(tx_id).await {
            Ok(records) => records,
            Err(err) => return Err(vec![format!("Failed to load WAL ops: {err}")]),
        };

        let mut errors = Vec::new();

        for record in &records {
            if run_kv {
                if let Err(err) = self.execute_kv_operation(&record.op).await {
                    errors.push(format!("KV op {} failed: {}", record.seq, err));
                }
            }
            if run_graph {
                if let Err(err) = self.execute_graph_operation(&record.op).await {
                    errors.push(format!("Graph op {} failed: {}", record.seq, err));
                }
            }
        }

        if errors.is_empty() {
            Ok(records.len())
        } else {
            Err(errors)
        }
    }

    async fn recover_transaction(&self, meta: TxMeta) -> RecoveryResult {
        let action = self.determine_recovery_action(&meta);

        match action {
            RecoveryAction::Abort => match self.wal.mark_aborted(&meta.tx_id).await {
                Ok(()) => RecoveryResult::success(meta.tx_id, RecoveryAction::Abort, 0),
                Err(err) => RecoveryResult::failed(
                    meta.tx_id,
                    RecoveryAction::Abort,
                    vec![format!("Failed to mark transaction aborted: {err}")],
                ),
            },
            RecoveryAction::CommitAndProject => match self.apply_ops(&meta.tx_id, false, true).await {
                Ok(ops) => {
                    let mut errors = Vec::new();
                    if let Err(err) = self.wal.mark_committed(&meta.tx_id).await {
                        errors.push(format!("Failed to mark committed: {err}"));
                    }
                    if let Err(err) = self.wal.mark_graph_applied(&meta.tx_id).await {
                        errors.push(format!("Failed to mark graph applied: {err}"));
                    }
                    if errors.is_empty() {
                        RecoveryResult::success(meta.tx_id, RecoveryAction::CommitAndProject, ops)
                    } else {
                        RecoveryResult::failed(meta.tx_id, RecoveryAction::CommitAndProject, errors)
                    }
                }
                Err(errors) => RecoveryResult::failed(meta.tx_id, RecoveryAction::CommitAndProject, errors),
            },
            RecoveryAction::ProjectOnly => match self.apply_ops(&meta.tx_id, false, true).await {
                Ok(ops) => match self.wal.mark_graph_applied(&meta.tx_id).await {
                    Ok(()) => RecoveryResult::success(meta.tx_id, RecoveryAction::ProjectOnly, ops),
                    Err(err) => RecoveryResult::failed(
                        meta.tx_id,
                        RecoveryAction::ProjectOnly,
                        vec![format!("Failed to mark graph applied: {err}")],
                    ),
                },
                Err(errors) => RecoveryResult::failed(meta.tx_id, RecoveryAction::ProjectOnly, errors),
            },
            RecoveryAction::NoAction => {
                RecoveryResult::success(meta.tx_id, RecoveryAction::NoAction, 0)
            }
        }
    }

    pub async fn recover_all(&self) -> Vec<RecoveryResult> {
        let pending = match self.wal.list_incomplete_transactions().await {
            Ok(metas) => metas,
            Err(err) => {
                tracing::error!("Failed to list incomplete transactions: {}", err);
                return vec![];
            }
        };

        if pending.is_empty() {
            tracing::info!("No incomplete transactions to recover");
            return vec![];
        }

        let mut results = Vec::with_capacity(pending.len());
        for meta in pending {
            let result = self.recover_transaction(meta).await;
            if result.is_success() {
                tracing::info!("Recovered transaction {}", result.tx_id);
            } else {
                tracing::error!("Failed to recover transaction {}: {:?}", result.tx_id, result.errors);
            }
            results.push(result);
        }
        results
    }

    pub async fn cleanup_old_transactions(&self, older_than_hours: u64) -> Result<usize> {
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            - older_than_hours * 3600;

        self.wal.cleanup_old_transactions(cutoff).await
    }

    pub fn spawn_cleanup_task(
        self: &Arc<Self>,
        interval_hours: Option<u64>,
        retention_hours: Option<u64>,
    ) {
        let interval = interval_hours.unwrap_or(24);
        let retention = retention_hours.unwrap_or(168);
        let manager = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(interval * 3600));
            loop {
                ticker.tick().await;
                match manager.cleanup_old_transactions(retention).await {
                    Ok(cleaned) => {
                        if cleaned > 0 {
                            tracing::info!("WAL cleanup: removed {} old records", cleaned);
                        }
                    }
                    Err(err) => tracing::error!("WAL cleanup failed: {}", err),
                }
            }
        });
    }
}

/// Builder for creating entity operations with WAL logging
pub struct TransactionalEntityBuilder {
    entity: metavisor_core::Entity,
}

impl TransactionalEntityBuilder {
    pub fn new(entity: metavisor_core::Entity) -> Self {
        Self { entity }
    }

    pub fn build_create_op(&self) -> Result<OpType> {
        let serialized = serde_json::to_vec(&self.entity).map_err(StorageError::Serialization)?;
        Ok(OpType::CreateEntity {
            guid: self.entity.guid.clone().unwrap_or_default(),
            entity_type: self.entity.type_name.clone(),
            serialized_data: serialized,
        })
    }

    pub fn build_update_op(&self) -> Result<OpType> {
        let serialized = serde_json::to_vec(&self.entity).map_err(StorageError::Serialization)?;
        Ok(OpType::UpdateEntity {
            guid: self.entity.guid.clone().unwrap_or_default(),
            serialized_data: serialized,
        })
    }

    pub fn build_graph_node_op(&self) -> OpType {
        OpType::AddGraphNode {
            entity_guid: self.entity.guid.clone().unwrap_or_default(),
            entity_type: self.entity.type_name.clone(),
        }
    }
}

/// Builder for creating relationship operations with WAL logging
pub struct TransactionalRelationshipBuilder {
    relationship: metavisor_core::Relationship,
}

impl TransactionalRelationshipBuilder {
    pub fn new(relationship: metavisor_core::Relationship) -> Self {
        Self { relationship }
    }

    pub fn build_create_op(&self) -> Result<OpType> {
        let serialized =
            serde_json::to_vec(&self.relationship).map_err(StorageError::Serialization)?;

        Ok(OpType::CreateRelationship {
            guid: self.relationship.guid.clone().unwrap_or_default(),
            serialized_data: serialized,
        })
    }

    pub fn build_graph_edge_op(&self) -> Result<OpType> {
        let propagate_tags = self
            .relationship
            .propagate_tags
            .map(|tags| serde_json::to_string(&tags))
            .transpose()
            .map_err(StorageError::Serialization)?;

        Ok(OpType::AddGraphEdge {
            relationship_guid: self.relationship.guid.clone().unwrap_or_default(),
            relationship_type: self.relationship.type_name.clone(),
            from_guid: self
                .relationship
                .end1
                .as_ref()
                .and_then(|e| e.guid.clone())
                .unwrap_or_default(),
            to_guid: self
                .relationship
                .end2
                .as_ref()
                .and_then(|e| e.guid.clone())
                .unwrap_or_default(),
            propagate_tags,
        })
    }

    pub fn build_update_op(&self) -> Result<OpType> {
        let serialized =
            serde_json::to_vec(&self.relationship).map_err(StorageError::Serialization)?;

        Ok(OpType::UpdateRelationship {
            guid: self.relationship.guid.clone().unwrap_or_default(),
            serialized_data: serialized,
        })
    }
}

/// Statistics about transaction recovery
#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub total_transactions: usize,
    pub successful_recoveries: usize,
    pub failed_recoveries: usize,
    pub commit_and_projects: usize,
    pub project_only: usize,
    pub aborted: usize,
}

impl RecoveryStats {
    pub fn from_results(results: &[RecoveryResult]) -> Self {
        let mut stats = Self {
            total_transactions: results.len(),
            ..Default::default()
        };

        for result in results {
            if result.is_success() {
                stats.successful_recoveries += 1;
            } else {
                stats.failed_recoveries += 1;
            }

            match result.action {
                RecoveryAction::CommitAndProject => stats.commit_and_projects += 1,
                RecoveryAction::ProjectOnly => stats.project_only += 1,
                RecoveryAction::Abort => stats.aborted += 1,
                RecoveryAction::NoAction => {}
            }
        }

        stats
    }
}

/// Result of store initialization with recovery
#[derive(Debug, Clone)]
pub struct InitializationResult {
    pub wal_recovery: Vec<RecoveryResult>,
    pub consistency_report: crate::ConsistencyReport,
    pub repair_result: crate::RepairResult,
}

impl InitializationResult {
    pub fn had_changes(&self) -> bool {
        self.wal_recovery
            .iter()
            .any(|r| r.operations_recovered > 0 || r.action == RecoveryAction::Abort)
            || self.repair_result.total_repaired() > 0
    }

    pub fn total_changes(&self) -> usize {
        let wal_changes: usize = self.wal_recovery.iter().map(|r| r.operations_recovered).sum();
        wal_changes + self.repair_result.total_repaired()
    }

    pub fn wal_recovery_success(&self) -> bool {
        self.wal_recovery.iter().all(|r| r.is_success())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_store::KvEntityStore;
    use crate::kv::KvStore;
    use crate::relationship_store::KvRelationshipStore;
    use crate::type_store::KvTypeStore;

    async fn create_test_stores() -> (
        Arc<WriteAheadLog>,
        Arc<dyn metavisor_core::store::EntityStore>,
        Arc<dyn metavisor_core::store::RelationshipStore>,
        Arc<GrafeoGraphStore>,
        tempfile::TempDir,
    ) {
        let tempdir = tempfile::TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let wal = Arc::new(WriteAheadLog::new(kv.clone()));

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store: Arc<dyn metavisor_core::store::EntityStore> =
            Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let rel_store: Arc<dyn metavisor_core::store::RelationshipStore> =
            Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));
        let graph_store = Arc::new(
            GrafeoGraphStore::new_in_memory(entity_store.clone(), rel_store.clone()).unwrap(),
        );

        (wal, entity_store, rel_store, graph_store, tempdir)
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let (wal, entity_store, rel_store, graph_store, _temp) = create_test_stores().await;
        let manager = TransactionManager::new(wal, entity_store, rel_store, graph_store);
        let tx = manager.begin_transaction().await.unwrap();
        assert!(tx.is_active());
        assert!(!tx.id().is_empty());
    }

    #[tokio::test]
    async fn test_builders() {
        let entity = metavisor_core::Entity::new("Table");
        let entity_builder = TransactionalEntityBuilder::new(entity);
        assert!(matches!(
            entity_builder.build_create_op().unwrap(),
            OpType::CreateEntity { .. }
        ));

        let relationship = metavisor_core::Relationship::new("depends_on");
        let relationship_builder = TransactionalRelationshipBuilder::new(relationship);
        assert!(matches!(
            relationship_builder.build_create_op().unwrap(),
            OpType::CreateRelationship { .. }
        ));
    }
}
