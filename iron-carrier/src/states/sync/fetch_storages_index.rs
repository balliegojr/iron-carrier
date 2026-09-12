use std::collections::{HashMap, HashSet};

use crate::{
    Context,
    node_id::NodeId,
    state_machine::{Result, State, StateMachineError},
    storage::Storage,
    transaction_log::SyncStatus,
};

use super::events::{QueryStorageIndex, StorageIndexStatus};

/// Fetch storage information from all nodes in the sync session. Only nodes that need to sync will
/// reply with their storage info.
#[derive(Debug)]
pub struct FetchStorageIndex {
    storage_name: String,
    nodes: HashSet<NodeId>,
}
impl FetchStorageIndex {
    pub fn new(storage_name: String, nodes: HashSet<NodeId>) -> Self {
        Self {
            storage_name,
            nodes,
        }
    }

    async fn get_storage_from_nodes(&self, context: &Context) -> Result<HashMap<NodeId, Storage>> {
        let peer_storages = context
            .rpc
            .multi_call(
                QueryStorageIndex {
                    name: self.storage_name.to_string(),
                },
                self.nodes.clone(),
            )
            .result()
            .await?
            .replies();

        Ok(peer_storages
            .iter()
            .filter_map(|reply| {
                let node = reply.node_id();
                let node_storage = reply.data().ok()?;

                if node_storage.name != self.storage_name {
                    return None;
                }

                match node_storage.storage_index {
                    StorageIndexStatus::StorageMissing => None,
                    StorageIndexStatus::SyncNecessary(storage) => Some((node, storage)),
                }
            })
            .collect())
    }
}

impl State for FetchStorageIndex {
    type Output = HashMap<NodeId, Storage>;

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        let storages = self.get_storage_from_nodes(context).await?;
        if storages.is_empty() || storages.len() == 1 {
            log::trace!("Not enough peers to sync storage {}", self.storage_name);
            Err(StateMachineError::Abort)?
        }

        let storage_hash = storages.values().next().unwrap().hash;
        if storages.values().all(|s| s.hash == storage_hash) {
            log::trace!(
                "Storage already in sync {} for all peers",
                self.storage_name
            );
            Err(StateMachineError::Abort)?
        }

        for node in storages.keys() {
            let _ = context
                .transaction_log
                .save_sync_status(
                    node.to_string().as_str(),
                    &self.storage_name,
                    SyncStatus::Started,
                )
                .await;
        }

        let peers: Vec<NodeId> = storages.keys().copied().collect();
        log::trace!(
            "Storage {0} to be synchronized with {peers:?}",
            self.storage_name
        );

        Ok(storages)
    }
}
