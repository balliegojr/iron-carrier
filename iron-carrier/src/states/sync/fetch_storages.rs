use std::collections::HashMap;

use crate::{
    config::PathConfig,
    node_id::NodeId,
    state_machine::{Result, State, StateMachineError},
    storage::{self, Storage},
    Context,
};

use super::events::{QueryStorageIndex, StorageIndex, StorageIndexStatus};

#[derive(Debug)]
pub struct FetchStorages {
    storage_name: &'static str,
    storage_config: &'static PathConfig,
}
impl FetchStorages {
    pub fn new(storage_name: &'static str, storage_config: &'static PathConfig) -> Self {
        Self {
            storage_name,
            storage_config,
        }
    }

    async fn get_storage_from_nodes(
        &self,
        context: &Context,
        storage: &Storage,
    ) -> Result<HashMap<NodeId, Storage>> {
        let peer_storages = context
            .rpc
            .broadcast(QueryStorageIndex {
                name: self.storage_name.to_string(),
                hash: storage.hash,
            })
            .result()
            .await?
            .replies();

        Ok(peer_storages
            .iter()
            .filter_map(|reply| {
                let node = reply.node_id();
                let node_storage = reply.data::<StorageIndex>().ok()?;

                if node_storage.name != self.storage_name {
                    return None;
                }

                match node_storage.storage_index {
                    StorageIndexStatus::StorageMissing => {
                        // peer doesn't have the storage...
                        None
                    }
                    StorageIndexStatus::StorageInSync => Some((node, storage.clone())),
                    StorageIndexStatus::SyncNecessary(storage) => Some((node, storage)),
                }
            })
            .collect())
    }
}

impl State for FetchStorages {
    type Output = HashMap<NodeId, Storage>;

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        let storage = storage::get_storage_info(
            self.storage_name,
            self.storage_config,
            &context.transaction_log,
        )
        .await?;

        let mut peers_storages = self.get_storage_from_nodes(context, &storage).await?;
        if peers_storages.is_empty() {
            log::trace!("Storage already in sync with all peers");
            Err(StateMachineError::Abort)?
        }

        peers_storages.insert(context.config.node_id_hashed, storage);

        let peers: Vec<NodeId> = peers_storages.keys().copied().collect();
        log::trace!(
            "Storage {0} to be synchronized with {peers:?}",
            self.storage_name
        );

        Ok(peers_storages)
    }
}
