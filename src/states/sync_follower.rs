use std::{collections::HashSet, fmt::Display};

use crate::{
    file_transfer::TransferFiles,
    ignored_files::IgnoredFilesCache,
    network_events::{NetworkEvents, StorageIndexStatus, Synchronization, Transition},
    node_id::NodeId,
    state_machine::State,
    storage::FileInfo,
    SharedState,
};

#[derive(Debug)]
pub struct SyncFollower {
    sync_leader: NodeId,
}

impl Display for SyncFollower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncFollower")
    }
}

impl SyncFollower {
    pub fn new(sync_leader: NodeId) -> Self {
        Self { sync_leader }
    }
}

impl State for SyncFollower {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        log::info!("full sync starting....");

        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut files_to_send = Vec::default();
        while let Some((peer_id, ev)) = shared_state.connection_handler.next_event().await {
            if peer_id != self.sync_leader {
                log::trace!("Received event from non leader peer");
                continue;
            }

            match execute_event(
                shared_state,
                ev,
                self.sync_leader,
                &mut ignored_files_cache,
                &mut files_to_send,
            )
            .await
            {
                Ok(should_continue) => {
                    if !should_continue {
                        break;
                    }
                }
                Err(err) => {
                    log::error!("Error executing event {err}");
                }
            }
        }

        shared_state.transaction_log.flush().await?;

        log::info!("full sync end....");

        Ok(())
    }
}

async fn execute_event(
    shared_state: &SharedState,
    ev: NetworkEvents,
    sync_leader: NodeId,

    ignored_files_cache: &mut IgnoredFilesCache,
    files_to_send: &mut Vec<(FileInfo, HashSet<NodeId>)>,
) -> crate::Result<bool> {
    match ev {
        NetworkEvents::Synchronization(Synchronization::QueryStorageIndex { name, hash }) => {
            log::debug!("Queried about {name} storage");
            let storage_index = match shared_state.config.storages.get(&name) {
                Some(storage_config) => {
                    match crate::storage::get_storage_info(
                        &name,
                        storage_config,
                        shared_state.transaction_log,
                    )
                    .await
                    {
                        Ok(storage) => {
                            if storage.hash != hash {
                                StorageIndexStatus::SyncNecessary(storage.files)
                            } else {
                                StorageIndexStatus::StorageInSync
                            }
                        }
                        Err(err) => {
                            log::error!("There was an error reading the storage: {err}");
                            StorageIndexStatus::StorageMissing
                        }
                    }
                }
                None => StorageIndexStatus::StorageMissing,
            };

            shared_state
                .connection_handler
                .send_to(
                    Synchronization::ReplyStorageIndex {
                        name,
                        storage_index,
                    }
                    .into(),
                    sync_leader,
                )
                .await?;
        }
        NetworkEvents::Synchronization(Synchronization::SendFileTo { file, nodes }) => {
            files_to_send.push((file, nodes))
        }
        NetworkEvents::Synchronization(Synchronization::DeleteFile { file }) => {
            crate::storage::file_operations::delete_file(
                shared_state.config,
                shared_state.transaction_log,
                &file,
                ignored_files_cache,
            )
            .await?;
        }
        NetworkEvents::Synchronization(Synchronization::MoveFile { file }) => {
            crate::storage::file_operations::move_file(
                shared_state.config,
                shared_state.transaction_log,
                &file,
                ignored_files_cache,
            )
            .await?;
        }
        NetworkEvents::Synchronization(Synchronization::StartTransferingFiles) => {
            TransferFiles::new(Some(sync_leader), std::mem::take(files_to_send))
                .execute(shared_state)
                .await?;
        }
        NetworkEvents::RequestTransition(Transition::Done) => return Ok(false),
        _ => {}
    }

    Ok(true)
}
