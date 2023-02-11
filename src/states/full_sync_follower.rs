use std::{collections::HashSet, fmt::Display};

use crate::{
    file_transfer::TransferFiles,
    network_events::{NetworkEvents, Synchronization, Transition},
    state_machine::Step,
    SharedState,
};

#[derive(Debug)]
pub struct FullSyncFollower {
    sync_leader: u64,
}

impl Display for FullSyncFollower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncFollower")
    }
}

impl FullSyncFollower {
    pub fn new(sync_leader: u64) -> Self {
        Self { sync_leader }
    }
}

impl Step for FullSyncFollower {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        // TODO: check for ignored files before delete/moving/receiving
        log::info!("full sync starting....");

        let mut files_to_send = Vec::default();
        while let Some((peer_id, ev)) = shared_state.connection_handler.next_event().await {
            if peer_id != self.sync_leader {
                log::trace!("Received event from non leader peer");
                continue;
            }

            match ev {
                NetworkEvents::Synchronization(Synchronization::QueryStorageIndex {
                    name,
                    hash,
                }) => {
                    log::debug!("Queried about {name} storage");
                    let files = match shared_state.config.storages.get(&name) {
                        Some(storage_config) => {
                            match crate::storage::get_storage(
                                &name,
                                storage_config,
                                shared_state.transaction_log,
                            )
                            .await
                            {
                                Ok(storage) if storage.hash != hash => Some(storage.files),
                                Err(err) => {
                                    log::error!("There was an error reading the storage: {err}");
                                    None
                                }
                                _ => None,
                            }
                        }
                        None => None,
                    };

                    shared_state
                        .connection_handler
                        .send_to(
                            Synchronization::ReplyStorageIndex { name, files }.into(),
                            self.sync_leader,
                        )
                        .await?;
                }
                NetworkEvents::Synchronization(Synchronization::SendFileTo { file, nodes }) => {
                    files_to_send.push((file, nodes));
                }
                NetworkEvents::Synchronization(Synchronization::DeleteFile { file }) => {
                    crate::storage::delete_file(
                        shared_state.config,
                        shared_state.transaction_log,
                        &file,
                    )
                    .await?;
                }
                NetworkEvents::Synchronization(Synchronization::MoveFile { file }) => {
                    crate::storage::move_file(
                        shared_state.config,
                        shared_state.transaction_log,
                        &file,
                    )
                    .await?;
                }
                NetworkEvents::Synchronization(Synchronization::StartTransferingFiles) => {
                    TransferFiles::new(
                        std::mem::take(&mut files_to_send),
                        HashSet::from([self.sync_leader]),
                    )
                    .execute(shared_state)
                    .await?;
                }
                NetworkEvents::RequestTransition(Transition::Done) => break,
                _ => continue,
            }
        }

        shared_state.transaction_log.flush().await?;

        log::info!("full sync end....");

        Ok(())
    }
}
