use std::fmt::Display;

use tokio_stream::StreamExt;

use crate::{
    file_transfer::process_transfer_events,
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
        // TODO: check for ignred files before delete/moving/receiving
        log::info!("full sync starting....");

        let mut event_stream =
            (shared_state.connection_handler.events_stream().await).filter_map(|(peer_id, ev)| {
                if peer_id == self.sync_leader {
                    Some(ev)
                } else {
                    None
                }
            });

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let event_processing_handler = tokio::spawn(process_transfer_events(*shared_state, rx));

        while let Some(ev) = event_stream.next().await {
            match ev {
                NetworkEvents::Synchronization(Synchronization::QueryStorageIndex {
                    name,
                    hash,
                }) => {
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
                    tx.send((file, nodes)).await?;
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
                NetworkEvents::RequestTransition(Transition::Done) => break,
                _ => continue,
            }
        }

        drop(tx);

        log::debug!("Wait for file transfers to finish");

        event_processing_handler.await??;
        shared_state.transaction_log.flush().await?;

        Ok(())
    }
}
