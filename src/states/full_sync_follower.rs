use std::fmt::Display;

use tokio_stream::StreamExt;

use crate::{
    file_transfer::process_transfer_events,
    network_events::{NetworkEvents, Synchronization, Transition},
    state_machine::StateStep,
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

#[async_trait::async_trait]
impl StateStep for FullSyncFollower {
    type GlobalState = SharedState;
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<GlobalState = Self::GlobalState>>>> {
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
        let event_processing_handler = tokio::spawn(process_transfer_events(
            shared_state.connection_handler,
            shared_state.config,
            rx,
        ));

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
                                shared_state.config,
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
                    crate::storage::delete_file(&file, shared_state.config).await?;
                }
                NetworkEvents::RequestTransition(Transition::Done) => break,
                _ => continue,
            }
        }

        event_processing_handler.await??;

        Ok(None)
    }
}
