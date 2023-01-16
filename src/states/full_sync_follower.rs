use std::{fmt::Display, time::Duration};

use tokio_stream::StreamExt;

use crate::{state_machine::StateStep, NetworkEvents, SharedState, Synchronization};

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
impl StateStep<SharedState> for FullSyncFollower {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
        log::info!("full sync starting....");

        let mut event_stream =
            (shared_state.connection_handler.events_stream().await).filter_map(|(peer_id, ev)| {
                if peer_id == self.sync_leader {
                    Some(ev)
                } else {
                    None
                }
            });

        while let Some(ev) = event_stream.next().await {
            match ev {
                NetworkEvents::Synchronization(crate::Synchronization::QueryStorageIndex {
                    name,
                    hash,
                }) => {
                    if let Some(storage_config) = shared_state.config.storages.get(&name) {
                        match crate::storage::get_storage(
                            &name,
                            storage_config,
                            shared_state.config,
                        )
                        .await
                        {
                            Ok(storage) => {
                                if storage.hash != hash {
                                    shared_state
                                        .connection_handler
                                        .send_to(
                                            self.sync_leader,
                                            NetworkEvents::Synchronization(
                                                Synchronization::ReplyStorageIndex {
                                                    name,
                                                    files: Some(storage.files),
                                                },
                                            ),
                                        )
                                        .await;
                                }
                            }
                            Err(err) => {
                                log::error!("There was an error reading the storage: {err}");
                                shared_state
                                    .connection_handler
                                    .send_to(
                                        self.sync_leader,
                                        NetworkEvents::Synchronization(
                                            Synchronization::ReplyStorageIndex {
                                                name,
                                                files: None,
                                            },
                                        ),
                                    )
                                    .await;
                            }
                        };
                    }
                }
                NetworkEvents::RequestTransition(crate::Transition::Done) => break,
                _ => continue,
            }
        }

        Ok(shared_state.default_state())
    }
}
