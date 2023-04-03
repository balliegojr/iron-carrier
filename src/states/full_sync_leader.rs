use std::{collections::HashSet, fmt::Display};

use crate::{
    file_transfer::TransferFiles,
    network_events::Transition,
    state_machine::{State, StateComposer},
    SharedState, StateMachineError,
};
use matching_files::BuildMatchingFiles;
use sync_actions::DispatchActions;

mod matching_files;
mod sync_actions;

#[derive(Debug, Default)]
pub struct FullSyncLeader {
    storages_to_sync: HashSet<String>,
}

impl FullSyncLeader {
    pub fn sync_everything() -> Self {
        Self {
            storages_to_sync: Default::default(),
        }
    }
    pub fn sync_just(storages_to_sync: HashSet<String>) -> Self {
        Self { storages_to_sync }
    }
}

impl Display for FullSyncLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

impl State for FullSyncLeader {
    type Output = ();
    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        shared_state
            .connection_handler
            .broadcast(Transition::FullSync.into())
            .await?;

        log::info!("full sync starting as initiator....");

        for (storage_name, storage_config) in
            shared_state.config.storages.iter().filter(|(storage, _)| {
                self.storages_to_sync.is_empty() || self.storages_to_sync.contains(storage.as_str())
            })
        {
            let sync_result = BuildMatchingFiles::new(storage_name, storage_config)
                .and_then(|(peers, matched)| DispatchActions::new(peers, matched))
                .and_then(|(files_to_send, peers_with_tasks)| {
                    TransferFiles::new(files_to_send, peers_with_tasks)
                })
                .execute(shared_state)
                .await;

            if let Err(err) = sync_result {
                if !err.is::<StateMachineError>() {
                    log::error!("{err}");
                }
            }
        }

        shared_state.transaction_log.flush().await?;

        log::info!("full sync end....");

        shared_state
            .connection_handler
            .broadcast(Transition::Done.into())
            .await?;

        if let Some(when_sync_done) = shared_state.after_sync {
            let _ = when_sync_done.send(()).await;
        }

        Ok(())
    }
}
