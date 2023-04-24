use std::{collections::HashSet, fmt::Display};

use crate::{
    config::PathConfig,
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
pub struct SyncLeader {
    storages_to_sync: HashSet<String>,
}

impl SyncLeader {
    pub fn sync_everything() -> Self {
        Self {
            storages_to_sync: Default::default(),
        }
    }
    pub fn sync_just(storages_to_sync: HashSet<String>) -> Self {
        Self { storages_to_sync }
    }

    fn storages_to_sync(&self, (storage, _path_config): &(&String, &PathConfig)) -> bool {
        self.storages_to_sync.is_empty() || self.storages_to_sync.contains(storage.as_str())
    }
}

impl Display for SyncLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

impl State for SyncLeader {
    type Output = ();
    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        shared_state
            .connection_handler
            .broadcast(Transition::FullSync.into())
            .await?;

        log::info!("full sync starting as initiator....");

        for (storage_name, storage_config) in shared_state
            .config
            .storages
            .iter()
            .filter(|e| self.storages_to_sync(e))
        {
            let sync_result = BuildMatchingFiles::new(storage_name, storage_config)
                .and_then(|(peers, matched)| DispatchActions::new(peers, matched))
                .and_then(|files_to_send| TransferFiles::new(None, files_to_send))
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
