use std::fmt::Display;

use crate::{
    file_transfer::TransferFiles,
    // file_transfer::TransferFiles,
    state_machine::{Result, State, StateComposer},
    states::sync::{actions::Dispatcher, events::SyncCompleted, files_matcher::FilesMatcher},
    sync_options::SyncOptions,
    SharedState,
    StateMachineError,
};

#[derive(Debug, Default)]
pub struct Leader {
    sync_options: SyncOptions,
}

impl Leader {
    pub fn sync(sync_options: SyncOptions) -> Self {
        Self { sync_options }
    }

    fn storages_to_sync(&self, storage: &str) -> bool {
        self.sync_options.storages().is_empty() || self.sync_options.storages().contains(storage)
    }
}

impl Display for Leader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

impl State for Leader {
    type Output = ();
    async fn execute(self, shared_state: &SharedState) -> Result<Self::Output> {
        log::debug!("start sync as leader");
        for (storage_name, storage_config) in shared_state
            .config
            .storages
            .iter()
            .filter(|(key, _)| self.storages_to_sync(key.as_str()))
        {
            let sync_result = FilesMatcher::new(storage_name, storage_config)
                .and_then(|(peers, matched)| Dispatcher::new(peers, matched))
                .and_then(|files_to_send| TransferFiles::new(None, files_to_send))
                .execute(shared_state)
                .await;

            if let Err(StateMachineError::Err(err)) = sync_result {
                log::error!("{err}");
            }
        }

        shared_state.transaction_log.flush().await?;
        shared_state.rpc.broadcast(SyncCompleted).ack().await?;

        if let Some(when_done) = shared_state.when_done.clone().as_mut() {
            let _ = when_done.send(()).await;
        }

        log::info!("end sync as leader");

        Ok(())
    }
}
