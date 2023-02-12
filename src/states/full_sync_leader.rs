use std::{collections::HashSet, fmt::Display};

use crate::{
    file_transfer::TransferFiles,
    network_events::Transition,
    state_machine::{StateComposer, Step},
    SharedState,
};
use matching_files::BuildMatchingFiles;
use sync_actions::DispatchActions;
use tokio::sync::mpsc::Sender;

mod matching_files;
mod sync_actions;

#[derive(Debug, Default)]
pub struct FullSyncLeader {
    storages_to_sync: HashSet<String>,
    when_sync_done: Option<Sender<()>>,
}

impl FullSyncLeader {
    pub fn sync_everything(when_sync_done: Option<Sender<()>>) -> Self {
        Self {
            storages_to_sync: Default::default(),
            when_sync_done,
        }
    }
    pub fn sync_just(
        storages_to_sync: HashSet<String>,
        when_sync_done: Option<Sender<()>>,
    ) -> Self {
        Self {
            storages_to_sync,
            when_sync_done,
        }
    }
}

impl Display for FullSyncLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

impl Step for FullSyncLeader {
    type Output = ();
    async fn execute(self, shared_state: &SharedState) -> crate::Result<Option<Self::Output>> {
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
            // FIXME: This loop is necessary to do a two pass in the storage synchronization
            // Why is this necessary? When 3 or more nodes are in the sync process, A, B and C
            // if A is the leader and A and B states are equal, but C is different, A will sync
            // with C but B will not be synched.
            // There are 4 possible solutions to this problem
            // 1. Two pass synchronization (The current one, I consider a workaround...)
            // 2. Always return the storage index when querying the hash, this is probably the
            //    best solution, since we already have to read the file tree to build the hash
            // 3. Query a second time if one of the nodes have a different hash,
            // 4. change the response for query to be explicity about not having a storage, this way we
            //    will be able to assume that actions should be replicated to nodes that have
            //    the same hash

            loop {
                let sync_result = BuildMatchingFiles::new(storage_name, storage_config)
                    .and_then(|(peers, matched)| DispatchActions::new(peers, matched))
                    .and_then(|(files_to_send, peers_with_tasks)| {
                        TransferFiles::new(files_to_send, peers_with_tasks)
                    })
                    .execute(shared_state)
                    .await;

                match sync_result {
                    Ok(None) => break,
                    Err(err) => {
                        log::error!("{err}");
                        break;
                    }
                    _ => {}
                }
            }
        }

        shared_state.transaction_log.flush().await?;

        log::info!("full sync end....");

        shared_state
            .connection_handler
            .broadcast(Transition::Done.into())
            .await?;

        if let Some(when_sync_done) = self.when_sync_done {
            let _ = when_sync_done.send(()).await;
        }

        Ok(Some(()))
    }
}
