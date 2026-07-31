use std::{collections::HashSet, fmt::Display};

use crate::{
    Context, StateMachineError,
    node_id::NodeId,
    state_machine::{Result, State, StateComposer},
    states::sync::{
        action_dispatcher::ActionDispatcher,
        events::{SaveSyncStatus, SyncCompleted},
        fetch_storages::FetchStorages,
    },
    sync_options::SyncOptions,
    transaction_log::SyncStatus,
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
    async fn execute(self, context: &Context) -> Result<Self::Output> {
        // TODO: investigate the possibility of start a follower inside the leader.
        // this will allow to simplify the leading code by replicating all the follower events
        // to achieve this, it will be necessary to create a loopback connection inside the rpc code
        // or refactor the follower code to allow for receiving events from two different sources
        //
        // the loopback connection will make the code cleaner, but it may create a problem for
        // broadcast events

        log::debug!("start sync as leader");
        for storage_name in context
            .config
            .storages
            .keys()
            .filter(|key| self.storages_to_sync(key.as_str()))
        {
            let mut nodes_in_session: Option<HashSet<NodeId>> = Default::default();
            let sync_result = FetchStorages::new(storage_name)
                .and_then(|storages| {
                    nodes_in_session = Some(
                        storages
                            .keys()
                            .filter(|node| **node != context.config.node_id_hashed)
                            .copied()
                            .collect(),
                    );
                    ActionDispatcher::new(storages)
                })
                // .and_then(|files_to_send| TransferFiles::new(None, files_to_send))
                .execute(context)
                .await;

            let sync_status = match sync_result {
                Err(StateMachineError::Err(err)) => {
                    log::error!("{err}");
                    SyncStatus::Fail
                }
                _ => SyncStatus::Done,
            };

            if let Some(nodes_in_session) = nodes_in_session {
                for node in nodes_in_session.iter() {
                    let _ = context
                        .transaction_log
                        .save_sync_status(node.to_string().as_str(), storage_name, sync_status)
                        .await;
                }

                let _ = context
                    .rpc
                    .multi_call(
                        SaveSyncStatus {
                            nodes: nodes_in_session.clone(),
                            storage_name,
                            status: sync_status,
                        },
                        nodes_in_session,
                    )
                    .ack()
                    .await;
            }
        }

        context.transaction_log.flush().await?;
        context.rpc.broadcast(SyncCompleted).ack().await?;

        if let Some(when_done) = context.when_done.clone().as_mut() {
            let _ = when_done.send(()).await;
        }

        log::info!("end sync as leader");

        Ok(())
    }
}
