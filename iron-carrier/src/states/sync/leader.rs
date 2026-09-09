use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use crate::{
    Context, StateMachineError,
    node_id::NodeId,
    state_machine::{Result, State, StateComposer},
    states::sync::{
        action_dispatcher::ActionDispatcher,
        events::{ListStorageNames, SaveSyncStatus, SyncCompleted},
        fetch_storages_index::FetchStorageIndex,
        follower::Follower,
    },
    sync_options::SyncOptions,
    transaction_log::SyncStatus,
};

#[derive(Debug, Default)]
pub struct Leader {
    sync_options: Option<SyncOptions>,
}

impl Leader {
    pub fn sync(sync_options: Option<SyncOptions>) -> Self {
        Self { sync_options }
    }

    async fn list_storages(&self, context: &Context) -> Result<HashMap<String, HashSet<NodeId>>> {
        let replies = context
            .rpc
            .broadcast(ListStorageNames)
            .result()
            .await?
            .replies();

        let mut available_storages: HashMap<String, HashSet<NodeId>> = Default::default();
        for reply in replies {
            let node_storages = reply.data()?;

            for storage in node_storages.0 {
                available_storages
                    .entry(storage)
                    .or_default()
                    .insert(reply.node_id());
            }
        }

        if let Some(options) = self.sync_options.as_ref() {
            available_storages.retain(|s, _v| options.storages().contains(s));
        }

        Ok(available_storages)
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
        let follower = {
            // By spawning a follower inside the leader, the leader logic can be simplified by not
            // having leader exclusive logic to handle events.

            let follower_context = context.clone();
            tokio::spawn(async move {
                Follower::new(follower_context.config.node_id_hashed)
                    .execute(&follower_context)
                    .await
            })
        };

        log::debug!("start sync as leader");
        for (storage_name, nodes_in_session) in self.list_storages(context).await? {
            let sync_result = FetchStorageIndex::new(storage_name.clone())
                .and_then(ActionDispatcher::new)
                .execute(context)
                .await;

            let sync_status = match sync_result {
                Err(StateMachineError::Err(err)) => {
                    log::error!("{err}");
                    SyncStatus::Fail
                }
                _ => SyncStatus::Done,
            };

            let _ = context
                .rpc
                .multi_call(
                    SaveSyncStatus {
                        nodes: nodes_in_session.clone(),
                        storage_name: &storage_name,
                        status: sync_status,
                    },
                    nodes_in_session,
                )
                .ack()
                .await;
        }

        context.transaction_log.flush().await?;
        context
            .rpc
            .call(SyncCompleted, context.config.node_id_hashed)
            .ack()
            .await?;
        context.rpc.broadcast(SyncCompleted).ack().await?;

        if let Err(err) = follower.await {
            log::error!("failed to shutdown follower {err:?}");
        }

        if let Some(when_done) = context.when_done.clone().as_mut() {
            let _ = when_done.send(()).await;
        }

        log::info!("end sync as leader");

        Ok(())
    }
}
