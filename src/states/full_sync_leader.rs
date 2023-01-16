use std::{collections::HashMap, fmt::Display, time::Duration};
use tokio_stream::StreamExt;

use crate::{
    state_machine::StateStep,
    storage::{get_storage, FileInfo, Storage},
    NetworkEvents, SharedState,
};

#[derive(Debug)]
pub struct FullSyncLeader {}

impl Display for FullSyncLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

impl FullSyncLeader {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl StateStep<SharedState> for FullSyncLeader {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
        shared_state
            .connection_handler
            .broadcast(NetworkEvents::RequestTransition(
                crate::Transition::FullSync,
            ))
            .await?;

        log::info!("full sync starting as initiator....");

        for (storage_name, storage_config) in &shared_state.config.storages {
            let storage = get_storage(storage_name, storage_config, shared_state.config).await?;
            let peers_storages =
                wait_storage_from_peers(shared_state, &storage, storage_name).await?;

            dbg!(peers_storages);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        log::info!("full sync end....");

        shared_state
            .connection_handler
            .broadcast(NetworkEvents::RequestTransition(crate::Transition::Done))
            .await?;

        Ok(shared_state.default_state())
    }
}

async fn wait_storage_from_peers(
    shared_state: &SharedState,
    storage: &Storage,
    storage_name: &str,
) -> crate::Result<HashMap<u64, Vec<FileInfo>>> {
    let expected = shared_state
        .connection_handler
        .broadcast(NetworkEvents::Synchronization(
            crate::Synchronization::QueryStorageIndex {
                name: storage_name.to_string(),
                hash: storage.hash,
            },
        ))
        .await?;

    let stream = (shared_state.connection_handler.events_stream().await)
        .filter_map(|(peer_id, ev)| match ev {
            NetworkEvents::Synchronization(crate::Synchronization::ReplyStorageIndex {
                name,
                files,
            }) if name == storage_name && files.is_some() => Some((peer_id, files.unwrap())),
            _ => None,
        })
        .take(expected)
        .timeout(Duration::from_secs(3))
        .take_while(Result::is_ok);

    tokio::pin!(stream);

    let mut peer_storages = HashMap::default();
    while let Ok(Some((peer_id, files))) = stream.try_next().await {
        peer_storages.insert(peer_id, files);
    }

    Ok(peer_storages)

    // let x = shared_state
    //     .connection_handler
    //     .next_filtered_event(|_, ev| {
    //         matches!(
    //             ev,
    //             NetworkEvents::Synchronization(crate::Synchronization::ReplyStorageIndex { .. })
    //         )
    //     })
    //     .await;
    //
    // if let Some((
    //     peer_id,
    //     NetworkEvents::Synchronization(crate::Synchronization::ReplyStorageIndex { name, files }),
    // )) = shared_state.connection_handler.next_event().await
    // {
    //     if name == storage_name && files.is_some() {
    //     }
    // }
    //
    // Ok(peer_storages)
}
