use std::{collections::HashMap, fmt::Display, time::Duration};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::{
    config::PathConfig,
    file_transfer::process_transfer_events,
    network_events::{NetworkEvents, Synchronization, Transition},
    state_machine::StateStep,
    storage::{get_storage, FileInfo, Storage},
    SharedState,
};

mod matching_files;
mod sync_actions;

#[derive(Debug, Default)]
pub struct FullSyncLeader;

impl Display for FullSyncLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncLeader")
    }
}

#[async_trait::async_trait]
impl StateStep for FullSyncLeader {
    type GlobalState = SharedState;
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<GlobalState = Self::GlobalState>>>> {
        shared_state
            .connection_handler
            .broadcast(Transition::FullSync.into())
            .await?;

        // FIXME: Wait for confirmation that Transition was successful, or implement an event
        // stream that buffer the events...
        tokio::time::sleep(Duration::from_millis(100)).await;

        log::info!("full sync starting as initiator....");

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let transfer_events_handle = tokio::spawn(process_transfer_events(
            shared_state.connection_handler,
            shared_state.config,
            rx,
        ));

        for (storage_name, storage_config) in &shared_state.config.storages {
            sync_storage(storage_name, storage_config, shared_state, &tx).await?;
        }

        drop(tx);
        transfer_events_handle.await??;

        log::info!("full sync end....");

        shared_state
            .connection_handler
            .broadcast(Transition::Done.into())
            .await?;

        Ok(None)
    }
}

async fn sync_storage(
    storage_name: &str,
    storage_config: &PathConfig,
    shared_state: &SharedState,
    send_file_events: &Sender<(FileInfo, Vec<u64>)>,
) -> crate::Result<()> {
    log::trace!("Synchronization of storage {storage_name} started");

    let storage = get_storage(storage_name, storage_config, shared_state.config).await?;
    let peers_storages = wait_storage_from_peers(shared_state, &storage, storage_name).await?;

    let peers: Vec<u64> = peers_storages.keys().copied().collect();
    log::trace!("Storage {storage_name} to be synchronized with {peers:?}");

    let actions =
        matching_files::match_files(storage, peers_storages, shared_state.config.node_id_hashed)
            .filter_map(|file| {
                sync_actions::get_file_sync_action(shared_state.config.node_id_hashed, &peers, file)
            });

    for action in actions {
        if let Err(err) = execute_action(action, shared_state, send_file_events).await {
            log::error!("failed to execute action {err}");
        }
    }

    Ok(())
}

async fn wait_storage_from_peers(
    shared_state: &SharedState,
    storage: &Storage,
    storage_name: &str,
) -> crate::Result<HashMap<u64, Vec<FileInfo>>> {
    let expected = shared_state
        .connection_handler
        .broadcast(
            Synchronization::QueryStorageIndex {
                name: storage_name.to_string(),
                hash: storage.hash,
            }
            .into(),
        )
        .await?;

    let stream = (shared_state.connection_handler.events_stream().await)
        .filter_map(|(peer_id, ev)| match ev {
            NetworkEvents::Synchronization(sync_event) => match sync_event {
                Synchronization::ReplyStorageIndex { name, files }
                    if name == storage_name && files.is_some() =>
                {
                    Some((peer_id, files.unwrap()))
                }
                _ => None,
            },
            _ => None,
        })
        .take(expected)
        .timeout(Duration::from_secs(10))
        .take_while(Result::is_ok);

    tokio::pin!(stream);

    let mut peer_storages = HashMap::default();
    while let Ok(Some((peer_id, files))) = stream.try_next().await {
        peer_storages.insert(peer_id, files);
    }

    Ok(peer_storages)
}

async fn execute_action(
    action: sync_actions::SyncAction,
    shared_state: &SharedState,
    send_file_events: &Sender<(FileInfo, Vec<u64>)>,
) -> crate::Result<()> {
    match action {
        sync_actions::SyncAction::Delete { file, nodes } => {
            if nodes.contains(&shared_state.config.node_id_hashed) {
                crate::storage::delete_file(&file, shared_state.config).await?;
            }

            log::trace!("Requesting {nodes:?} to delete {:?} ", file.path);
            shared_state
                .connection_handler
                .broadcast_to(
                    Synchronization::DeleteFile { file: file.clone() }.into(),
                    &nodes,
                )
                .await?;
        }
        sync_actions::SyncAction::Send { file, nodes } => {
            send_file_events.send((file, nodes)).await?;
        }
        sync_actions::SyncAction::DelegateSend {
            delegate_to,
            file,
            nodes,
        } => {
            log::trace!(
                "Requesting {delegate_to} to send {:?} to {nodes:?}",
                file.path
            );

            shared_state
                .connection_handler
                .send_to(
                    Synchronization::SendFileTo { file, nodes }.into(),
                    delegate_to,
                )
                .await?
        }
    }

    Ok(())
}
