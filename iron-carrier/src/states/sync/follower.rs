use std::{fmt::Display, sync::Arc};

use tokio::sync::{Mutex, Semaphore};
use tokio_stream::StreamExt;

use crate::{
    Context, StateMachineError,
    file_transfer::{self},
    ignored_files::IgnoredFilesCache,
    network::rpc::RPCMessage,
    node_id::NodeId,
    protocol::MessageTypes,
    state_machine::{Result, State},
};

use super::events::{
    DeleteFile, MoveFile, QueryStorageIndex, SaveSyncStatus, StorageIndex, StorageIndexStatus,
};

#[derive(Debug)]
pub struct Follower {
    sync_leader: NodeId,
}

impl Display for Follower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSyncFollower")
    }
}

impl Follower {
    pub fn new(sync_leader: NodeId) -> Self {
        Self { sync_leader }
    }
}

impl State for Follower {
    type Output = ();

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        log::debug!("start sync as follower");

        let ignored_files_cache: Arc<Mutex<IgnoredFilesCache>> = Default::default();
        let mut events = context
            .rpc
            .subscribe([
                MessageTypes::QueryStorageIndex,
                MessageTypes::SyncCompleted,
                MessageTypes::DeleteFile,
                MessageTypes::MoveFile,
                MessageTypes::SendFileTo,
                MessageTypes::SaveSyncStatus,
                MessageTypes::ReceiveFile,
            ])
            .await?;

        let parallel_transfers = Arc::new(Semaphore::new(
            1.max(context.config.max_parallel_transfers.into()),
        ));

        loop {
            let request = events.next().await.ok_or(StateMachineError::Abort)?;
            // avoid processing broadcasts send by leaders in other syncs.
            // this situation can happen if other nodes started a new sync in the middle of a
            // ongoing sync.

            if request.node_id() != self.sync_leader {
                // TODO: introduce a busy response?
                request.cancel().await?;
                continue;
            }

            match request.message_type()? {
                MessageTypes::QueryStorageIndex => {
                    if let Err(err) = process_query_index_request(context, request).await {
                        log::error!("{err}")
                    }
                }
                MessageTypes::SyncCompleted => {
                    request.ack().await?;
                    break;
                }
                MessageTypes::DeleteFile => {
                    let mut guard = ignored_files_cache.lock().await;
                    if let Err(err) =
                        process_delete_file_request(context, &mut guard, request).await
                    {
                        log::error!("{err}")
                    }
                }
                MessageTypes::MoveFile => {
                    let mut guard = ignored_files_cache.lock().await;
                    if let Err(err) = process_move_file_request(context, &mut guard, request).await
                    {
                        log::error!("{err}")
                    }
                }
                MessageTypes::SendFileTo => {
                    tokio::spawn(file_transfer::send_file(
                        context.clone(),
                        request,
                        parallel_transfers.clone(),
                    ));
                }
                MessageTypes::ReceiveFile => {
                    tokio::spawn(file_transfer::receive_file(
                        context.clone(),
                        ignored_files_cache.clone(),
                        request,
                        parallel_transfers.clone(),
                    ));
                }
                MessageTypes::SaveSyncStatus => {
                    if let Err(err) = process_save_sync_status_request(context, request).await {
                        log::error!("{err}");
                    }
                }
                _ => unreachable!(),
            }
        }

        context.transaction_log.flush().await?;
        log::debug!("end sync as follower");

        Ok(())
    }
}

async fn process_query_index_request(context: &Context, request: RPCMessage) -> anyhow::Result<()> {
    let query: QueryStorageIndex = request.data()?;
    let storage_index = match crate::storage::build(context, &query.name).await {
        Ok(storage) => {
            if storage.hash != query.hash {
                StorageIndexStatus::SyncNecessary(storage)
            } else {
                StorageIndexStatus::StorageInSync
            }
        }
        Err(err) => {
            log::error!("There was an error reading the storage: {err}");
            StorageIndexStatus::StorageMissing
        }
    };

    request
        .reply(StorageIndex {
            name: query.name,
            storage_index,
        })
        .await
}

async fn process_delete_file_request(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let op: DeleteFile = request.data()?;
    crate::storage::file_operations::delete_file(
        context,
        ignored_files_cache,
        &op.storage,
        &op.path,
        op.timestamp,
    )
    .await?;
    request.ack().await
}

async fn process_move_file_request(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let op: MoveFile = request.data()?;
    crate::storage::file_operations::move_file(
        context,
        ignored_files_cache,
        &op.storage,
        &op.src_path,
        &op.dst_path,
        op.modified_at,
    )
    .await?;

    request.ack().await
}

async fn process_save_sync_status_request(
    context: &Context,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let save_sync_status: SaveSyncStatus = request.data()?;
    for node in save_sync_status
        .nodes
        .iter()
        .filter(|n| **n != context.config.node_id_hashed)
    {
        let _ = context
            .transaction_log
            .save_sync_status(
                node.to_string().as_str(),
                save_sync_status.storage_name,
                save_sync_status.status,
            )
            .await;
    }

    request.ack().await
}
