use std::{collections::HashSet, fmt::Display};

use tokio_stream::StreamExt;

use crate::{
    file_transfer::{TransferFiles, TransferFilesStart},
    hash_type_id::HashTypeId,
    ignored_files::IgnoredFilesCache,
    network::rpc::RPCMessage,
    node_id::NodeId,
    state_machine::{Result, State},
    states::sync::events::SyncCompleted,
    storage::FileInfo,
    Context, StateMachineError,
};

use super::events::{
    DeleteFile, MoveFile, QueryStorageIndex, SendFileTo, StorageIndex, StorageIndexStatus,
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

        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut events = context
            .rpc
            .subscribe(&[
                QueryStorageIndex::ID,
                SyncCompleted::ID,
                DeleteFile::ID,
                MoveFile::ID,
                SendFileTo::ID,
                TransferFilesStart::ID,
            ])
            .await?;

        let mut files_to_send: Vec<(FileInfo, HashSet<NodeId>)> = Default::default();

        loop {
            let request = events.next().await.ok_or(StateMachineError::Abort)?;
            match request.type_id() {
                QueryStorageIndex::ID => {
                    if let Err(err) = process_query_index_request(context, request).await {
                        log::error!("{err}")
                    }
                }
                SyncCompleted::ID => {
                    request.ack().await?;
                    break;
                }
                DeleteFile::ID => {
                    if let Err(err) =
                        process_delete_file_request(context, &mut ignored_files_cache, request)
                            .await
                    {
                        log::error!("{err}")
                    }
                }
                MoveFile::ID => {
                    if let Err(err) =
                        process_move_file_request(context, &mut ignored_files_cache, request).await
                    {
                        log::error!("{err}")
                    }
                }
                SendFileTo::ID => {
                    if let Err(err) =
                        process_send_file_to_request(&mut files_to_send, request).await
                    {
                        log::error!("{err}")
                    }
                }
                TransferFilesStart::ID => {
                    request.ack().await?;
                    if let Err(err) = TransferFiles::new(
                        Some(self.sync_leader),
                        std::mem::take(&mut files_to_send),
                    )
                    .execute(context)
                    .await
                    {
                        log::error!("{err}")
                    }
                }
                _ => unreachable!(),
            }
        }

        log::info!("end sync as follower");

        Ok(())
    }
}

async fn process_query_index_request(context: &Context, request: RPCMessage) -> anyhow::Result<()> {
    let query: QueryStorageIndex = request.data()?;
    let storage_index = match context.config.storages.get(&query.name) {
        Some(storage_config) => {
            match crate::storage::get_storage_info(
                &query.name,
                storage_config,
                &context.transaction_log,
            )
            .await
            {
                Ok(storage) => {
                    if storage.hash != query.hash {
                        StorageIndexStatus::SyncNecessary(storage.files)
                    } else {
                        StorageIndexStatus::StorageInSync
                    }
                }
                Err(err) => {
                    log::error!("There was an error reading the storage: {err}");
                    StorageIndexStatus::StorageMissing
                }
            }
        }
        None => StorageIndexStatus::StorageMissing,
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
        context.config,
        &context.transaction_log,
        &op.file,
        ignored_files_cache,
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
        context.config,
        &context.transaction_log,
        &op.file,
        ignored_files_cache,
    )
    .await?;

    request.ack().await
}

async fn process_send_file_to_request(
    files_to_send: &mut Vec<(FileInfo, HashSet<NodeId>)>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let op: SendFileTo = request.data()?;
    files_to_send.push((op.file, op.nodes));

    request.ack().await
}
