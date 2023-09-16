use std::{collections::HashSet, fmt::Display};

use tokio_stream::StreamExt;

use crate::{
    file_transfer::{TransferFiles, TransferFilesStart},
    hash_type_id::HashTypeId,
    ignored_files::IgnoredFilesCache,
    network::rpc::RPCMessage,
    node_id::NodeId,
    state_machine::State,
    states::sync::events::SyncCompleted,
    storage::FileInfo,
    SharedState, StateMachineError,
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

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        log::debug!("start sync as follower");

        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut events = shared_state
            .rpc
            .subscribe_many(vec![
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
                    if let Err(err) = process_query_index_request(shared_state, request).await {
                        log::error!("{err}")
                    }
                }
                SyncCompleted::ID => {
                    request.ack().await?;
                    break;
                }
                DeleteFile::ID => {
                    if let Err(err) =
                        process_delete_file_request(shared_state, &mut ignored_files_cache, request)
                            .await
                    {
                        log::error!("{err}")
                    }
                }
                MoveFile::ID => {
                    if let Err(err) =
                        process_move_file_request(shared_state, &mut ignored_files_cache, request)
                            .await
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
                    .execute(shared_state)
                    .await
                    {
                        log::error!("{err}")
                    }
                }
                _ => unreachable!(),
            }
        }

        log::info!("end sync as follower");

        events.free().await;

        Ok(())
    }
}

async fn process_query_index_request(
    shared_state: &SharedState,
    request: RPCMessage,
) -> crate::Result<()> {
    let query: QueryStorageIndex = request.data()?;
    let storage_index = match shared_state.config.storages.get(&query.name) {
        Some(storage_config) => {
            match crate::storage::get_storage_info(
                &query.name,
                storage_config,
                &shared_state.transaction_log,
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
    shared_state: &SharedState,
    ignored_files_cache: &mut IgnoredFilesCache,
    request: RPCMessage,
) -> crate::Result<()> {
    let op: DeleteFile = request.data()?;
    crate::storage::file_operations::delete_file(
        shared_state.config,
        &shared_state.transaction_log,
        &op.file,
        ignored_files_cache,
    )
    .await?;
    request.ack().await
}

async fn process_move_file_request(
    shared_state: &SharedState,
    ignored_files_cache: &mut IgnoredFilesCache,
    request: RPCMessage,
) -> crate::Result<()> {
    let op: MoveFile = request.data()?;
    crate::storage::file_operations::move_file(
        shared_state.config,
        &shared_state.transaction_log,
        &op.file,
        ignored_files_cache,
    )
    .await?;

    request.ack().await
}

async fn process_send_file_to_request(
    files_to_send: &mut Vec<(FileInfo, HashSet<NodeId>)>,
    request: RPCMessage,
) -> crate::Result<()> {
    let op: SendFileTo = request.data()?;
    files_to_send.push((op.file, op.nodes));

    request.ack().await
}
