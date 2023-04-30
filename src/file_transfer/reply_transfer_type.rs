use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    config::Config, hash_helper, ignored_files::IgnoredFilesCache, network_events::NetworkEvents,
    node_id::NodeId, state_machine::State, storage::FileInfo, StateMachineError,
};

use super::{FileTransfer, Transfer, TransferRecv, TransferType};
pub struct ReplyTransfer {
    transfer_chan: TransferRecv,
    transfer: Transfer,
    ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
    source_node: NodeId,
}

impl ReplyTransfer {
    pub fn new(
        transfer: Transfer,
        transfer_chan: TransferRecv,
        ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
        source_node: NodeId,
    ) -> Self {
        Self {
            transfer,
            transfer_chan,
            ignored_files_cache,
            source_node,
        }
    }
}

impl std::fmt::Debug for ReplyTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyTransfer")
            .field("transfer", &self.transfer)
            .field("source_node", &self.source_node)
            .finish()
    }
}

impl State for ReplyTransfer {
    type Output = (NodeId, Transfer, TransferRecv, TransferType);

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransfer::QueryTransferType { .. } => {
                    let mut ignored_files_cache = self.ignored_files_cache.lock().await;
                    let transfer_type = get_transfer_type(
                        &self.transfer.file,
                        shared_state.config,
                        &mut ignored_files_cache,
                    )
                    .await?;
                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransfer::ReplyTransferType { transfer_type },
                            ),
                            node_id,
                        )
                        .await?;

                    return Ok((
                        self.source_node,
                        self.transfer,
                        self.transfer_chan,
                        transfer_type,
                    ));
                }
                FileTransfer::RemovePeer if node_id == self.source_node => {
                    Err(StateMachineError::Abort)?
                }
                _ => {
                    log::error!("Received unexpected event: {ev:?}");
                }
            }
        }

        Err(StateMachineError::Abort)?
    }
}

async fn get_transfer_type(
    remote_file: &FileInfo,
    config: &'static Config,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<TransferType> {
    if let Some(storage_config) = config.storages.get(&remote_file.storage) {
        if ignored_files_cache
            .get(storage_config)
            .await
            .is_ignored(&remote_file.path)
        {
            return Ok(TransferType::NoTransfer);
        }
    }

    let file_path = remote_file.get_absolute_path(config)?;
    if !file_path.exists() {
        return Ok(TransferType::FullFile);
    }

    let local_file = remote_file.get_local_file_info(config)?;
    if hash_helper::calculate_file_hash(remote_file)
        != hash_helper::calculate_file_hash(&local_file)
    {
        Ok(TransferType::Partial)
    } else {
        Ok(TransferType::NoTransfer)
    }
}
