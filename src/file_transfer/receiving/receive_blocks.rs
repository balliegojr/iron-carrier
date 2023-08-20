use std::{collections::BTreeSet, io::SeekFrom};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    file_transfer::{block_index::BlockIndexPosition, FileTransferEvent, Transfer, TransferRecv},
    network_events::NetworkEvents,
    node_id::NodeId,
    state_machine::State,
    StateMachineError,
};

pub struct ReceiveBlocks {
    transfer: Transfer,
    transfer_chan: TransferRecv,
    source_node: NodeId,
    required_blocks: BTreeSet<BlockIndexPosition>,
}

impl State for ReceiveBlocks {
    type Output = ();

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        if self.required_blocks.is_empty() {
            shared_state
                .connection_handler
                .send_to(
                    NetworkEvents::FileTransfer(
                        self.transfer.transfer_id,
                        FileTransferEvent::TransferSucceeded,
                    ),
                    self.source_node,
                )
                .await?;
        }

        let mut file_handle = crate::storage::file_operations::open_file_for_writing(
            shared_state.config,
            &self.transfer.file,
        )
        .await?;

        self.adjust_file_size(&file_handle).await?;

        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransferEvent::TransferBlock { block_index, block } => {
                    self.write_block(block_index, &block, &mut file_handle)
                        .await?;
                    self.required_blocks.remove(&block_index);
                }
                FileTransferEvent::TransferComplete if self.required_blocks.is_empty() => {
                    file_handle.sync_all().await?;
                    crate::storage::fix_times_and_permissions(
                        &self.transfer.file,
                        shared_state.config,
                    )?;

                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransferEvent::TransferSucceeded,
                            ),
                            self.source_node,
                        )
                        .await?;

                    log::trace!("Transfer of {:?} finished", self.transfer.file.path);
                    return Ok(());
                }
                FileTransferEvent::TransferComplete => {
                    log::trace!(
                        "Transfer of {:?} failed, missing {} blocks",
                        self.transfer.file.path,
                        self.required_blocks.len()
                    );
                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransferEvent::TransferFailed {
                                    required_blocks: self.required_blocks.clone(),
                                },
                            ),
                            self.source_node,
                        )
                        .await?;
                }

                FileTransferEvent::RemovePeer if self.source_node == node_id => {
                    Err(StateMachineError::Abort)?
                }
                _ => {}
            }
        }

        Err(StateMachineError::Abort)?
    }
}

impl ReceiveBlocks {
    pub fn new(
        transfer: Transfer,
        transfer_chan: TransferRecv,
        required_blocks: BTreeSet<BlockIndexPosition>,
        source_node: NodeId,
    ) -> Self {
        Self {
            transfer,
            transfer_chan,
            required_blocks,
            source_node,
        }
    }

    pub async fn adjust_file_size(&mut self, file_handle: &File) -> crate::Result<()> {
        let file_size = self.transfer.file.file_size()?;
        if file_size != file_handle.metadata().await?.len() {
            file_handle.set_len(file_size).await?;
            log::trace!("set {:?} len to {file_size}", self.transfer.file.path);
        }

        Ok(())
    }
    pub async fn write_block(
        &mut self,
        block_index: BlockIndexPosition,
        block: &[u8],
        file_handle: &mut File,
    ) -> crate::Result<()> {
        let position = block_index.get_position(self.transfer.block_size);

        if file_handle.seek(SeekFrom::Start(position)).await? == position {
            file_handle.write_all(block).await?;
        }

        Ok(())
    }
}

impl std::fmt::Debug for ReceiveBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveBlocks")
            .field("transfer", &self.transfer)
            .field("source_node", &self.source_node)
            .field("required_blocks", &self.required_blocks)
            .finish()
    }
}
