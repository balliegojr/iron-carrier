use std::{collections::BTreeSet, io::SeekFrom};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc::Receiver,
};

use crate::{
    file_transfer::{
        block_index::BlockIndexPosition,
        file_transfer_event::{TransferBlock, TransferComplete},
        FileTransferEvent, Transfer, TransferRecv,
    },
    network::rpc::RPCMessage,
    network_events::NetworkEvents,
    node_id::NodeId,
    state_machine::State,
    StateMachineError,
};

use super::ReceivingTransfer;

pub struct ReceiveBlocks {
    receiving: ReceivingTransfer,
    data_input: Receiver<RPCMessage>,
}

impl State for ReceiveBlocks {
    type Output = ();

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        self.adjust_file_size(&self.receiving.handle).await?;

        while let Some(message) = self.data_input.recv().await {
            if message.is_type::<TransferBlock>() {
                let data: TransferBlock = message.data()?;

                self.write_block(data.block_index, &data.block, &mut self.receiving.handle)
                    .await?;
                self.receiving.block_index.remove(&data.block_index);
                message.ack().await;
            } else {
                let data: TransferComplete = message.data()?;
                if self.receiving.block_index.is_empty() {
                    file_handle.sync_all().await?;
                    crate::storage::fix_times_and_permissions(
                        &self.transfer.file,
                        shared_state.config,
                    )?;
                } else {
                }
            }
        }

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
