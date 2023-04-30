use std::io::SeekFrom;

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    network_events::NetworkEvents, node_id::NodeId, state_machine::State, StateMachineError,
};

use super::{BlockIndex, FileTransfer, Transfer, TransferRecv};

pub struct ReceiveBlocks {
    transfer: Transfer,
    transfer_chan: TransferRecv,
    expected_blocks: u64,
    source_node: NodeId,
}

impl State for ReceiveBlocks {
    type Output = ();

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        if self.expected_blocks == 0 {
            shared_state
                .connection_handler
                .send_to(
                    NetworkEvents::FileTransfer(
                        self.transfer.transfer_id,
                        FileTransfer::TransferSucceeded,
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

        log::trace!("Adjusting file size");
        self.adjust_file_size(&mut file_handle).await?;

        log::trace!("waiting blocks");
        let mut received_blocks = 0;
        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransfer::TransferBlock { block_index, block } => {
                    self.write_block(block_index, &block, &mut file_handle)
                        .await?;
                    received_blocks += 1;
                }
                FileTransfer::TransferComplete if received_blocks == self.expected_blocks => {
                    log::trace!("Success");
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
                                FileTransfer::TransferSucceeded,
                            ),
                            self.source_node,
                        )
                        .await?;

                    return Ok(());
                }
                FileTransfer::TransferComplete if received_blocks != self.expected_blocks => {
                    // FIXME: handle 'received blocks'
                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransfer::TransferFailed {
                                    received_blocks: Vec::default(),
                                },
                            ),
                            self.source_node,
                        )
                        .await?;
                }

                FileTransfer::RemovePeer if self.source_node == node_id => {
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
        expected_blocks: u64,
        source_node: NodeId,
    ) -> Self {
        Self {
            transfer,
            transfer_chan,
            expected_blocks,
            source_node,
        }
    }

    pub async fn adjust_file_size(&mut self, file_handle: &mut File) -> crate::Result<()> {
        let file_size = self.transfer.file.file_size()?;
        if file_size != file_handle.metadata().await?.len() {
            file_handle.set_len(file_size).await?;
            log::trace!("set {:?} len to {file_size}", self.transfer.file.path);
        }

        Ok(())
    }
    pub async fn write_block(
        &mut self,
        block_index: BlockIndex,
        block: &[u8],
        file_handle: &mut File,
    ) -> crate::Result<()> {
        let position = block_index * self.transfer.block_size;

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
            .field("expected_blocks", &self.expected_blocks)
            .field("source_node", &self.source_node)
            .finish()
    }
}
