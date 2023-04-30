use std::fmt::Debug;

use tokio::fs::File;

use crate::{
    file_transfer::FileTransfer, network_events::NetworkEvents, node_id::NodeId,
    state_machine::State, SharedState, StateMachineError,
};

use super::{block_index, BlockHash, Transfer, TransferRecv, TransferType};

pub struct ReplyRequiredBlocks {
    transfer_chan: TransferRecv,
    transfer: Transfer,
    transfer_type: TransferType,
    source_node: NodeId,
}

impl ReplyRequiredBlocks {
    pub fn new(
        transfer_chan: TransferRecv,
        transfer: Transfer,
        transfer_type: TransferType,
        source_node: NodeId,
    ) -> Self {
        Self {
            transfer_chan,
            transfer_type,
            source_node,
            transfer,
        }
    }

    pub async fn get_required_block_index(
        &mut self,
        remote_block_index: Vec<BlockHash>,
        file_handle: &mut File,
    ) -> crate::Result<Vec<BlockHash>> {
        let file_size = self.transfer.file.file_size()?;
        let local_block_index = block_index::get_file_block_index(
            file_handle,
            self.transfer.block_size,
            file_size,
            file_handle.metadata().await?.len(),
        )
        .await?;

        let required: Vec<u64> = remote_block_index
            .into_iter()
            .zip(local_block_index.into_iter())
            .enumerate()
            .filter_map(|(i, (remote, local))| {
                if remote != local {
                    Some(i as u64)
                } else {
                    None
                }
            })
            .collect();

        Ok(required)
    }

    pub fn get_expected_blocks_for_full_file(&self) -> crate::Result<u64> {
        let file_size = self.transfer.file.file_size()?;
        let block_size = self.transfer.block_size;
        let expected_blocks = (file_size / block_size) + 1;
        Ok(expected_blocks)
    }
}

impl State for ReplyRequiredBlocks {
    type Output = (Transfer, NodeId, TransferRecv, u64);

    async fn execute(mut self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        match self.transfer_type {
            TransferType::FullFile => {
                let expected_blocks = self.get_expected_blocks_for_full_file()?;
                return Ok((
                    self.transfer,
                    self.source_node,
                    self.transfer_chan,
                    expected_blocks,
                ));
            }
            TransferType::NoTransfer => {
                return Ok((self.transfer, self.source_node, self.transfer_chan, 0));
            }
            TransferType::Partial => {}
        }

        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransfer::QueryRequiredBlocks { sender_block_index } => {
                    let mut file_handle = crate::storage::file_operations::open_file_for_reading(
                        shared_state.config,
                        &self.transfer.file,
                    )
                    .await?;

                    let required_blocks = self
                        .get_required_block_index(sender_block_index, &mut file_handle)
                        .await?;

                    let expected_blocks = required_blocks.len() as u64;
                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransfer::ReplyRequiredBlocks { required_blocks },
                            ),
                            node_id,
                        )
                        .await?;

                    return Ok((
                        self.transfer,
                        self.source_node,
                        self.transfer_chan,
                        expected_blocks,
                    ));
                }
                FileTransfer::RemovePeer if node_id == self.source_node => {
                    Err(StateMachineError::Abort)?
                }
                ev => log::error!("Received unexpected event: {ev:?}"),
            }
        }

        Err(StateMachineError::Abort)?
    }
}

impl Debug for ReplyRequiredBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyRequiredBlocks")
            .field("transfer", &self.transfer)
            .field("transfer_type", &self.transfer_type)
            .field("source_node", &self.source_node)
            .finish()
    }
}
