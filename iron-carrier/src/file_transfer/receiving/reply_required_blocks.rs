use std::{collections::BTreeSet, fmt::Debug};

use crate::{
    config::Config,
    file_transfer::{
        block_index::{self, BlockIndexPosition, FullIndex},
        FileTransferEvent, Transfer, TransferRecv, TransferType,
    },
    network_events::NetworkEvents,
    node_id::NodeId,
    state_machine::State,
    SharedState, StateMachineError,
};

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

    pub async fn get_local_full_index(&mut self, config: &Config) -> crate::Result<FullIndex> {
        let mut file_handle =
            crate::storage::file_operations::open_file_for_reading(config, &self.transfer.file)
                .await?;

        let local_file_size = file_handle.metadata().await?.len();
        let file_size = self.transfer.file.file_size()?;
        let local_block_index = block_index::get_file_block_index(
            &mut file_handle,
            self.transfer.block_size,
            file_size,
            local_file_size,
        )
        .await?;

        Ok(local_block_index)
    }

    pub fn get_expected_blocks_for_full_file(&self) -> crate::Result<BTreeSet<BlockIndexPosition>> {
        let file_size = self.transfer.file.file_size()?;
        let block_size = self.transfer.block_size;
        let expected_blocks = (file_size / block_size) + 1;

        Ok((0..expected_blocks).map(Into::into).collect())
    }
}

impl State for ReplyRequiredBlocks {
    type Output = (Transfer, NodeId, TransferRecv, BTreeSet<BlockIndexPosition>);

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
                return Ok((
                    self.transfer,
                    self.source_node,
                    self.transfer_chan,
                    Default::default(),
                ));
            }
            TransferType::Partial => {}
        }

        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransferEvent::QueryRequiredBlocks { sender_block_index } => {
                    let local_index = self.get_local_full_index(shared_state.config).await?;

                    let required_blocks = sender_block_index.generate_diff(local_index);
                    shared_state
                        .connection_handler
                        .send_to(
                            NetworkEvents::FileTransfer(
                                self.transfer.transfer_id,
                                FileTransferEvent::ReplyRequiredBlocks {
                                    required_blocks: required_blocks.clone(),
                                },
                            ),
                            node_id,
                        )
                        .await?;

                    return Ok((
                        self.transfer,
                        self.source_node,
                        self.transfer_chan,
                        required_blocks,
                    ));
                }
                FileTransferEvent::RemovePeer if node_id == self.source_node => {
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
