use std::{
    collections::{BTreeMap, HashMap},
    io::SeekFrom,
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{
    network_events::NetworkEvents, node_id::NodeId, state_machine::State, IronCarrierError,
};

use super::{FileTransfer, Transfer, TransferRecv};

pub struct TransferBlocks {
    transfer: Transfer,
    transfer_chan: TransferRecv,
    file_handle: File,
    nodes_blocks: HashMap<NodeId, Vec<u64>>,
}

impl State for TransferBlocks {
    type Output = ();

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        while !self.nodes_blocks.is_empty() {
            self.transfer_blocks(shared_state).await?;
            self.wait_confirmation().await?;
        }
        Ok(())
    }
}

impl TransferBlocks {
    pub fn new(
        transfer: Transfer,
        transfer_chan: TransferRecv,
        file_handle: File,
        nodes_blocks: HashMap<NodeId, Vec<u64>>,
    ) -> Self {
        Self {
            transfer,
            transfer_chan,
            file_handle,
            nodes_blocks,
        }
    }

    async fn transfer_blocks(&mut self, shared_state: &crate::SharedState) -> crate::Result<()> {
        let mut block_nodes: BTreeMap<u64, Vec<NodeId>> = std::collections::BTreeMap::new();

        for (node, node_blocks) in self.nodes_blocks.iter() {
            for block in node_blocks {
                block_nodes.entry(*block).or_default().push(*node);
            }
        }

        let file_size = self.transfer.file.file_size()?;
        let mut block = vec![0u8; file_size as usize];
        for (block_index, nodes) in block_nodes.into_iter() {
            let position = block_index * self.transfer.block_size;
            let bytes_to_read = self.transfer.block_size.min(file_size - position);

            if self.file_handle.seek(SeekFrom::Start(position)).await? != position {
                return Err(IronCarrierError::IOReadingError.into());
            }

            self.file_handle
                .read_exact(&mut block[..bytes_to_read as usize])
                .await?;

            shared_state
                .connection_handler
                .stream_to(
                    self.transfer.transfer_id,
                    block_index,
                    &block[..bytes_to_read as usize],
                    nodes.iter(),
                )
                .await?;
        }

        shared_state
            .connection_handler
            .broadcast_to(
                NetworkEvents::FileTransfer(
                    self.transfer.transfer_id,
                    FileTransfer::TransferComplete,
                ),
                self.nodes_blocks.keys(),
            )
            .await
    }

    async fn wait_confirmation(&mut self) -> crate::Result<()> {
        let mut failed_transfers = 0;
        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransfer::TransferSucceeded | FileTransfer::RemovePeer => {
                    self.nodes_blocks.remove(&node_id);
                }
                FileTransfer::TransferFailed { received_blocks } => {
                    // FIXME: update required blocks and send again
                    failed_transfers += 1;
                }
                _ => {}
            }

            if self.nodes_blocks.len() == failed_transfers {
                break;
            }
        }

        Ok(())
    }
}
impl std::fmt::Debug for TransferBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferBlocks")
            .field("transfer", &self.transfer)
            .finish()
    }
}
