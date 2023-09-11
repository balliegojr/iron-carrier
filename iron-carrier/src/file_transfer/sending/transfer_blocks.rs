use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::SeekFrom,
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{
    file_transfer::{
        block_index::BlockIndexPosition,
        events::{TransferBlock, TransferComplete, TransferResult},
        Transfer,
    },
    node_id::NodeId,
    state_machine::State,
    IronCarrierError,
};

pub struct TransferBlocks {
    transfer: Transfer,
    file_handle: File,
    nodes_blocks: HashMap<NodeId, BTreeSet<BlockIndexPosition>>,
}

impl State for TransferBlocks {
    type Output = ();

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        while !self.nodes_blocks.is_empty() {
            self.transfer_blocks(shared_state).await?;
        }
        Ok(())
    }
}

impl TransferBlocks {
    pub fn new(
        transfer: Transfer,
        file_handle: File,
        nodes_blocks: HashMap<NodeId, BTreeSet<BlockIndexPosition>>,
    ) -> Self {
        Self {
            transfer,
            file_handle,
            nodes_blocks,
        }
    }

    async fn transfer_blocks(&mut self, shared_state: &crate::SharedState) -> crate::Result<()> {
        let mut block_nodes: BTreeMap<BlockIndexPosition, HashSet<NodeId>> =
            std::collections::BTreeMap::new();

        for (node, node_blocks) in self.nodes_blocks.iter() {
            for block in node_blocks {
                block_nodes.entry(*block).or_default().insert(*node);
            }
        }

        let file_size = self.transfer.file.file_size()?;
        let mut block = vec![0u8; file_size as usize];
        for (block_index, nodes) in block_nodes.into_iter() {
            let position = block_index.get_position(self.transfer.block_size);
            let bytes_to_read = self.transfer.block_size.min(file_size - position);

            if self.file_handle.seek(SeekFrom::Start(position)).await? != position {
                return Err(IronCarrierError::IOReadingError.into());
            }

            self.file_handle
                .read_exact(&mut block[..bytes_to_read as usize])
                .await?;

            // FIXME: remove nodes missing ack
            // FIXME: back to stream...
            shared_state
                .rpc
                .multi_call(
                    TransferBlock {
                        transfer_id: self.transfer.transfer_id,
                        block_index,
                        block: &block[..bytes_to_read as usize],
                    },
                    nodes,
                )
                .ack()
                .await?;
        }

        let results = shared_state
            .rpc
            .multi_call(
                TransferComplete {
                    transfer_id: self.transfer.transfer_id,
                },
                self.nodes_blocks.keys().cloned().collect(),
            )
            .result()
            .await?;

        for result in results {
            match result.data::<TransferResult>()? {
                TransferResult::Success => {
                    self.nodes_blocks.remove(&result.node_id());
                }
                TransferResult::Failed { required_blocks } => {
                    self.nodes_blocks
                        .entry(result.node_id())
                        .and_modify(|e| *e = required_blocks);
                }
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
