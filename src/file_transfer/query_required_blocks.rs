use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use tokio::fs::File;

use crate::{
    file_transfer::FileTransfer, network_events::NetworkEvents, node_id::NodeId,
    state_machine::State, SharedState, StateMachineError,
};

use super::{block_index, Transfer, TransferRecv, TransferType};

pub struct QueryRequiredBlocks {
    transfer: Transfer,
    transfer_chan: TransferRecv,
    transfer_types: HashMap<NodeId, TransferType>,
}

impl State for QueryRequiredBlocks {
    type Output = (Transfer, TransferRecv, File, HashMap<NodeId, Vec<u64>>);

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        if !self.is_any_transfer_required() {
            Err(StateMachineError::Abort)?
        }

        let nodes_to_query = self.get_nodes_with_partial_transfer();
        let mut file_handle = crate::storage::file_operations::open_file_for_reading(
            shared_state.config,
            &self.transfer.file,
        )
        .await?;

        let block_hashes = self.get_block_index(&mut file_handle).await?;
        let mut required_blocks = if nodes_to_query.is_empty() {
            Default::default()
        } else {
            self.query_required_blocks(shared_state, &block_hashes, nodes_to_query)
                .await?
        };

        for (node_id, transfer_type) in self.transfer_types {
            match transfer_type {
                TransferType::FullFile => {
                    required_blocks.insert(node_id, (0..block_hashes.len() as u64).collect());
                }
                TransferType::NoTransfer => {
                    required_blocks.insert(node_id, Vec::new());
                }
                TransferType::Partial => {}
            }
        }

        Ok((
            self.transfer,
            self.transfer_chan,
            file_handle,
            required_blocks,
        ))
    }
}

impl QueryRequiredBlocks {
    pub fn new(
        transfer: Transfer,
        transfer_chan: TransferRecv,
        transfer_types: HashMap<NodeId, TransferType>,
    ) -> Self {
        Self {
            transfer,
            transfer_chan,
            transfer_types,
        }
    }

    fn is_any_transfer_required(&self) -> bool {
        self.transfer_types
            .values()
            .any(|t| !matches!(t, TransferType::NoTransfer))
    }

    async fn get_block_index(&self, file_handle: &mut File) -> crate::Result<Vec<u64>> {
        block_index::get_file_block_index(
            file_handle,
            self.transfer.block_size,
            self.transfer.file.file_size()?,
            file_handle.metadata().await?.len(),
        )
        .await
    }

    fn get_nodes_with_partial_transfer(&self) -> HashSet<NodeId> {
        self.transfer_types
            .iter()
            .filter_map(|(node, transfer_type)| {
                if matches!(transfer_type, TransferType::Partial) {
                    Some(*node)
                } else {
                    None
                }
            })
            .collect()
    }

    async fn query_required_blocks(
        &mut self,
        shared_state: &SharedState,
        block_hashes: &[u64],
        mut nodes: HashSet<NodeId>,
    ) -> crate::Result<HashMap<NodeId, Vec<u64>>> {
        shared_state
            .connection_handler
            .broadcast_to(
                NetworkEvents::FileTransfer(
                    self.transfer.transfer_id,
                    FileTransfer::QueryRequiredBlocks {
                        sender_block_index: block_hashes.to_vec(),
                    },
                ),
                nodes.iter(),
            )
            .await?;

        let mut replies = HashMap::with_capacity(nodes.len());
        while let Some((node_id, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransfer::ReplyRequiredBlocks { required_blocks } => {
                    replies.insert(node_id, required_blocks);
                }
                FileTransfer::RemovePeer => {
                    replies.remove(&node_id);
                    nodes.remove(&node_id);
                    self.transfer_types.remove(&node_id);
                }
                _ => {
                    log::error!("Received unexpected event: {ev:?}");
                }
            }

            if replies.len() == nodes.len() {
                return Ok(replies);
            }
        }

        Err(StateMachineError::Abort)?
    }
}

impl Debug for QueryRequiredBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRequiredBlocks")
            .field("transfer", &self.transfer)
            .field("transfer_types", &self.transfer_types)
            .finish()
    }
}
