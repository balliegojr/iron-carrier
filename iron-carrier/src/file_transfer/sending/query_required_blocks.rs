use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
};

use tokio::fs::File;

use crate::{
    file_transfer::{
        block_index::{self, BlockIndexPosition, FullIndex},
        events::{self, RequiredBlocks, TransferType},
        Transfer,
    },
    node_id::NodeId,
    state_machine::State,
    SharedState, StateMachineError,
};

pub struct QueryRequiredBlocks {
    transfer: Transfer,
    transfer_types: HashMap<NodeId, TransferType>,
}

impl State for QueryRequiredBlocks {
    type Output = (
        Transfer,
        File,
        HashMap<NodeId, BTreeSet<BlockIndexPosition>>,
    );

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

        let full_index = self.get_block_index(&mut file_handle).await?;
        let mut required_blocks = if nodes_to_query.is_empty() {
            Default::default()
        } else {
            self.query_required_blocks(shared_state, &full_index, nodes_to_query)
                .await?
        };

        for (node_id, transfer_type) in self.transfer_types {
            match transfer_type {
                TransferType::FullFile => {
                    required_blocks.insert(node_id, full_index.to_partial());
                }
                TransferType::NoTransfer => {
                    required_blocks.insert(node_id, Default::default());
                }
                TransferType::Partial => {}
            }
        }

        Ok((self.transfer, file_handle, required_blocks))
    }
}

impl QueryRequiredBlocks {
    pub fn new(transfer: Transfer, transfer_types: HashMap<NodeId, TransferType>) -> Self {
        Self {
            transfer,
            transfer_types,
        }
    }

    fn is_any_transfer_required(&self) -> bool {
        self.transfer_types
            .values()
            .any(|t| !matches!(t, TransferType::NoTransfer))
    }

    async fn get_block_index(&self, file_handle: &mut File) -> crate::Result<FullIndex> {
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
        block_hashes: &FullIndex,
        nodes: HashSet<NodeId>,
    ) -> crate::Result<HashMap<NodeId, BTreeSet<BlockIndexPosition>>> {
        shared_state
            .rpc
            .multi_call(
                events::QueryRequiredBlocks {
                    transfer_id: self.transfer.transfer_id,
                    sender_block_index: block_hashes.clone(),
                },
                nodes,
            )
            .result()
            .await
            .and_then(|replies| {
                replies
                    .into_iter()
                    .map(|reply| {
                        reply.data().map(|required_blocks: RequiredBlocks| {
                            (reply.node_id(), required_blocks.required_blocks)
                        })
                    })
                    .collect()
            })
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
