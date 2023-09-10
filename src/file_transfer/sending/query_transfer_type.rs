use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use crate::{
    file_transfer::{
        events::{QueryTransferType, TransferType},
        Transfer,
    },
    node_id::NodeId,
    state_machine::State,
};

pub struct QueryTransfer {
    transfer: Transfer,
    nodes: HashSet<NodeId>,
}

impl QueryTransfer {
    pub fn new(transfer: Transfer, nodes: HashSet<NodeId>) -> Self {
        Self { transfer, nodes }
    }
}

impl State for QueryTransfer {
    type Output = (Transfer, HashMap<NodeId, TransferType>);

    async fn execute(self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        //FIXME: remove the NoTransfer nodes
        shared_state
            .rpc
            .multi_call(
                QueryTransferType {
                    // transfer_id: self.transfer.transfer_id,
                    file: self.transfer.file.clone(),
                },
                self.nodes,
            )
            .result()
            .await
            .and_then(|replies| {
                replies
                    .into_iter()
                    .map(|reply| {
                        reply
                            .data()
                            .map(|transfer_type: TransferType| (reply.node_id(), transfer_type))
                    })
                    .collect()
            })
            .map(|replies| (self.transfer, replies))
    }
}

impl Debug for QueryTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryTransfer")
            .field("transfer", &self.transfer)
            .field("nodes", &self.nodes)
            .finish()
    }
}
