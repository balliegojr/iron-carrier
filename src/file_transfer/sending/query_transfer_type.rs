use std::collections::{HashMap, HashSet};

use crate::{
    file_transfer::{FileTransferEvent, Transfer, TransferRecv, TransferType},
    network_events::NetworkEvents,
    node_id::NodeId,
    state_machine::State,
    StateMachineError,
};

#[derive(Debug)]
pub struct QueryTransfer {
    transfer: Transfer,
    nodes: HashSet<NodeId>,
    transfer_chan: TransferRecv,
}

impl QueryTransfer {
    pub fn new(transfer: Transfer, nodes: HashSet<NodeId>, transfer_chan: TransferRecv) -> Self {
        Self {
            transfer,
            nodes,
            transfer_chan,
        }
    }
}

impl State for QueryTransfer {
    type Output = (Transfer, TransferRecv, HashMap<NodeId, TransferType>);

    async fn execute(mut self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        shared_state
            .connection_handler
            .broadcast_to(
                NetworkEvents::FileTransfer(
                    self.transfer.transfer_id,
                    FileTransferEvent::QueryTransferType {
                        file: self.transfer.file.clone(),
                    },
                ),
                self.nodes.iter(),
            )
            .await?;

        let mut transfer_types = HashMap::with_capacity(self.nodes.len());
        while let Some((node, ev)) = self.transfer_chan.recv().await {
            match ev {
                FileTransferEvent::ReplyTransferType { transfer_type } => {
                    transfer_types.insert(node, transfer_type);
                }
                FileTransferEvent::RemovePeer => {
                    self.nodes.remove(&node);
                    transfer_types.remove(&node);
                }
                _ => {
                    log::error!("Received unexpected event: {ev:?}");
                    continue;
                }
            }

            log::trace!("{:?}, {:?}", self.nodes, transfer_types);

            if transfer_types.len() == self.nodes.len() {
                return Ok((self.transfer, self.transfer_chan, transfer_types));
            }
        }

        Err(StateMachineError::Abort)?
    }
}
