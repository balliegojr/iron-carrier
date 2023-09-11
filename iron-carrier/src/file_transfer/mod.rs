use std::collections::HashSet;

use crate::{node_id::NodeId, state_machine::State, storage::FileInfo, SharedState};

mod block_index;
pub use block_index::BlockIndexPosition;

mod events;
mod transfer;

mod sending;
pub use sending::*;

mod receiving;
pub use receiving::*;

pub use events::TransferFilesStart;
pub use transfer::{Transfer, TransferId};

use self::events::TransferFilesCompleted;

#[derive(Debug)]
pub struct TransferFiles {
    files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
    sync_leader_id: Option<NodeId>,
}

impl State for TransferFiles {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let leader_id = self.sync_leader_id;
        let wait_complete_from = match leader_id {
            Some(leader_id) => HashSet::from([leader_id]),
            None => shared_state.rpc.broadcast(TransferFilesStart).ack().await?,
        };

        let receive_task = tokio::spawn(receiving::receive_files(
            shared_state.clone(),
            wait_complete_from,
        ));

        if let Err(err) = sending::send_files(shared_state, self.files_to_send).await {
            log::error!("{err}")
        }
        match leader_id {
            Some(leader_id) => {
                let _ = shared_state
                    .rpc
                    .call(TransferFilesCompleted, leader_id)
                    .ack()
                    .await;

                if let Err(err) = receive_task.await {
                    log::error!("{err}")
                }
            }
            None => {
                if let Err(err) = receive_task.await {
                    log::error!("{err}")
                }
                let _ = shared_state
                    .rpc
                    .broadcast(TransferFilesCompleted)
                    .ack()
                    .await;
            }
        }

        Ok(())
    }
}

impl TransferFiles {
    pub fn new(
        sync_leader_id: Option<NodeId>,
        files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
    ) -> Self {
        Self {
            files_to_send,
            sync_leader_id,
        }
    }
}
