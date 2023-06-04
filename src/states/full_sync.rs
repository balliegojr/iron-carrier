use crate::{node_id::NodeId, state_machine::State, sync_options::SyncOptions};

use super::{SyncFollower, SyncLeader};

#[derive(Debug)]
pub struct FullSync {
    leader_node_id: NodeId,
    sync_options: SyncOptions,
}

impl FullSync {
    pub fn new(leader_node_id: NodeId, sync_options: SyncOptions) -> Self {
        Self {
            leader_node_id,
            sync_options,
        }
    }
}

impl State for FullSync {
    type Output = ();

    async fn execute(self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        if self.leader_node_id == shared_state.config.node_id_hashed {
            (SyncLeader::sync(self.sync_options))
                .execute(shared_state)
                .await
        } else {
            SyncFollower::new(self.leader_node_id)
                .execute(shared_state)
                .await
        }
    }
}
