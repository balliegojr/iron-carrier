use crate::{node_id::NodeId, state_machine::State};

use super::{SyncFollower, SyncLeader};

#[derive(Debug)]
pub struct FullSync {
    leader_node_id: NodeId,
}

impl FullSync {
    pub fn new(leader_node_id: NodeId) -> Self {
        Self { leader_node_id }
    }
}

impl State for FullSync {
    type Output = ();

    async fn execute(self, shared_state: &crate::SharedState) -> crate::Result<Self::Output> {
        if self.leader_node_id == shared_state.config.node_id_hashed {
            (SyncLeader::sync_everything()).execute(shared_state).await
        } else {
            SyncFollower::new(self.leader_node_id)
                .execute(shared_state)
                .await
        }
    }
}
