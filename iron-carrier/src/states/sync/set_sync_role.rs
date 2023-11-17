use crate::{
    node_id::NodeId,
    state_machine::{Result, State},
    sync_options::SyncOptions,
};

use super::{follower::Follower, leader::Leader};

#[derive(Debug)]
pub struct SetSyncRole {
    leader_node_id: NodeId,
    sync_options: SyncOptions,
}

impl SetSyncRole {
    pub fn new(leader_node_id: NodeId, sync_options: SyncOptions) -> Self {
        Self {
            leader_node_id,
            sync_options,
        }
    }
}

impl State for SetSyncRole {
    type Output = ();

    async fn execute(self, context: &crate::Context) -> Result<Self::Output> {
        if self.leader_node_id == context.config.node_id_hashed {
            (Leader::sync(self.sync_options)).execute(context).await
        } else {
            Follower::new(self.leader_node_id).execute(context).await
        }
    }
}
