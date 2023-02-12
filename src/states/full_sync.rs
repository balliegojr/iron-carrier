use crate::state_machine::Step;
use tokio::sync::mpsc::Sender;

use super::{FullSyncFollower, FullSyncLeader};

#[derive(Debug)]
pub struct FullSync {
    leader_node_id: u64,
    when_sync_done: Option<Sender<()>>,
}

impl FullSync {
    pub fn new(leader_node_id: u64, when_sync_done: Option<Sender<()>>) -> Self {
        Self {
            leader_node_id,
            when_sync_done,
        }
    }
}

impl Step for FullSync {
    type Output = ();

    async fn execute(
        self,
        shared_state: &crate::SharedState,
    ) -> crate::Result<Option<Self::Output>> {
        if self.leader_node_id == shared_state.config.node_id_hashed {
            (FullSyncLeader::sync_everything(self.when_sync_done))
                .execute(shared_state)
                .await
        } else {
            FullSyncFollower::new(self.leader_node_id)
                .execute(shared_state)
                .await
        }
    }
}
