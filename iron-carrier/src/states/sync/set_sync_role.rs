use crate::{
    state_machine::{Result, State},
    states::consensus::ConsensusResult,
    sync_options::SyncOptions,
};

use super::{follower::Follower, leader::Leader};

#[derive(Debug)]
pub struct SetSyncRole {
    consensus_result: ConsensusResult,
    sync_options: Option<SyncOptions>,
}

impl SetSyncRole {
    pub fn new(consensus_result: ConsensusResult, sync_options: Option<SyncOptions>) -> Self {
        Self {
            consensus_result,
            sync_options,
        }
    }
}

impl State for SetSyncRole {
    type Output = ();

    async fn execute(self, context: &crate::Context) -> Result<Self::Output> {
        if self.consensus_result.leader == context.config.node_id_hashed {
            (Leader::sync(
                self.sync_options,
                self.consensus_result.participants.unwrap_or_default(),
            ))
            .execute(context)
            .await
        } else {
            Follower::new(self.consensus_result.leader)
                .execute(context)
                .await
        }
    }
}
