use crate::{
    state_machine::{Result, State},
    states::consensus::ConsensusResult,
    sync_options::SyncOptions,
};

use super::{follower::Follower, leader::Leader};

#[derive(Debug)]
pub struct SetSyncRole {
    consensus_result: Option<ConsensusResult>,
    sync_options: Option<SyncOptions>,
}

impl SetSyncRole {
    pub fn new(
        consensus_result: Option<ConsensusResult>,
        sync_options: Option<SyncOptions>,
    ) -> Self {
        Self {
            consensus_result,
            sync_options,
        }
    }
}

impl State for SetSyncRole {
    type Output = ();

    async fn execute(self, context: &crate::Context) -> Result<Self::Output> {
        if let Some(consensus_result) = self.consensus_result {
            (Leader::sync(self.sync_options, consensus_result.participants))
                .execute(context)
                .await
        } else {
            Follower.execute(context).await
        }
    }
}
