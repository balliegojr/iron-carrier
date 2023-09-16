//! This consensus protocol is based on Raft.
//!
//! Here we are only interested in the leader election, the log replication process is not
//! implemented.
//! This protocol also expects absolute voting instead of majority
use std::{fmt::Display, time::Duration};

use iron_carrier_macros::HashTypeId;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::{
    constants::MAX_ELECTION_TERMS,
    hash_type_id::HashTypeId,
    node_id::NodeId,
    state_machine::{State, StateMachineError},
    SharedState,
};

/// Possible states that a node can be
///
/// Only the first two are actually used, when a node becomes leader, it imediately transition to
/// FullSync state and request the same for the other followers, ending the election process
#[derive(Debug, PartialEq, Eq, Default)]
pub enum NodeState {
    #[default]
    Candidate,
    Follower,
    Leader,
}

/// Possible election events
#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ElectionEvents {
    RequestVoteForTerm(u32),
    VoteOnTerm(u32, bool),
}

#[derive(Debug, Default)]
pub struct Consensus {
    election_state: NodeState,
}

impl Display for Consensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Consensus")
    }
}

impl State for Consensus {
    type Output = NodeId;
    async fn execute(mut self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        // This election process repeats until a candidate becomes the leader
        //
        // After a timeout of 100-250ms, if the node is a candidate, it advances the current term
        // and request votes from every other node
        //
        // The node receives a request to vote for a term
        // If the current term for this node is lower than the voting term, it votes "yes" and
        // becomes a follower
        // If the current term is higher than the voting term, it votes "no"
        //
        // If the candidate receives "yes" votes from every other peer, it becomes a leader and
        // transition to the next state
        //
        // There are two scenarios that can abort an election
        // 1. If there are no participants. This can happen when the other nodes disconnects before
        //    the election completes
        // 2. After reaching a number of terms without reaching a consensus. If this happens it
        //    means other nodes are stuck at some invalid state or there is a bug in the consensus
        //    protocol

        let mut deadline = tokio::time::Instant::now() + Duration::from_millis(random_wait_time());
        let mut term = 0u32;

        let mut events = shared_state
            .rpc
            .subscribe_many(vec![
                StartConsensus::ID,
                RequestVote::ID,
                ConsensusReached::ID,
            ])
            .await?;

        tokio::spawn(shared_state.rpc.broadcast(StartConsensus).ack());

        let leader_id = loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline), if self.election_state == NodeState::Candidate => {
                    if term > MAX_ELECTION_TERMS {
                        log::error!("Election reached maximum term of {MAX_ELECTION_TERMS}");
                        Err(StateMachineError::Abort)?
                    }

                    term += 1;
                    let replies = shared_state
                        .rpc
                        .broadcast
                        (RequestVote { term })
                        .result()
                        .await?;

                    if replies.is_empty() {
                        log::error!("No participants in the consensus");
                        Err(StateMachineError::Abort)?
                    }

                    if replies.iter().all(|v| v.data::<TermVote>().map(|v| v.vote).unwrap_or_default()) {
                        log::debug!("Node wins election");
                        self.election_state = NodeState::Leader;
                        shared_state.rpc.broadcast(ConsensusReached).ack().await?;

                        break shared_state.config.node_id_hashed
                    }

                    deadline = tokio::time::Instant::now() + Duration::from_millis(random_wait_time());
                }

                request = events.next() => {
                    let request = request.ok_or(StateMachineError::Abort)?;
                    match request.type_id() {
                        RequestVote::ID => {
                            let data = request.data::<RequestVote>()?;
                            if term < data.term {
                                term = data.term;
                                self.election_state = NodeState::Follower;

                                request.reply(TermVote { vote: true }).await?;
                            } else {
                                request.reply(TermVote { vote: false }).await?;
                            }
                        }
                        ConsensusReached::ID => {
                            let leader = request.node_id();
                            request.ack().await?;
                            break leader;
                        }
                        StartConsensus::ID => {
                            let _ = request.ack().await;
                        }
                        _ => { unreachable!() }
                    }
                }
            }
        };

        events.free().await;
        Ok(leader_id)
    }
}

#[derive(Debug, Serialize, Deserialize, HashTypeId)]
pub struct StartConsensus;
#[derive(Debug, Serialize, Deserialize, HashTypeId)]
pub struct ConsensusReached;

#[derive(Debug, Serialize, Deserialize, HashTypeId)]
struct RequestVote {
    pub term: u32,
}

#[derive(Debug, Serialize, Deserialize, HashTypeId)]
struct TermVote {
    pub vote: bool,
}

fn random_wait_time() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(100..250)
}