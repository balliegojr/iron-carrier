//! This consensus protocol is based on Raft.
//!
//! Here we are only interested in the leader election, the log replication process is not
//! implemented.
//! This protocol also expects absolute voting instead of majority
use std::{fmt::Display, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    constants::MAX_ELECTION_TERMS,
    network_events::{self, NetworkEvents},
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

impl Consensus {
    pub fn new() -> Self {
        Self {
            election_state: NodeState::Candidate,
        }
    }
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
        let mut term = ElectionTerm::default();

        shared_state
            .connection_handler
            .broadcast(network_events::Transition::Consensus.into())
            .await?;

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline), if self.election_state == NodeState::Candidate => {
                    if term.term > MAX_ELECTION_TERMS {
                        log::error!("Election reached maximum term of {MAX_ELECTION_TERMS}");
                        Err(StateMachineError::Abort)?
                    }

                    term = term.next();
                    term.participants = shared_state
                        .connection_handler
                        .broadcast(
                            ElectionEvents::RequestVoteForTerm(term.term).into(),
                        )
                        .await?;

                    if term.participants == 0 {
                        log::error!("No participants in the consensus");
                        Err(StateMachineError::Abort)?
                    }

                    log::debug!("Requested vote for {}", term.participants);

                    deadline = tokio::time::Instant::now() + Duration::from_millis(random_wait_time());
                }
                Some((node_id, network_event)) = shared_state.connection_handler.next_event() => {
                    match network_event {
                        NetworkEvents::ConsensusElection(ev) => {
                            match ev {
                                ElectionEvents::RequestVoteForTerm(vote_term) if term.term < vote_term => {
                                    term.term = vote_term;
                                    self.election_state = NodeState::Follower;

                                    shared_state.connection_handler.send_to(ElectionEvents::VoteOnTerm(vote_term, true).into(), node_id).await?;
                                }
                                ElectionEvents::RequestVoteForTerm(vote_term) => {
                                    shared_state.connection_handler.send_to(ElectionEvents::VoteOnTerm(vote_term, false).into(), node_id).await?;
                                }
                                ElectionEvents::VoteOnTerm(vote_term, vote) if self.election_state == NodeState::Candidate && term.term == vote_term => {
                                    term.compute_vote(vote);

                                    if term.absolute_win() {
                                        log::debug!("Node wins election");
                                        self.election_state = NodeState::Leader;

                                        return Ok(shared_state.config.node_id_hashed)
                                    }
                                }
                                _ => {}
                            }
                        }
                        NetworkEvents::RequestTransition(network_events::Transition::FullSync) => if self.election_state == NodeState::Follower {
                            return Ok(node_id)
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct ElectionTerm {
    term: u32,
    participants: usize,
    voted_yes: usize,
    voted_no: usize,
}

impl ElectionTerm {
    pub fn next(self) -> Self {
        Self {
            term: self.term + 1,
            ..Default::default()
        }
    }

    fn compute_vote(&mut self, vote: bool) {
        if vote {
            self.voted_yes += 1;
        } else {
            self.voted_no += 1;
        }
    }

    fn absolute_win(&self) -> bool {
        self.voted_yes == self.participants
    }
}

fn random_wait_time() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(100..250)
}
