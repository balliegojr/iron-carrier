//! This consensus protocol is based on Raft.
//!
//! Here we are only interested in the leader election, the log replication process is not
//! implemented.
//! This protocol also expects absolute voting instead of majority
use std::{fmt::Display, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::{state_machine::StateStep, NetworkEvents, SharedState};

/// Possible states that a node can be
///
/// Only the first two are actually used, when a node becomes leader, it imediately transition to
/// FullSync state and request the same for the other followers, ending the election process
#[derive(Debug, PartialEq, Eq)]
enum NodeState {
    Candidate,
    Follower,
    Leader,
}

/// Possible election events
#[derive(Debug, Deserialize, Serialize)]
pub enum ElectionEvents {
    RequestVoteForTerm(u32),
    VoteOnTerm(u32, bool),
}

#[derive(Debug)]
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

#[async_trait::async_trait]
impl StateStep<SharedState> for Consensus {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
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

        let mut deadline = tokio::time::Instant::now() + Duration::from_millis(get_timeout());
        let mut term = ElectionTerm::default();

        shared_state
            .connection_handler
            .broadcast(NetworkEvents::RequestTransition(
                crate::Transition::Consensus,
            ))
            .await?;

        let events = shared_state.connection_handler.events_stream().await;
        tokio::pin!(events);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline), if self.election_state == NodeState::Candidate => {
                    term = term.next();
                    term.participants = shared_state
                        .connection_handler
                        .broadcast(NetworkEvents::ConsensusElection(
                            ElectionEvents::RequestVoteForTerm(term.term),
                        ))
                        .await?;


                    if term.participants == 0 {
                        return Ok(shared_state.default_state());
                    }

                    deadline = tokio::time::Instant::now() + Duration::from_millis(get_timeout());
                }
                Some((peer_id, network_event)) = events.next() => {
                    match network_event {
                        NetworkEvents::ConsensusElection(ev) => {
                            match ev {
                                ElectionEvents::RequestVoteForTerm(vote_term) if term.term < vote_term => {
                                    term.term = vote_term;
                                    self.election_state = NodeState::Follower;

                                    shared_state.connection_handler.send_to(peer_id, NetworkEvents::ConsensusElection(ElectionEvents::VoteOnTerm(vote_term, true))).await?;
                                }
                                ElectionEvents::RequestVoteForTerm(vote_term) => {
                                    shared_state.connection_handler.send_to(peer_id, NetworkEvents::ConsensusElection(ElectionEvents::VoteOnTerm(vote_term, false))).await?;
                                }
                                ElectionEvents::VoteOnTerm(vote_term, vote) if self.election_state == NodeState::Candidate && term.term == vote_term => {
                                    term.compute_vote(vote);

                                    if term.absolute_win() {
                                        log::debug!("Node wins election");
                                        self.election_state = NodeState::Leader;
                                        return Ok(Some(Box::new(crate::states::FullSyncLeader::new())))
                                    }
                                }
                                _ => {}
                            }
                        }
                        NetworkEvents::RequestTransition(crate::Transition::FullSync) => if self.election_state == NodeState::Follower {
                            return Ok(Some(Box::new(crate::states::FullSyncFollower::new(peer_id))))
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

fn get_timeout() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(100..250)
}
