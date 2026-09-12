//! This consensus protocol is based on Raft.
//!
//! Here we are only interested in the leader election, the log replication process is not
//! implemented.
//! This protocol also expects absolute voting instead of majority
use std::{collections::HashSet, fmt::Display, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::{
    Context,
    constants::MAX_ELECTION_TERMS,
    node_id::NodeId,
    protocol::Protocol,
    state_machine::{Result, State, StateMachineError},
};

/// Possible states that a node can be
///
/// Only the first two are actually used, when a node becomes leader, it imediately transition to
/// FullSync state and request the same for the other followers, ending the election process
#[derive(Debug, PartialEq, Eq, Default)]
pub enum NodeState {
    #[default]
    Init,
    Candidate,
    Follower,
    Leader,
}

#[derive(Debug)]
pub struct Consensus {
    election_state: NodeState,
    participants: HashSet<NodeId>,
}

#[derive(Debug)]
pub struct ConsensusResult {
    pub participants: HashSet<NodeId>,
}

impl Consensus {
    pub fn new(participants: HashSet<NodeId>) -> Self {
        Self {
            election_state: Default::default(),
            participants,
        }
    }
}

impl Display for Consensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Consensus")
    }
}

type MaybeFuture<T> = Option<std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>>;

impl State for Consensus {
    type Output = Option<ConsensusResult>;
    async fn execute(mut self, context: &Context) -> Result<Self::Output> {
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
        let mut events = context
            .rpc
            .subscribe([
                StartConsensus::MESSAGE_TYPE,
                RequestVote::MESSAGE_TYPE,
                ConsensusReached::MESSAGE_TYPE,
            ])
            .await?;

        let mut replies_fut: MaybeFuture<
            anyhow::Result<crate::network::rpc::GroupCallResponse<TermVote>>,
        > = None;

        let mut init_fut: MaybeFuture<anyhow::Result<HashSet<NodeId>>> = Some(Box::pin(
            context
                .rpc
                .multi_call(StartConsensus, self.participants.clone())
                .timeout(Duration::from_secs(60))
                .ack(),
        ));

        let mut term = 0u32;
        let result = loop {
            tokio::select! {
                _participant_nodes = async { init_fut.as_mut().unwrap().await }, if init_fut.is_some() => {
                    init_fut = None;
                    self.election_state = NodeState::Candidate;
                }
                _ = tokio::time::sleep_until(deadline), if replies_fut.is_none() && self.election_state == NodeState::Candidate => {
                    if term > MAX_ELECTION_TERMS {
                        log::error!("Election reached maximum term of {MAX_ELECTION_TERMS}");
                        Err(StateMachineError::Abort)?
                    }

                    term += 1;
                    replies_fut = Some(Box::pin(context
                        .rpc
                        .broadcast
                        (RequestVote { term })
                        .timeout(Duration::from_secs(1))
                        .result()));

                }

                response = async { replies_fut.as_mut().unwrap().await }, if replies_fut.is_some() => {
                    replies_fut = None;

                    match response {
                        Ok(response) => {
                            let replies =  response.replies();

                            if replies.is_empty() {
                                log::error!("No participants in the consensus");
                                Err(StateMachineError::Abort)?
                            }

                            if replies.iter().all(|v| v.data().map(|v| v.vote).unwrap_or_default()) {
                                log::debug!("Node wins election");
                                self.election_state = NodeState::Leader;
                                let nodes: HashSet<NodeId> = replies.into_iter().map(|r| r.node_id()).collect();
                                context.rpc.multi_call(ConsensusReached, nodes.clone())
                                    .timeout(Duration::from_secs(60))
                                    .ack().await?;

                                break Some(ConsensusResult {  participants: nodes })
                            }
                        }
                        Err(err) => {
                            log::error!("Error getting consensus response {err}");
                        }
                    }

                    deadline = tokio::time::Instant::now() + Duration::from_millis(random_wait_time());
                }

                request = events.next() => {
                    let request = request.ok_or(StateMachineError::Abort)?;
                    match request.message_type()? {
                        RequestVote::MESSAGE_TYPE => {
                            let event = request.into_event::<RequestVote>();
                            let data = event.data()?;
                            if term < data.term {
                                term = data.term;
                                self.election_state = NodeState::Follower;

                                event.reply(TermVote { vote: true }).await?;
                            } else {
                                event.reply(TermVote { vote: false }).await?;
                            }
                        }
                        ConsensusReached::MESSAGE_TYPE => {
                            log::info!("Node {} won election", request.node_id());
                            request.into_event::<ConsensusReached>().ack().await?;
                            break None;
                        }
                        StartConsensus::MESSAGE_TYPE => {
                            let _ = request.into_event::<StartConsensus>().ack().await;
                        }
                        _ => { unreachable!() }
                    }
                }
            }
        };

        Ok(result)
    }
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct StartConsensus;

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct ConsensusReached;

#[derive(Debug, Serialize, Deserialize, Protocol)]
#[protocol(response = TermVote)]
pub struct RequestVote {
    pub term: u32,
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct TermVote {
    pub vote: bool,
}

fn random_wait_time() -> u64 {
    let mut rng = rand::rng();
    rng.random_range(100..250)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ensure_can_reach_consensus() -> anyhow::Result<()> {
        let [one, two, three] = crate::context::local_contexts().await;
        let n_one = one.config.node_id_hashed;
        let n_two = two.config.node_id_hashed;
        let n_three = three.config.node_id_hashed;

        let (r_one, r_two, r_three) = tokio::join!(
            tokio::spawn(
                async move { Consensus::new([n_two, n_three].into(),).execute(&one).await }
            ),
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Consensus::new([n_one, n_three].into()).execute(&two).await
            }),
            tokio::spawn(
                async move { Consensus::new([n_one, n_two].into()).execute(&three).await }
            ),
        );

        let results = [r_one??, r_two??, r_three??];
        assert_eq!(2, results.iter().filter(|r| r.is_none()).count());

        let leader = results.into_iter().flatten().next().unwrap();
        assert_eq!(2, leader.participants.len());

        Ok(())
    }
}
