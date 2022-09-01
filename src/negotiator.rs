//! Implements leader election, loosely based on Raft protocol

use std::{fmt::Display, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{events::CommandDispatcher, sync::SyncEvent};

/// Possible states for this node
enum State {
    /// No negotation ever happened for this node
    Idle,
    /// This node is voting for another peer
    Passive,
    /// This node is a candidate in the election
    Candidate,
    /// This node is the winner of the election
    Active,
}

/// Possible commands for each election round
#[derive(Debug, Deserialize, Serialize)]
pub enum Negotiation {
    /// Initiates an election round
    Start,
    Ask(u32),
    Accept,
    Reject,
}

impl Display for Negotiation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Negotiation::Start => write!(f, "start"),
            Negotiation::Ask(round) => write!(f, "ask {round}"),
            Negotiation::Accept => write!(f, "accept"),
            Negotiation::Reject => write!(f, "reject"),
        }
    }
}

/// Raft based leader election.
///
/// The leader election is used to choose a peer to start the synchronization, in case multiple peers try to start at the same time
pub struct Negotiator {
    commands: CommandDispatcher,
    round: u32,
    state: State,

    peers: usize,
    accepted: usize,
    rejected: usize,
}

impl Negotiator {
    pub fn new(commands: CommandDispatcher) -> Self {
        Self {
            commands,
            state: State::Idle,
            round: 0,
            accepted: 0,
            rejected: 0,
            peers: 0,
        }
    }
    pub fn clear(&mut self) {
        self.state = State::Idle;
        self.round = 0;
        self.accepted = 0;
        self.rejected = 0;
        self.peers = 0;
    }

    /// Handle negotiation events
    pub fn handle_negotiation(&mut self, negotiation: Negotiation, peer: Option<String>) {
        match negotiation {
            // When a round starts, if the node is Idle or Candidate, it votes for himself and broadcast `ask` requesting for votes
            Negotiation::Start => match self.state {
                State::Idle | State::Candidate => {
                    self.state = State::Candidate;
                    self.round += 1;
                    self.accepted = 0;
                    self.rejected = 0;
                    self.peers = self.commands.broadcast(Negotiation::Ask(self.round));
                }
                State::Passive | State::Active => {}
            },
            // Node is requested to vote on the current round
            Negotiation::Ask(round) => match self.state {
                // When the node is idle, it becomes passive and vote for the candidate
                State::Idle => {
                    self.state = State::Passive;
                    self.round = round;
                    self.commands
                        .to(Negotiation::Accept, peer.unwrap().as_str());
                }
                // When the node is passive or a candidate,
                // if the received round is higher then the nodes current round, it votes for the candidate
                State::Passive | State::Candidate => {
                    if round > self.round {
                        self.state = State::Passive;
                        self.round = round;
                        self.commands
                            .to(Negotiation::Accept, peer.unwrap().as_str());
                    } else {
                        self.commands
                            .to(Negotiation::Reject, peer.unwrap().as_str());
                    }
                }
                // When the node is active, it won't participate in the election round
                // this is not part of the raft protocol, it is done here to avoid multiple synchronization in parallel
                State::Active => {
                    self.commands
                        .to(Negotiation::Reject, peer.unwrap().as_str());
                }
            },
            // Accept and Reject just increase the number of votes received and verify round results
            Negotiation::Accept => {
                self.accepted += 1;
                if self.accepted + self.rejected == self.peers {
                    self.round_results();
                }
            }
            Negotiation::Reject => {
                self.rejected += 1;
                if self.accepted + self.rejected == self.peers {
                    self.round_results();
                }
            }
        }
    }

    /// Wrap round results. if the node was unanimously voted for, it becomes active and starts the synchronization.  
    /// Otherwise it starts the next round
    fn round_results(&mut self) {
        if self.accepted == self.peers {
            if !matches!(self.state, State::Active) {
                self.state = State::Active;
                self.commands.now(SyncEvent::StartSync);
            }
        } else if self.accepted + self.rejected == self.peers {
            self.next_round();
        }
    }

    fn next_round(&self) {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(100..250);

        self.commands
            .after(Negotiation::Start, Duration::from_millis(timeout));
    }

    pub fn set_passive(&mut self) {
        self.state = State::Passive;
    }
}
