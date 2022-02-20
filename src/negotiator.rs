use std::{fmt::Display, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{conn::CommandDispatcher, sync::SyncEvent};

enum State {
    Idle,
    Passive,
    Candidate,
    Active,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Negotiation {
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

    pub fn handle_negotiation(&mut self, negotiation: Negotiation, peer: Option<String>) {
        match negotiation {
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
            Negotiation::Ask(round) => match self.state {
                State::Idle => {
                    self.state = State::Passive;
                    self.round = round;
                    self.commands
                        .to(Negotiation::Accept, peer.unwrap().as_str());
                }
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
                State::Active => {
                    self.commands
                        .to(Negotiation::Reject, peer.unwrap().as_str());
                }
            },
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
