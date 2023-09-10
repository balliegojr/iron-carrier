//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network
#![allow(incomplete_features)]
#![feature(hash_extract_if)]
#![feature(btree_extract_if)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(is_sorted)]
#![feature(async_fn_in_trait)]

use network::{rpc::RPCHandler, ConnectionHandler};
use node_id::NodeId;
use serde::{Deserialize, Serialize};
use state_machine::{State, StateComposer};
use states::SetSyncRole;
use sync_options::SyncOptions;
use thiserror::Error;
use tokio::sync::{mpsc::Sender, oneshot::Receiver};
use transaction_log::TransactionLog;
use validation::Validated;

pub mod config;
use config::Config;

pub mod constants;
mod hash_helper;
mod ignored_files;
mod node_id;
pub mod relative_path;
mod stream;
mod sync_options;
mod time;

pub mod leak;

mod network;
mod storage;

mod file_transfer;
mod state_machine;
pub use state_machine::StateMachineError;
mod states;

pub mod transaction_log;
pub mod validation;

/// Result<T, IronCarrierError> alias
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Error types
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum IronCarrierError {
    /// Configuration file was not found
    #[error("Configuration file not found on provided path")]
    ConfigFileNotFound,
    /// Configfuration file is not a valid yaml file  
    /// Or it contains invalid configuration
    #[error("Configuration file has invalid configuration, {0}")]
    ConfigFileIsInvalid(String),
    /// Peer Address is not correct  
    /// A valid ip:port string should be provided
    #[error("Invalid Peer Address")]
    InvalidPeerAddress,
    /// Provided alias was not configurated for current peer
    #[error("Storage {0} not available on this node")]
    StorageNotAvailable(String),
    /// It wasn't possible to read a file
    #[error("There was an error reading information from disk")]
    IOReadingError,

    #[error("There was an error parsing the network message")]
    ParseNetworkMessage,

    #[error("Received an invalid operation")]
    InvalidOperation,

    #[error("Failed to find file")]
    FileNotFound,

    #[error("Attempt to connect with a node that has a different version")]
    VersionMismatch,

    #[error("Attempt to connect with a node of a different group")]
    GroupMismatch,

    #[error("Node Connection Timeout")]
    ConnectionTimeout,

    #[error("Node {0} is not connected")]
    UnknownNode(NodeId),

    #[error("Reply for the message did not arrive in time")]
    ReplyTimeOut,

    #[error("Received an invalid reply")]
    InvalidReply,
}

impl From<bincode::Error> for IronCarrierError {
    fn from(_: bincode::Error) -> Self {
        IronCarrierError::ParseNetworkMessage
    }
}

#[derive(Clone)]
pub struct SharedState {
    config: &'static Validated<Config>,
    connection_handler: ConnectionHandler,
    rpc: RPCHandler,
    transaction_log: TransactionLog,
    when_done: Option<Sender<()>>,
}

// TODO: implement client mode if the daemon is running (basic a cli that shows sync status)
// TODO: add sync information to the transaction log (when it was last synched and what nodes
// participated

pub async fn run_full_sync(config: &'static validation::Validated<config::Config>) -> Result<()> {
    let (connection_handler, rpc) = network::start_network_service(config).await?;
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path).await?;

    let shared_state = SharedState {
        config,
        connection_handler,
        rpc,
        transaction_log,
        when_done: None,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| SetSyncRole::new(leader_id, SyncOptions::default()))
        .execute(&shared_state)
        .await?;

    Ok(())
}

pub async fn start_daemon(
    config: &'static validation::Validated<config::Config>,
    when_done: Option<Sender<()>>,
) -> Result<()> {
    log::trace!("My id is {}", config.node_id);

    let (connection_handler, rpc) = network::start_network_service(config).await?;
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path).await?;

    let shared_state = SharedState {
        config,
        connection_handler,
        rpc,
        transaction_log,
        when_done,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| SetSyncRole::new(leader_id, SyncOptions::default()))
        .then_default_to(states::Daemon::default)
        .execute(&shared_state)
        .await?;

    Ok(())
}
