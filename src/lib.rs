//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network

#![feature(hash_drain_filter)]
#![feature(btree_drain_filter)]
#![feature(drain_filter)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(is_sorted)]
#![feature(async_fn_in_trait)]

use network::ConnectionHandler;
use serde::{Deserialize, Serialize};
use state_machine::{StateComposer, Step};
use states::FullSync;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use transaction_log::TransactionLog;
use validation::Verified;

pub mod config;
use config::Config;

pub mod constants;
mod hash_helper;
mod ignored_files;
mod network_events;
pub mod relative_path;
mod stream;

pub mod leak;
use leak::Leak;

mod network;
mod storage;

mod file_transfer;
mod state_machine;
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
}

impl From<bincode::Error> for IronCarrierError {
    fn from(_: bincode::Error) -> Self {
        IronCarrierError::ParseNetworkMessage
    }
}

#[derive(Clone, Copy)]
pub struct SharedState {
    config: &'static Verified<Config>,
    connection_handler: &'static ConnectionHandler<network_events::NetworkEvents>,
    transaction_log: &'static TransactionLog,
}

// TODO: implement operation mode
// TODO: implement client mode if the daemon is running (basic a cli that shows sync status)
// TODO: add sync information to the transaction log (when it was last synched and what nodes
// participated

pub async fn run_full_sync(config: &'static validation::Verified<config::Config>) -> Result<()> {
    let connection_handler = network::ConnectionHandler::new(config).await?.leak();
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path)
        .await?
        .leak();

    let shared_state = SharedState {
        config,
        connection_handler,
        transaction_log,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| FullSync::new(leader_id, None))
        .execute(&shared_state)
        .await?;

    shared_state
        .connection_handler
        .close_all_connections()
        .await?;

    Ok(())
}

pub async fn start_daemon(
    config: &'static validation::Verified<config::Config>,
    when_sync_done: Option<Sender<()>>,
) -> Result<()> {
    log::trace!("My id is {}", config.node_id);

    let connection_handler = network::ConnectionHandler::new(config).await?.leak();
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path)
        .await?
        .leak();

    let shared_state = SharedState {
        config,
        connection_handler,
        transaction_log,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| FullSync::new(leader_id, when_sync_done.clone()))
        .then_loop(|| {
            states::Daemon::new(when_sync_done.clone()).and_then(|daemon_task| daemon_task)
        })
        .execute(&shared_state)
        .await?;

    Ok(())
}
