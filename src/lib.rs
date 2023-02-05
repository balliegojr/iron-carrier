//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network

#![feature(hash_drain_filter)]
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
use transaction_log::TransactionLog;
use validation::Verified;

pub mod config;
use config::Config;

mod network_events;
mod stream;
// mod connection;

// mod events;
// use events::{CommandDispatcher, CommandHandler, Commands};

pub mod constants;
// mod debouncer;
mod hash_helper;

mod ignored_files;
// use ignored_files::IgnoredFiles;

pub mod leak;
use leak::Leak;

mod network;
mod storage;

mod file_transfer;
mod state_machine;
mod states;

// mod storage_hash_cache;
// use storage_hash_cache::StorageHashCache;

// mod sync;
// use sync::{FileTransferMan, FileWatcher, Synchronizer};

pub mod transaction_log;
// use transaction_log::TransactionLogWriter;

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

pub async fn run_full_sync(
    config: &'static validation::Verified<config::Config>,
) -> crate::Result<()> {
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
        .and_then(|_| states::Consensus::new())
        .and_then(FullSync::new)
        .execute(&shared_state)
        .await
}

pub async fn start_daemon(
    config: &'static validation::Verified<config::Config>,
) -> crate::Result<()> {
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
        .and_then(|_| states::Consensus::new())
        .and_then(FullSync::new)
        .then_loop(|| states::Daemon::new().and_then(|daemon_task| daemon_task))
        .execute(&shared_state)
        .await
}
