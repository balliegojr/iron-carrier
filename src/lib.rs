//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network

use std::{net::SocketAddr, sync::Arc};

use config::Config;
use conn::{CommandDispatcher, CommandType, ConnectionManager};
use serde::{Deserialize, Serialize};
use sync::{FileTransferMan, FileWatcher, Synchronizer};

use thiserror::Error;

pub mod config;
mod conn;
mod fs;
mod transaction_log;
// mod network;
mod hash_helper;
mod sync;

/// Result<T, IronCarrierError> alias
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + 'static + Send + Sync>>;

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
    /// It wasn't possible to write a file
    #[error("There was an error writing information to disk")]
    IOWritingError,
    /// It wasn't possible to start the server    
    #[error("There was an error starting the server: {0}")]
    ServerStartError(String),
    /// The target peer is disconnected
    #[error("The target peer is not available: {0}")]
    PeerDisconectedError(SocketAddr),
    /// It wasn't possible to read from network socket
    #[error("There was an error reading information from network stream")]
    NetworkIOReadingError,
    /// It wans't possible to write to network socket
    #[error("There was an error writing information to network stream")]
    NetworkIOWritingError,
    /// It wasn't possible to parse command frame
    #[error("There was an error parsing the provide command")]
    ParseCommandError,
    /// It wasn't possible to parse the log file
    #[error("There was an error parsing the log")]
    ParseLogError,

    #[error("Invalid state change: {0}")]
    InvalidStateChange(String),

    #[error("Received an invalid message")]
    InvalidMessage,
}

impl From<bincode::Error> for IronCarrierError {
    fn from(_: bincode::Error) -> Self {
        IronCarrierError::ParseCommandError
    }
}
pub fn run(config: config::Config) -> crate::Result<()> {
    log::trace!("My id is {}", config.node_id);

    let config = Arc::new(config);
    let connection_man = ConnectionManager::new(config.clone());
    let _file_watcher = get_file_watcher(config.clone(), connection_man.command_dispatcher());
    let mut file_transfer_man =
        FileTransferMan::new(connection_man.command_dispatcher(), config.clone());

    let mut synchronizer = Synchronizer::new(config, connection_man.command_dispatcher())?;

    connection_man.on_command(move |command, peer_id| -> crate::Result<()> {
        match command {
            CommandType::Command(event) => match peer_id {
                Some(peer_id) => synchronizer.handle_network_event(event, peer_id),
                None => synchronizer.handle_signal(event),
            },

            CommandType::Stream(data) => file_transfer_man.handle_stream(&data, peer_id.unwrap()),
            CommandType::FileHandler(event) => file_transfer_man.handle_event(event),
        }
    })
}

fn get_file_watcher(config: Arc<Config>, dispatcher: CommandDispatcher) -> Option<FileWatcher> {
    if config.enable_file_watcher {
        FileWatcher::new(dispatcher, config).ok()
    } else {
        None
    }
}
