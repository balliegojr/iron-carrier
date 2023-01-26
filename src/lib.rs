//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network

#![feature(hash_drain_filter)]
#![feature(drain_filter)]
#![feature(let_chains)]

use network::ConnectionHandler;
use serde::{Deserialize, Serialize};
use std::{fs::File, net::SocketAddr, time::Duration};
use thiserror::Error;
use validation::Verified;

pub mod config;
use config::Config;

mod network_events;
mod stream;
// mod connection;

// mod events;
// use events::{CommandDispatcher, CommandHandler, Commands};

pub mod constants;
mod debouncer;
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

mod transaction_log;
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
        IronCarrierError::ParseCommandError
    }
}

pub struct SharedState {
    daemon: bool,
    config: &'static Verified<Config>,
    connection_handler: &'static ConnectionHandler<network_events::NetworkEvents>,
}

// TODO: move the default state to a trait, so the state machine can handle this
impl SharedState {
    pub fn default_state(&self) -> Option<Box<dyn state_machine::StateStep<SharedState>>> {
        if self.daemon {
            Some(Box::new(states::Daemon::new()))
        } else {
            None
        }
    }
}

pub async fn run_full_sync(
    config: &'static validation::Verified<config::Config>,
) -> crate::Result<()> {
    let connection_handler = network::ConnectionHandler::new(config).await?.leak();

    let shared_state = SharedState {
        daemon: false,
        config,
        connection_handler,
    };
    let state_machine = state_machine::StateStepper::new(shared_state);
    state_machine
        .execute(Box::<states::DiscoverPeers>::default())
        .await;

    Ok(())
}

pub async fn start_daemon(
    config: &'static validation::Verified<config::Config>,
) -> crate::Result<()> {
    log::trace!("My id is {}", config.node_id);

    let connection_handler = network::ConnectionHandler::new(config).await?.leak();

    let shared_state = SharedState {
        daemon: true,
        config,
        connection_handler,
    };
    let state_machine = state_machine::StateStepper::new(shared_state);
    state_machine
        .execute(Box::<states::DiscoverPeers>::default())
        .await;

    Ok(())
    // let ignored_files = IgnoredFiles::new(config).leak();
    // let storage_state = storage_hash_cache::StorageHashCache::new(config, ignored_files).leak();
    //
    // let mut log_writer = transaction_log::TransactionLogWriter::new_from_path(&config.log_path)?;
    // let connection_man = get_command_handler_when_ready(config);
    // let file_watcher = get_file_watcher(
    //     config,
    //     connection_man.command_dispatcher(),
    //     log_writer.clone(),
    //     ignored_files,
    // );
    //
    // let mut file_transfer_man = FileTransferMan::new(
    //     connection_man.command_dispatcher(),
    //     config,
    //     log_writer.clone(),
    //     storage_state,
    // );
    //
    // let mut negotiator = Negotiator::new(connection_man.command_dispatcher());
    // let mut synchronizer = Synchronizer::new(
    //     config,
    //     connection_man.command_dispatcher(),
    //     storage_state,
    //     ignored_files,
    // )?;
    //
    // connection_man.on_command(move |command, peer_id| -> crate::Result<bool> {
    //     match command {
    //         Commands::Command(event) => {
    //             if matches!(event, sync::SyncEvent::StartSync) && peer_id.is_some() {
    //                 negotiator.set_passive();
    //             }
    //
    //             match peer_id {
    //                 Some(peer_id) => synchronizer.handle_network_event(event, &peer_id),
    //                 None => synchronizer.handle_signal(event),
    //             }
    //         }
    //
    //         Commands::Stream(data) => file_transfer_man.handle_stream(&data, &peer_id.unwrap()),
    //         Commands::FileHandler(event) => {
    //             file_transfer_man.handle_event(event, peer_id.as_deref())
    //         }
    //         Commands::Watcher(event) => match &file_watcher {
    //             Some(watcher) => watcher.handle_event(event),
    //             None => Ok(false),
    //         },
    //         Commands::Clear => {
    //             log_writer.compress_log()?;
    //             file_transfer_man.clear();
    //             storage_state.clear();
    //             synchronizer.clear();
    //             negotiator.clear();
    //
    //             // TODO: only clear ignored files when there is a change in the .ignore file
    //             ignored_files.clear();
    //
    //             Ok(false)
    //         }
    //         Commands::Negotiation(event) => {
    //             negotiator.handle_negotiation(event, peer_id);
    //             Ok(false)
    //         }
    //     }
    // })
}
//
// fn get_file_watcher(
//     config: &'static Config,
//     dispatcher: CommandDispatcher,
//     log_writer: TransactionLogWriter<File>,
//     ignored_files: &'static IgnoredFiles,
// ) -> Option<FileWatcher> {
//     if config.enable_file_watcher {
//         FileWatcher::new(dispatcher, config, log_writer, ignored_files).ok()
//     } else {
//         None
//     }
// }

// fn get_command_handler_when_ready(config: &'static Config) -> CommandHandler {
//     loop {
//         let connection_manager = CommandHandler::new(config);
//         match connection_manager {
//             Ok(connection_manager) => return connection_manager,
//             Err(err) => {
//                 log::error!("Error initializing connection {err}");
//                 std::thread::sleep(Duration::from_secs(1));
//             }
//         }
//     }
// }
