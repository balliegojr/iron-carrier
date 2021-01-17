#![warn(missing_docs)]
#![warn(missing_doc_code_examples)]

//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt::Display};

pub mod config;
mod crypto;
mod deletion_tracker;
mod fs;
mod network;
pub mod sync;

/// Result<T, IronCarrierError> alias
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + 'static + Send + Sync>>;

/// Error types
#[derive(Debug, Serialize, Deserialize)]
pub enum IronCarrierError {
    /// Configuration file was not found
    ConfigFileNotFound,
    /// Configfuration file is not a valid yaml file  
    /// Or it contains invalid configuration
    ConfigFileIsInvalid(String),
    /// Peer Address is not correct  
    /// A valid ip:port string should be provided
    InvalidPeerAddress,
    /// Provided alias was not configurated for current peer
    AliasNotAvailable(String),
    /// It wasn't possible to read a file
    IOReadingError,
    /// It wasn't possible to write a file
    IOWritingError,
    /// It wasn't possible to start the server    
    ServerStartError(String),
    /// The target peer is disconnected
    PeerDisconectedError(String),
    /// It wasn't possible to read from network socket
    NetworkIOReadingError,
    /// It wans't possible to write to network socket
    NetworkIOWritingError,
    /// It wasn't possible to parse command frame
    ParseCommandError,
    /// It wasn't possible to parse the log file
    ParseLogError,
}

impl Display for IronCarrierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IronCarrierError::ConfigFileNotFound => {
                write!(f, "Configuration file not found on provided path")
            }
            IronCarrierError::ConfigFileIsInvalid(reason) => {
                write!(
                    f,
                    "Configuration file has invalid configuration, {}",
                    reason
                )
            }
            IronCarrierError::InvalidPeerAddress => {
                write!(f, "Invalid Peer Address")
            }
            IronCarrierError::AliasNotAvailable(alias) => {
                write!(f, "Alias {} not available on this node", alias)
            }
            IronCarrierError::IOReadingError => {
                write!(f, "There was an error reading information from disk")
            }
            IronCarrierError::IOWritingError => {
                write!(f, "There was an error writing information to disk")
            }
            IronCarrierError::ServerStartError(reason) => {
                write!(f, "There was an error starting the server: {}", reason)
            }
            IronCarrierError::NetworkIOReadingError => {
                write!(
                    f,
                    "There was an error reading information from network stream"
                )
            }
            IronCarrierError::NetworkIOWritingError => {
                write!(
                    f,
                    "There was an error writing information to network stream"
                )
            }
            IronCarrierError::ParseCommandError => {
                write!(f, "There was an error parsing the provide command")
            }
            IronCarrierError::PeerDisconectedError(peer_address) => {
                write!(f, "The target peer is not available: {}", peer_address)
            }
            IronCarrierError::ParseLogError => {
                write!(f, "There was an error parsing the log")
            }
        }
    }
}

impl Error for IronCarrierError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            _ => None,
        }
    }
}

impl From<bincode::Error> for IronCarrierError {
    fn from(_: bincode::Error) -> Self {
        IronCarrierError::ParseCommandError
    }
}

pub async fn run(config: config::Config) -> Result<()> {
    let mut s = sync::Synchronizer::new(config);
    s.start().await
}