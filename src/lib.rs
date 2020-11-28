use std::{error::Error, fmt::Display};
use serde::{Serialize, Deserialize };

pub mod config;
mod fs;
mod crypto;
mod network;
pub mod sync;


#[derive(Debug, Serialize, Deserialize)]
pub enum RSyncError {
    InvalidConfigPath,
    InvalidConfigFile,
    InvalidAlias(String),
    InvalidPeerAddress,
    ErrorReadingLocalFiles,
    ErrorFetchingPeerFiles,
    CantStartServer(String),
    CantConnectToPeer(String),
    CantCommunicateWithPeer(String),
    ErrorSendingFile,
    ErrorWritingFile(String),
    ErrorParsingCommands,
    ErrorReadingFile(String),
    ErrorRemovingFile(String),
    ErrorMovingFile(String)
}

impl Display for RSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RSyncError::InvalidConfigPath => { write!(f, "Configuration file not found") }
            RSyncError::InvalidConfigFile => { write!(f, "Provided configuration file is not valid")}
            RSyncError::InvalidAlias(alias) => { write!(f, "Provided alias is invalid for this peer {}", alias) }
            RSyncError::ErrorReadingLocalFiles => { write!(f, "There was an error reading files") }
            RSyncError::CantStartServer(_) => { write!(f, "Can't start server") }
            RSyncError::CantConnectToPeer(_) => { write!(f, "Can't connect to peer") }
            RSyncError::CantCommunicateWithPeer(p) => { write!(f, "Can't communicate with peer {}", p) }
            RSyncError::ErrorFetchingPeerFiles => { write!(f, "Can't fetch peer file list")}
            RSyncError::ErrorSendingFile => { write!(f, "Error sending file")}
            RSyncError::ErrorWritingFile(reason) => {write!(f, "Error writing file: {}", reason)}
            RSyncError::ErrorParsingCommands => { write!(f, "Error parsing command from peer")}
            RSyncError::ErrorReadingFile(file) => { write!(f, "Error reading file {}", file)}
            RSyncError::InvalidPeerAddress => { write!(f, "Invalid Peer Configuration")}
            RSyncError::ErrorRemovingFile(reason) => { write!(f, "Error removing file: {}", reason)}
            RSyncError::ErrorMovingFile(reason) => { write!(f, "Error moving file: {}", reason)}
        }
    }
}

impl Error for RSyncError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            _ => { None }
        }
    }
}

impl From<bincode::Error> for RSyncError {
    fn from(_: bincode::Error) -> Self {
        RSyncError::ErrorParsingCommands
    }
}

// impl From<tokio::io::Error> for RSyncError {
//     fn from(_: tokio::io::Error) -> Self {
//         RSyncError::
//     }
// }
