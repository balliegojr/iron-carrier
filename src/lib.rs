use std::{error::Error, fmt::Display};

use fs::FileInfo;
use network::{peer::{Peer, SyncAction}, server::Server};
use tokio::task::JoinHandle;

pub mod config;
mod fs;
mod crypto;
mod network;

const DEFAULT_PORT: u32 = 8090;

#[derive(Debug)]
pub enum RSyncError {
    InvalidConfigPath,
    InvalidConfigFile,
    InvalidAlias(String),
    ErrorReadingLocalFiles,
    ErrorFetchingPeerFiles,
    CantStartServer(String),
    CantConnectToPeer(String),
    CantCommunicateWithPeer(String),
    ErrorSendingFile,
    ErrorWritingFile(String),
    ErrorParsingCommands
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

type AliasHashFiles<'a> = (&'a str, u64, Vec<FileInfo>);

pub fn start_server<'a>(config: &'a config::Config) -> JoinHandle<()> {
    let config = config.clone();

    tokio::spawn(async move {
        let mut server = Server::new(&config);
        match server.wait_commands().await {
            Ok(_) => { println!("Server listening on port {}", &config.port.unwrap_or_else(|| DEFAULT_PORT)); }
            Err(err) => { eprintln!("{}", err) }
        }
    })
}

fn need_to_send_file<'a>(alias: &'a str, file: &FileInfo, peer: &Peer) -> bool {
    match peer.get_peer_file_info(alias, file) {
        Some(peer_file) => { file.modified_at > peer_file.modified_at || file.size != peer_file.size }
        None => { true }
    }
}

pub async fn full_sync_available_peers(config: &config::Config) -> Result<(), RSyncError>{
    let mut sync_folders: Vec<AliasHashFiles> = Vec::new();
    for (path_alias, path)  in &config.paths {
        let (hash, local_files) = match fs::get_files_with_hash(path).await {
            Ok(files) => files,
            Err(_) => return Err(RSyncError::ErrorReadingLocalFiles)
        };

        sync_folders.push((path_alias, hash, local_files));
    }
    
    let mut peers = Vec::new();
    {
        let alias_hash: Vec<(String, u64)> = sync_folders.iter().map(|(alias, hash, _)| (alias.to_string(), *hash)).collect();
        for peer_address in &config.peers {
            let mut peer = match Peer::new(peer_address).await {
                Ok(peer) => peer,
                Err(e) => {
                    eprintln!("{}: {}", peer_address, e);
                    continue;
                }
            };

            match peer.fetch_unsynced_file_list(&alias_hash).await {
                Ok(_) => { peers.push(peer) }
                Err(e) => { 
                    eprintln!("{}", e);
                    continue;
                }
            }
        }
    }
    
    for (alias, _, files) in sync_folders {
        let root_path = config.paths.get(alias).unwrap();

        for peer in &mut peers {
            if !peer.has_alias(alias) { continue; }

            for file_info in &files {
                if need_to_send_file(alias, &file_info, &peer) {
                    peer.enqueue_sync_action(SyncAction::Send(alias, root_path, file_info.clone()))
                }
            }
        }
    }

    for peer in &mut peers {
        peer.sync().await
    }

    Ok(())
}