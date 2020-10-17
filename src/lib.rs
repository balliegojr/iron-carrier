use std::{error::Error, fmt::Display, thread};

use fs::FileInfo;
use network::peer::{Peer, SyncAction};
use thread::JoinHandle;

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
    CantStartServer(Box<dyn Error>),
    CantConnectToPeer(Box<dyn Error>),
    CantCommunicateWithPeer(String)
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
        }
    }
}

impl Error for RSyncError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            RSyncError::InvalidConfigPath => { None }
            RSyncError::InvalidConfigFile => { None }
            RSyncError::InvalidAlias(_) => { None }
            RSyncError::ErrorReadingLocalFiles => { None }
            RSyncError::CantStartServer(_) => { None }
            RSyncError::CantConnectToPeer(_) => { None }
            RSyncError::CantCommunicateWithPeer(_) => { None }
        }
    }
}

type AliasHashFiles<'a> = (&'a str, u64, Vec<FileInfo>);

pub fn start_server(config: config::Config) -> JoinHandle<()>{
    thread::spawn(move || {
        let server = network::server::Server::new(&config);
        match server.wait_commands() {
            Ok(_) => {}
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

pub fn full_sync_available_peers(config: &config::Config) -> Result<(), RSyncError>{
    let mut sync_folders: Vec<AliasHashFiles> = Vec::new();
    for (path_alias, path)  in &config.paths {
        let (hash, local_files) = match fs::get_files_with_hash(path) {
            Ok(files) => files,
            Err(_) => return Err(RSyncError::ErrorReadingLocalFiles)
        };

        sync_folders.push((path_alias, hash, local_files));
    }
    
    let mut peers = Vec::new();
    {
        let alias_hash: Vec<(String, u64)> = sync_folders.iter().map(|(alias, hash, _)| (alias.to_string(), *hash)).collect();
        for peer_address in &config.peers {
            let mut peer = match Peer::new(peer_address) {
                Ok(peer) => peer,
                Err(e) => {
                    eprintln!("Peer {} is not available", peer_address);
                    continue;
                }
            };

            match peer.fetch_unsynced_file_list(&alias_hash) {
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
        peer.sync()
    }

    Ok(())
}