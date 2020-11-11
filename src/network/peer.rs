use std::{collections::HashMap, path::PathBuf};
use tokio::{fs::File, net::{TcpStream}, sync::mpsc::Sender};
use super::stream::{Command, CommandStream};
use crate::{RSyncError, fs::FileInfo, sync::SyncEvent};


pub enum SyncAction<'a> {
    Send(&'a str, PathBuf, FileInfo),
}

#[derive(PartialEq)]
enum PeerStatus {
    Disconnected,
    Connected,
    Syncing
}

pub(crate) struct Peer {
    address: String,
    status: PeerStatus,
    stream: Option<CommandStream<TcpStream>>,
    
    peer_sync_hash: HashMap<String, u64>,
    sync_events: Sender<SyncEvent>
}

impl Peer {
    pub fn new(address: String, sync_events: Sender<SyncEvent>) -> Peer {
        
        Peer {
            address: address,
            stream: None,
            peer_sync_hash: HashMap::new(),
            status: PeerStatus::Disconnected,
            sync_events: sync_events
        }
    }
    
    pub async fn connect(&mut self) {
        match TcpStream::connect(&self.address).await {
            Ok(stream) => {
                self.stream = Some(CommandStream::new(stream));
                self.status = PeerStatus::Connected;
            }
            Err(_) => self.status = PeerStatus::Disconnected
        };
    }

    pub async fn disconnect(&mut self) {
        self.stream = None;
        if self.status == PeerStatus::Syncing {
            self.sync_events.send(SyncEvent::SyncToPeerFailed(self.address.clone())).await;
        }

        self.status = PeerStatus::Disconnected;
        self.peer_sync_hash = HashMap::new();
    }

    async fn fetch_peer_status(&mut self) -> Result<(), RSyncError>{
        if self.status == PeerStatus::Disconnected {
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_owned()));
        }
        
        self.send_command(&Command::QueryServerSyncHash).await?;

        let stream = self.stream.as_mut().unwrap();
        if let Some(cmd) = stream.next_command().await? {
            match cmd {
                Command::ReplyServerSyncHash(sync_hash) => {
                    self.peer_sync_hash = sync_hash;
                }
                _ => {
                    println!("Received other command");
                }
            }
        }

        Ok(())
    }

    pub fn need_to_sync(&self, alias: &str, hash: u64) -> bool {
        self.peer_sync_hash.get(alias).map(|h| *h != hash).unwrap_or(false)
    }

    async fn send_command(&mut self, cmd: &Command) -> Result<(), RSyncError> {
        if self.status == PeerStatus::Disconnected {
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_owned()));
        }

        let stream = self.stream.as_mut().unwrap();
        if let Err(e) = stream.send_command(cmd).await {
            eprintln!("{}", e);
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_string()));
        };

        Ok(())
    }

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> Result<Vec<FileInfo>, RSyncError> {
        self.send_command(&Command::QueryFileList(alias.to_owned())).await?;
        if let Some(cmd) = self.stream.as_mut().unwrap().next_command().await? {
            match cmd {
                Command::ReplyFileList(_alias, files) => { return Ok(files) }
                _ => {}
            }
        }

        return Err(RSyncError::ErrorParsingCommands);

    }

    pub async fn sync_action<'a>(&'a mut self, action: SyncAction<'a>) -> Result<(), RSyncError>{
        match action {
            SyncAction::Send(alias, root_folder, file_info) => { 
                self.send_file(alias, root_folder, &file_info).await?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, alias: &str, root_folder: PathBuf, file_info: &FileInfo) -> Result<(), RSyncError> {
        if self.status == PeerStatus::Disconnected {
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_owned()));
        }
        
        let mut path = std::path::PathBuf::from(root_folder);
        path.push(&file_info.path);
        
        let mut file = File::open(path).await.map_err(| e| RSyncError::ErrorReadingFile(e.to_string()))?;
        
        let stream = self.stream.as_mut().unwrap();

        stream.send_command(&Command::InitFileTransfer(alias.to_string(), file_info.clone()))
            .await
            .map_err(|_| RSyncError::ErrorParsingCommands)?;
        
        match stream.send_data_from_buffer(file_info.size, &mut file).await {
            Ok(_) => { match stream.next_command().await? {
                Some(_) => { Ok(()) }
                None => Err(RSyncError::ErrorSendingFile)
            }}
            Err(_) => Err(RSyncError::ErrorSendingFile)
        }
    }

    pub async fn start_sync(&mut self) -> Result<(), RSyncError> {
        self.send_command(&Command::TryInitSync).await?;
        let stream = match self.stream.as_mut() {
            Some(stream) => { stream}
            None => { return Err(RSyncError::CantConnectToPeer(self.address.clone())) }
        };

        let command = stream.next_command().await?;
        match command {
            Some(command) => {
                if let Command::CommandSuccess = command {
                    self.status = PeerStatus::Syncing;
                    self.fetch_peer_status().await
                } else {
                    self.sync_events.send(SyncEvent::SyncToPeerFailed(self.address.clone())).await;
                    return Err(RSyncError::CantCommunicateWithPeer(self.address.clone()));
                }
            }
            None => {
                self.disconnect().await;
                self.sync_events.send(SyncEvent::SyncToPeerFailed(self.address.clone())).await;
                return Err(RSyncError::CantCommunicateWithPeer(self.address.clone()));
            }
        }
    }

    pub async fn finish_sync(&mut self, sync_hash: HashMap<String, u64>) -> Result<(), RSyncError> {
        self.send_command(&Command::SyncFinished(sync_hash)).await?;
        self.sync_events.send(SyncEvent::SyncToPeerSuccess(self.address.clone())).await;

        self.status = PeerStatus::Connected;

        let stream = self.stream.as_mut().unwrap();
        let _command = stream.next_command().await?;

        self.disconnect().await;
        Ok(())
    }
}




