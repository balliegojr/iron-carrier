use std::{collections::HashMap, error::Error };
use crate::{RSyncError, fs::FileInfo};
use tokio::{fs::File, net::{TcpStream}};

use super::stream::{Command, CommandStream};


pub enum SyncAction<'a> {
    Send(&'a str, &'a str, FileInfo),
    // Receive(FileInfo)
}

pub struct Peer<'a> {
    address: &'a str,
    stream: CommandStream<TcpStream>,
    path_files: HashMap<String, Vec<FileInfo>>,
    sync_queue: Vec<SyncAction<'a>>
}

impl <'a> Peer<'a> {
    pub async fn new(address: &'a str) -> Result<Peer<'a>, RSyncError> {
        let stream = match TcpStream::connect(address).await {
            Ok(stream) => stream,
            Err(_) => return Err(RSyncError::CantConnectToPeer(address.to_owned()))
        };

        Ok(Peer {
            address: address,
            stream: CommandStream::new(stream),
            path_files: HashMap::new(),
            sync_queue: Vec::new()
        })
    }
    
    async fn send(&mut self, cmd: &Command) -> Result<Option<Command>, RSyncError> {
        if let Err(e) = self.stream.send_command(cmd).await {
            eprintln!("{}", e);
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_string()));
        };
        
        match self.stream.next_command().await {
            Ok(response) => Ok(response),
            Err(_) => Err(RSyncError::CantCommunicateWithPeer(self.address.to_string()))
        }
    }

    pub async fn fetch_unsynced_file_list(&mut self, alias_hash: &Vec<(String, u64)>) -> Result<(), RSyncError> {
        let response = self.send(&Command::FetchUnsyncedFileList(alias_hash.clone())).await?;
        match response {
            Some(cmd) => {
                match cmd {
                    Command::FileList(files) => {
                        for (alias, files) in files {
                            self.path_files.insert(alias, files);
                        }
                    }
                    _ => { return Err(RSyncError::ErrorReadingLocalFiles) }
                }
            }
            None => { return Err(RSyncError::ErrorReadingLocalFiles) }
        }

       
        Ok(())
    }

    pub fn has_alias(&self, alias: &str) -> bool {
        self.path_files.contains_key(alias)
    }

    pub fn get_peer_file_info(&'a self, alias: &str, local_file: &FileInfo) -> Option<&'a FileInfo> {
        if let Some(files) = self.path_files.get(alias) {
            for file in files {
                if file.path == local_file.path {
                    return Some(file);
                }
            }
        };

        return None
    }

    pub fn enqueue_sync_action(&mut self, action: SyncAction<'a>) {
        self.sync_queue.push(action);
    }

    async fn send_file(&mut self, alias: &str, root_folder: &str, file_info: &FileInfo) -> Result<Command, Box<dyn Error>> {
        let mut path = std::path::PathBuf::from(root_folder);
        path.push(&file_info.path);
        
        let mut file = File::open(path).await?;
        
        self.stream.send_command(&Command::InitFileTransfer(alias.to_string(), file_info.clone())).await?;
        
        match self.stream.send_data_from_buffer(file_info.size, &mut file).await {
            Ok(_) => { match self.stream.next_command().await? {
                Some(cmd) => { Ok(cmd) }
                None => {
                    Ok(Command::FileTransferFailed)
                }
            }}
            Err(_) => { Ok(Command::FileTransferFailed) }
        }
    }

    pub async fn sync(&mut self) {
        loop {
            match self.sync_queue.pop() {
                Some(action) => {
                    match action {
                        SyncAction::Send(alias, root_folder, file_info) => { 
                            match self.send_file(alias, root_folder, &file_info).await {
                                Ok(_) => { println!("successfully sent file {}", file_info.path.to_str().unwrap_or_default()) }
                                Err(_) => { eprintln!("an error ocurred sending file {} ", file_info.path.to_str().unwrap_or_default()) }
                            };
                        }
                        // SyncAction::Receive(_) => {}
                    }
                }
                None => { break; }
            }
        }
    }
}




