use std::{collections::HashMap, error::Error, io::Read, io::Write, net::TcpStream};
use crate::{RSyncError, fs::FileInfo};

use super::{BUFFER_SIZE, Command, Response, ResponseResult};



pub enum SyncAction<'a> {
    Send(&'a str, &'a str, FileInfo),
    // Receive(FileInfo)
}

pub struct Peer<'a> {
    address: &'a str,
    stream: TcpStream,
    path_files: HashMap<String, Vec<FileInfo>>,
    sync_queue: Vec<SyncAction<'a>>
}

impl <'a> Peer<'a> {
    pub fn new(address: &'a str) -> Result<Self, RSyncError> {
        let stream = match TcpStream::connect(address) {
            Ok(stream) => stream,
            Err(err) => return Err(RSyncError::CantConnectToPeer(Box::new(err)))
        };

        // stream.set_nonblocking(false);
        
        
        Ok(Peer {
            address: address,
            stream,
            path_files: HashMap::new(),
            sync_queue: Vec::new()
        })
    }
    
    fn send(&self, cmd: &Command) -> Result<Response, RSyncError> {
        if let Err(e) = bincode::serialize_into(&self.stream, cmd) {
            eprintln!("{}", e);
            return Err(RSyncError::CantCommunicateWithPeer(self.address.to_string()));
        };

        let response: ResponseResult = bincode::deserialize_from(&self.stream);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(RSyncError::CantCommunicateWithPeer(self.address.to_string()))
        }
    }

    pub fn fetch_unsynced_file_list(&mut self, alias_hash: &Vec<(String, u64)>) -> Result<(), RSyncError> {
        let response = self.send(&Command::UnsyncedFileList(alias_hash.clone()))?;
        if let Response::FileList(response) = response {
            for (alias, files) in response {
                self.path_files.insert(alias, files);
            }
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

    fn send_file(&mut self, alias: &str, root_folder: &str, file_info: &FileInfo) -> Result<Response, Box<dyn Error>> {
        let mut path = std::path::PathBuf::from(root_folder);
        path.push(&file_info.path);
        
        let mut file = std::fs::File::open(path)?;
        let mut buf = [0u8; BUFFER_SIZE];

        bincode::serialize_into(&self.stream, &Command::PrepareFile(alias.to_owned(), file_info.clone()))?;

        loop {
            let read_size = file.read(&mut buf)?;
            if read_size > 0 {
                self.stream.write(&buf)?;
            } else {
                break;
            }
        }

        let response: Response = bincode::deserialize_from(&self.stream)?;
        Ok(response)
    }

    pub fn sync(&mut self) {
        loop {
            match self.sync_queue.pop() {
                Some(action) => {
                    match action {
                        SyncAction::Send(alias, root_folder, file_info) => { 
                            match self.send_file(alias, root_folder, &file_info) {
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




