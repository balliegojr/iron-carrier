use std::{collections::HashMap, net::{TcpStream}};
use crate::{RSyncError, fs::FileInfo};

use super::{Command, Response, ResponseResult};

pub enum PeerHash {
    NoConfig,
    NoHash,
    Hash(u64)
}

pub struct Peer<'a> {
    address: &'a str,
    stream: TcpStream,
    path_files: HashMap<String, Vec<FileInfo>>
}

impl <'a> Peer<'a> {
    pub fn new(address: &'a str) -> Result<Self, RSyncError> {
        let stream = match TcpStream::connect(address) {
            Ok(stream) => stream,
            Err(err) => return Err(RSyncError::CantConnectToPeer(Box::new(err)))
        };
        
        
        Ok(Peer {
            address: address,
            stream,
            path_files: HashMap::new()
        })
    }
    
    fn send(&self, cmd: &Command) -> Result<Response, RSyncError> {
        if let Err(_) = bincode::serialize_into(&self.stream, cmd) {
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

    pub fn get_peer_file_info(&self, path_name: &str, local_file: &FileInfo) -> &'a FileInfo {
        todo!()
    }
}




