use std::{collections::HashMap};
use tokio::{fs::File, net::{TcpStream}};
use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};
use crate::{RSyncError, config::Config, fs::FileInfo};


macro_rules! send_message {
    ($self:expr, $func:ident()) => {
        if $self.status == PeerStatus::Disconnected {
            return Err(RSyncError::CantCommunicateWithPeer($self.address.to_owned()));
        }

        let message = FrameMessage::new(stringify!($func).to_string());
        $self.frame_writer.as_mut().unwrap().write_frame(message).await?;
    };
    
    ($self:expr, $func:ident($($arg:expr),+)) => {
        if $self.status == PeerStatus::Disconnected {
            return Err(RSyncError::CantCommunicateWithPeer($self.address.to_owned()));
        }

        let mut message = FrameMessage::new(stringify!($func).to_string());
        $(
            message.append_arg(&$arg)?;
        )+
    
        $self.frame_writer.as_mut().unwrap().write_frame(message).await?;
    };
}

macro_rules! rpc_call {
    ($self:expr, $func:ident($($arg:expr),*)) => {{
        send_message!($self, $func($($arg),*));

        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(message) => {
                if message.frame_name() == stringify!($func) {
                    Ok(())
                } else {
                    Err(RSyncError::ErrorParsingCommands)
                }
            }
            None => {
                Err(RSyncError::ErrorParsingCommands)
            }
        }
    }};

    ($self:expr, $func:ident($($arg:expr),*), $t:ty) => {{
        send_message!($self, $func($($arg),*));

        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(mut message) => {
                if message.frame_name() == stringify!($func) {
                    Ok(message.next_arg::<$t>()?)
                } else {
                    Err(RSyncError::ErrorParsingCommands)
                }
            }
            None => {
                Err(RSyncError::ErrorParsingCommands)
            }
        }
    }};
    ($self:expr, $func:ident($($arg:expr),*), [$t:ty],+) => {{
        send_message!($self, $func($($arg),*))

        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(message) => {
                if message.is_same_message(stringify!($func)) {
                    let result = ;
                    Ok($( message.next_arg::<$t>()?, )*)
                } else {
                    Err(RSyncError::ErrorParsingCommands)
                }
            }
            None => {
                Err(RSyncError::ErrorParsingCommands)
            }
        }
    }}
}


pub enum SyncAction<'a> {
    Send(&'a FileInfo),
    Move(&'a FileInfo, &'a FileInfo),
    Delete(&'a FileInfo)
}

#[derive(PartialEq)]
enum PeerStatus {
    Disconnected,
    Connected,
    Syncing
}

pub(crate) struct Peer<'a> {
    address: &'a str,
    config: &'a Config,
    status: PeerStatus,
    frame_writer: Option<FrameWriter<TcpStream>>,
    frame_reader: Option<FrameReader<TcpStream>>,
    direct_stream: Option<DirectStream<TcpStream>>,
    
    peer_sync_hash: HashMap<String, u64>,
}

impl <'a> Peer<'a> {
    pub fn new(address: &'a str, config: &'a Config) -> Self {
        
        Peer {
            address: address,
            frame_writer: None,
            frame_reader: None,
            direct_stream: None,
            peer_sync_hash: HashMap::new(),
            status: PeerStatus::Disconnected,
            config
        }
    }

    pub fn get_address(&'a self) -> &'a str {
        &self.address
    }
    
    pub async fn connect(&mut self) -> Result<(), RSyncError> {
        match TcpStream::connect(&self.address).await {
            Ok(stream) => {
                let (direct_stream, reader, writer) = get_streamers(stream);
                self.frame_reader = Some(reader);
                self.frame_writer = Some(writer);
                self.direct_stream = Some(direct_stream);
                self.status = PeerStatus::Connected;
                Ok(())
            }
            Err(_) => {
                self.status = PeerStatus::Disconnected;
                Err(RSyncError::CantConnectToPeer(self.address.to_owned()))
            }
        }
    }

    pub async fn disconnect(&mut self) {
        self.frame_reader = None;
        self.frame_writer = None;
        self.direct_stream = None;

        self.status = PeerStatus::Disconnected;
        self.peer_sync_hash = HashMap::new();
    }

    async fn fetch_peer_status(&mut self) -> Result<(), RSyncError>{
        let sync_hash = rpc_call!(
            self,
            server_sync_hash(),
            Result<HashMap<String,u64>, RSyncError>
        )?;

        self.peer_sync_hash = sync_hash?;

        Ok(())
    }

    pub fn need_to_sync(&self, alias: &str, hash: u64) -> bool {
        self.peer_sync_hash.get(alias).map(|h| *h != hash).unwrap_or(false)
    }

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> Result<Vec<FileInfo>, RSyncError> {
        let files = rpc_call!(
            self,
            query_file_list(alias),
            Option<Vec<FileInfo>>
        )?;

        return files.ok_or_else(|| RSyncError::ErrorParsingCommands);
    }

    pub async fn sync_action<'b>(&mut self, action: &'b SyncAction<'b>) -> Result<(), RSyncError>{
        match action {
            SyncAction::Send(file_info) => { 
                self.send_file(file_info).await?
            }
            SyncAction::Move(src, dest) => {
                rpc_call!(
                    self,
                    move_file(src, dest)
                )?
            }
            SyncAction::Delete(file) => {
                rpc_call!(
                    self,
                    delete_file(file)
                )?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, file_info: &FileInfo) -> Result<(), RSyncError> {
        let should_sync = rpc_call!(
            self,
            prepare_file_transfer(file_info),
            bool
        )?;
        
        if should_sync {
            let file_path = file_info.get_absolute_path(&self.config)?;

            let mut file = File::open(file_path).await.map_err(| e| RSyncError::ErrorReadingFile(e.to_string()))?;
            self.direct_stream.as_mut().unwrap().write_to_stream(file_info.size.unwrap(), &mut file)
                .await
                .map_err(|_| RSyncError::ErrorSendingFile)?;
    
            self.frame_reader.as_mut().unwrap().next_frame().await?.unwrap().frame_name();
        }
        Ok(())
    }

    pub async fn start_sync(&mut self) -> Result<(), RSyncError> {
        rpc_call!(
            self,
            init_sync()
        )?;
            
        self.status = PeerStatus::Syncing;
        self.fetch_peer_status().await
    }

    pub async fn finish_sync(&mut self, sync_hash: HashMap<String, u64>) -> Result<(), RSyncError> {
        rpc_call!(
            self,
            finish_sync(sync_hash)
        )?;

        self.status = PeerStatus::Connected;
        self.disconnect().await;
        Ok(())
    }
}




