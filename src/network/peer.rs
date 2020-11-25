use std::{collections::HashMap};
use tokio::{fs::File, net::{TcpStream}};
use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};
use crate::{RSyncError, fs::{LocalFile, RemoteFile}};


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
    Send(&'a str, &'a LocalFile),
    Move(&'a str, &'a LocalFile, &'a LocalFile),
    Delete(&'a str, &'a LocalFile)
}

#[derive(PartialEq)]
enum PeerStatus {
    Disconnected,
    Connected,
    Syncing
}

pub(crate) struct Peer<'a> {
    address: &'a str,
    status: PeerStatus,
    frame_writer: Option<FrameWriter<TcpStream>>,
    frame_reader: Option<FrameReader<TcpStream>>,
    direct_stream: Option<DirectStream<TcpStream>>,
    
    peer_sync_hash: HashMap<String, u64>,
}

impl <'a> Peer<'a> {
    pub fn new(address: &'a str) -> Self {
        
        Peer {
            address: address,
            frame_writer: None,
            frame_reader: None,
            direct_stream: None,
            peer_sync_hash: HashMap::new(),
            status: PeerStatus::Disconnected,
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

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> Result<Vec<RemoteFile>, RSyncError> {
        let files = rpc_call!(
            self,
            query_file_list(alias),
            Option<Vec<RemoteFile>>
        )?;

        return files.ok_or_else(|| RSyncError::ErrorParsingCommands);
    }

    pub async fn sync_action<'b>(&mut self, action: &'b SyncAction<'b>) -> Result<(), RSyncError>{
        match action {
            SyncAction::Send(alias, file_info) => { 
                self.send_file(alias, &file_info).await?
            }
            SyncAction::Move(alias, src, dest) => {
                rpc_call!(
                    self,
                    move_file(alias, RemoteFile::from(*src), RemoteFile::from(*dest))
                )?
            }
            SyncAction::Delete(alias, file_info) => {
                rpc_call!(
                    self,
                    delete_file(alias, RemoteFile::from(*file_info))
                )?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, alias: &str, file_info: &LocalFile) -> Result<(), RSyncError> {
        
        let mut file = File::open(&file_info.absolute_path).await.map_err(| e| RSyncError::ErrorReadingFile(e.to_string()))?;
        rpc_call!(
            self,
            prepare_file_transfer(alias, RemoteFile::from(file_info))
        )?;

        self.direct_stream.as_mut().unwrap().write_to_stream(file_info.size, &mut file)
            .await
            .map_err(|_| RSyncError::ErrorSendingFile)?;

        self.frame_reader.as_mut().unwrap().next_frame().await?.unwrap().frame_name();
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




