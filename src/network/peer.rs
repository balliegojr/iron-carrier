use std::{collections::HashMap};
use tokio::{fs::File, net::{TcpStream}};
use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};
use crate::{IronCarrierError, config::Config, fs::FileInfo, sync::FileAction, fs};


macro_rules! send_message {
    ($self:expr, $func:ident()) => {
        if $self.status == PeerStatus::Disconnected {
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()));
        }

        let message = FrameMessage::new(stringify!($func).to_string());
        $self.frame_writer.as_mut().unwrap().write_frame(message).await?;
    };
    
    ($self:expr, $func:ident($($arg:expr),+)) => {
        if $self.status == PeerStatus::Disconnected {
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()));
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
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                Err(IronCarrierError::ParseCommandError)
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
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                Err(IronCarrierError::ParseCommandError)
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
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                Err(IronCarrierError::ParseCommandError)
            }
        }
    }}
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
    
    pub async fn connect(&mut self) -> crate::Result<()> {
        let stream = TcpStream::connect(&self.address).await.map_err(|_| IronCarrierError::PeerDisconectedError(self.address.to_owned()))?;
        let (direct_stream, reader, writer) = get_streamers(stream);

        

        self.frame_reader = Some(reader);
        self.frame_writer = Some(writer);
        self.direct_stream = Some(direct_stream);
        self.status = PeerStatus::Connected;
        
        self.send_peer_address().await
    }

    async fn send_peer_address(&mut self) -> crate::Result<()> {
        rpc_call!(self, set_peer_address(self.address))
    }

    pub async fn disconnect(&mut self) {
        self.frame_reader = None;
        self.frame_writer = None;
        self.direct_stream = None;

        self.status = PeerStatus::Disconnected;
        self.peer_sync_hash = HashMap::new();
    }

    async fn fetch_peer_status(&mut self) -> crate::Result<()>{
        let sync_hash = rpc_call!(
            self,
            server_sync_hash(),
            crate::Result<HashMap<String,u64>>
        )?;

        self.peer_sync_hash = sync_hash?;

        Ok(())
    }

    pub fn need_to_sync(&self, alias: &str, hash: u64) -> bool {
        self.peer_sync_hash.get(alias).map(|h| *h != hash).unwrap_or(false)
    }

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> crate::Result<Vec<FileInfo>> {
        rpc_call!(
            self,
            query_file_list(alias),
            crate::Result<Vec<FileInfo>>
        )?
    }

    pub async fn sync_action<'b>(&mut self, action: &'b FileAction) -> crate::Result<()>{
        match action {
            FileAction::Create(file_info) | FileAction::Update(file_info)=> { 
                self.send_file(&file_info).await?
            }
            FileAction::Move(src, dest) => {
                rpc_call!(
                    self,
                    move_file(src, dest)
                )?
            }
            FileAction::Remove(file_info) => {
                rpc_call!(
                    self,
                    delete_file(file_info)
                )?
            },
            FileAction::Request(file_info) => {
                self.request_file(&file_info).await?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        let should_sync = rpc_call!(
            self,
            create_or_update_file(file_info),
            bool
        )?;
        
        if should_sync {
            let file_path = file_info.get_absolute_path(&self.config)?;

            let mut file = File::open(file_path).await.map_err(|_| IronCarrierError::IOReadingError)?;
            self.direct_stream.as_mut().unwrap().write_to_stream(file_info.size.unwrap(), &mut file)
                .await
                .map_err(|_| IronCarrierError::NetworkIOWritingError)?;
    
            self.frame_reader.as_mut().unwrap().next_frame().await?.unwrap().frame_name();
        }
        Ok(())
    }

    async fn request_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        rpc_call!(
            self,
            request_file(file_info)
        )?;

        if let Err(_) = fs::write_file(&file_info, &self.config, self.direct_stream.as_mut().unwrap()).await {
            println!("failed to write file");
        }
        self.frame_writer.as_mut().unwrap().write_frame("request_file_complete".into()).await?;
        
        Ok(())
    }
    
    pub async fn start_sync(&mut self) -> crate::Result<()> {
        rpc_call!(
            self,
            init_sync()
        )?;
            
        self.status = PeerStatus::Syncing;
        self.fetch_peer_status().await
    }

    pub async fn finish_sync(&mut self, two_way_sync: bool) -> crate::Result<()> {
        rpc_call!(
            self,
            finish_sync(two_way_sync)
        )?;

        self.status = PeerStatus::Connected;
        self.disconnect().await;
        Ok(())
    }
}




