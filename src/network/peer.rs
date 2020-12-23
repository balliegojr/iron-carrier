use std::{collections::HashMap};
use tokio::{fs::File, net::{TcpStream}};
use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};
use crate::{IronCarrierError, config::Config, fs::FileInfo, sync::FileAction, fs};

type RpcResult<T> = Result<T, IronCarrierError>;

macro_rules! send_message {
    ($self:expr, $func:ident()) => {
        if $self.status == PeerStatus::Disconnected {
            log::warn!("attempted to call disconnected peer");
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()).into());
        }

        log::debug!("sending message {} to peer", stringify!($func));
        let message = FrameMessage::new(stringify!($func).to_string());
        $self.frame_writer.as_mut().unwrap().write_frame(message).await?;
    };
    
    ($self:expr, $func:ident($($arg:expr),+)) => {
        if $self.status == PeerStatus::Disconnected {
            log::warn!("attempted to call disconnected peer");
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()).into());
        }

        log::debug!("sending message {} to peer", stringify!($func));
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

        log::debug!("waiting response for {}", stringify!($func));
        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(message) => {
                if message.frame_name() == stringify!($func) {
                    log::debug!("received response from peer");
                    Ok(())
                } else {
                    log::error!("received wrong response {}", message.frame_name());
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                log::error!("didn't receive response for {}", stringify!($func));
                Err(IronCarrierError::ParseCommandError)
            }
        }
    }};

    ($self:expr, $func:ident($($arg:expr),*), $t:ty) => {{
        send_message!($self, $func($($arg),*));

        log::debug!("waiting response for {}", stringify!($func));
        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(mut message) => {
                if message.frame_name() == stringify!($func) {
                    log::debug!("received response from peer");
                    Ok(message.next_arg::<$t>()?)
                } else {
                    log::error!("received wrong response {}", message.frame_name());
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                log::error!("didn't receive response for {}", stringify!($func));
                Err(IronCarrierError::ParseCommandError)
            }
        }
    }};
    ($self:expr, $func:ident($($arg:expr),*), [$t:ty],+) => {{
        send_message!($self, $func($($arg),*))

        log::debug!("waiting response for {}", stringify!($func));
        let response_message = $self.frame_reader.as_mut().unwrap().next_frame().await?;
        match response_message {
            Some(message) => {
                if message.is_same_message(stringify!($func)) {
                    log::debug!("received response from peer");
                    Ok($( message.next_arg::<$t>()?, )*)
                } else {
                    log::error!("received wrong response {}", message.frame_name());
                    Err(IronCarrierError::ParseCommandError)
                }
            }
            None => {
                log::error!("didn't receive response for {}", stringify!($func));
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
        log::info!("connecting to peer {:?}", self.address);
        let stream = TcpStream::connect(&self.address).await?;
        let (direct_stream, reader, writer) = get_streamers(stream);
       

        self.frame_reader = Some(reader);
        self.frame_writer = Some(writer);
        self.direct_stream = Some(direct_stream);
        self.status = PeerStatus::Connected;
        
        self.send_server_port().await
    }

    async fn send_server_port(&mut self) -> crate::Result<()> {
        log::debug!("setting peer port to {}", self.config.port);

        rpc_call!(self, set_peer_port(self.config.port))?;
        Ok(())
    }

    pub async fn disconnect(&mut self) {
        log::info!("disconnecting from peer {}", self.address);
        self.frame_reader = None;
        self.frame_writer = None;
        self.direct_stream = None;

        self.status = PeerStatus::Disconnected;
        self.peer_sync_hash = HashMap::new();
    }

    async fn fetch_peer_status(&mut self) -> crate::Result<()>{
        log::debug!("asking peer for status");

        let sync_hash = rpc_call!(
            self,
            server_sync_hash(),
            RpcResult<HashMap<String,u64>>
        )?;

        self.peer_sync_hash = sync_hash?;

        Ok(())
    }

    pub fn need_to_sync(&self, alias: &str, hash: u64) -> bool {
        self.peer_sync_hash.get(alias).map(|h| *h != hash).unwrap_or(false)
    }

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> crate::Result<Vec<FileInfo>> {
        log::debug!("querying peer for list of files for alias {}", alias);
        let result = rpc_call!(
            self,
            query_file_list(alias),
            RpcResult<Vec<FileInfo>>
        )?;

        match result {
            Ok(files) => { Ok(files) }
            Err(err) => { Err(err.into())}
        }
    }

    pub async fn sync_action<'b>(&mut self, action: &'b FileAction) -> crate::Result<()>{

        match action {
            FileAction::Create(file_info) | FileAction::Update(file_info)=> { 
                self.send_file(&file_info).await?
            }
            FileAction::Move(src, dest) => {
                log::debug!("asking peer {} to move file {:?} to {:?}", self.address, src.path, dest.path);
                rpc_call!(
                    self,
                    move_file(src, dest)
                )?
            }
            FileAction::Remove(file_info) => {
                log::debug!("asking peer {} to remove file {:?}", self.address, file_info.path);
                rpc_call!(
                    self,
                    delete_file(file_info)
                )?
            },
            FileAction::Request(file_info) => {
                log::debug!("asking peer {} for file {:?}", self.address, file_info.path);
                // I must find waht is wrong with this call
                // self.request_file(&file_info).await?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        log::debug!("sending file {:?} to peer {}", file_info.path, self.address);
        let should_sync = rpc_call!(
            self,
            create_or_update_file(file_info),
            bool
        )?;
        
        if should_sync {
            let file_path = file_info.get_absolute_path(&self.config)?;

            let mut file = File::open(file_path).await?;
            self.direct_stream.as_mut().unwrap().write_to_stream(file_info.size.unwrap(), &mut file)
                .await?;
    
            self.frame_reader.as_mut().unwrap().next_frame().await?.unwrap().frame_name();
        } else {
            log::debug!("peer refused file");
        }
        Ok(())
    }

    async fn request_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        rpc_call!(
            self,
            request_file(file_info)
        )?;

        if let Err(_) = fs::write_file(&file_info, &self.config, self.direct_stream.as_mut().unwrap()).await {
            log::error!("failed to write file");
        }
        self.frame_writer.as_mut().unwrap().write_frame("request_file_complete".into()).await?;
        
        Ok(())
    }
    
    pub async fn start_sync(&mut self) -> crate::Result<()> {
        log::debug!("asking peer {} to start sync", self.address);
        rpc_call!(
            self,
            init_sync()
        )?;
            
        log::debug!("starting sync with peer {}", self.address);
        self.status = PeerStatus::Syncing;
        self.fetch_peer_status().await
    }

    pub async fn finish_sync(&mut self, two_way_sync: bool) -> crate::Result<()> {
        log::debug!("finishing sync with peer {}", self.address);
        rpc_call!(
            self,
            finish_sync(two_way_sync)
        )?;

        self.status = PeerStatus::Connected;
        self.disconnect().await;
        Ok(())
    }
}




