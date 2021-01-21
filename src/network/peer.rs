use super::streaming::{
    file_streamers, frame_stream, FileReceiver, FileSender, FrameMessage, FrameReader, FrameWriter,
};
use crate::{
    config::Config, fs::FileInfo, sync::BlockingEvent, sync::FileAction, IronCarrierError,
};
use std::collections::HashMap;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::mpsc::Sender,
};

type RpcResult<T> = Result<T, IronCarrierError>;

macro_rules! send_message {
    ($self:expr, $func:ident()) => {
        if $self.status == PeerStatus::Disconnected {
            log::warn!("attempted to call disconnected peer");
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()).into());
        }

        log::debug!("sending message {} to peer", stringify!($func));
        let message = FrameMessage::new(stringify!($func));
        $self.frame_writer.write_frame(message).await?;
    };

    ($self:expr, $func:ident($($arg:expr),+)) => {
        if $self.status == PeerStatus::Disconnected {
            log::warn!("attempted to call disconnected peer");
            return Err(IronCarrierError::PeerDisconectedError($self.address.to_owned()).into());
        }

        log::debug!("sending message {} to peer", stringify!($func));
        let message = FrameMessage::new(stringify!($func))
        $( .with_arg(&$arg)? )+;

        $self.frame_writer.write_frame(message).await?;
    };
}

macro_rules! rpc_call {
    ($self:expr, $func:ident($($arg:expr),*)) => {{
        send_message!($self, $func($($arg),*));

        log::debug!("waiting response for {}", stringify!($func));
        let response_message = $self.frame_reader.next_frame().await?;
        match response_message {
            Some(message) => {
                if message.frame_ident() == stringify!($func) {
                    log::debug!("received response from peer");
                    Ok(())
                } else {
                    log::error!("received wrong response {}", message.frame_ident());
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
        let response_message = $self.frame_reader.next_frame().await?;
        match response_message {
            Some(mut message) => {
                if message.frame_ident() == stringify!($func) {
                    log::debug!("received response from peer");
                    Ok(message.next_arg::<$t>()?)
                } else {
                    log::error!("received wrong response {}", message.frame_ident());
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
    Syncing,
}

pub(crate) struct Peer<'a, TReader, TWriter>
where
    TReader: AsyncRead + Unpin,
    TWriter: AsyncWrite + Unpin,
{
    address: &'a str,
    config: &'a Config,
    status: PeerStatus,
    frame_writer: FrameWriter<TWriter>,
    frame_reader: FrameReader<TReader>,
    file_sender: FileSender<TWriter>,
    file_receiver: FileReceiver<'a, TReader>,
    peer_sync_hash: HashMap<String, u64>,
    event_blocker: Sender<BlockingEvent>,
}

impl<'a> Peer<'a, ReadHalf<TcpStream>, WriteHalf<TcpStream>> {
    pub async fn new(
        address: &'a str,
        config: &'a Config,
        event_blocker: Sender<BlockingEvent>,
    ) -> crate::Result<Peer<'a, ReadHalf<TcpStream>, WriteHalf<TcpStream>>> {
        log::info!("connecting to peer {:?}", address);

        let (frame_reader, frame_writer) = frame_stream(TcpStream::connect(address).await?);
        let (file_receiver, file_sender) =
            file_streamers(TcpStream::connect(address).await?, config.clone());

        Ok(Peer {
            address,
            frame_writer,
            frame_reader,
            file_sender,
            file_receiver,
            peer_sync_hash: HashMap::new(),
            status: PeerStatus::Connected,
            config,
            event_blocker,
        })
    }

    pub fn get_address(&'a self) -> &'a str {
        &self.address
    }

    async fn send_server_port(&mut self) -> crate::Result<()> {
        log::debug!("setting peer port to {}", self.config.port);

        rpc_call!(self, set_peer_port(self.config.port))?;
        Ok(())
    }

    async fn fetch_peer_status(&mut self) -> crate::Result<()> {
        log::debug!("asking peer for status");

        let sync_hash = rpc_call!(self, server_sync_hash(), RpcResult<HashMap<String, u64>>)?;

        self.peer_sync_hash = sync_hash?;

        Ok(())
    }

    pub fn need_to_sync(&self, alias: &str, hash: u64) -> bool {
        self.peer_sync_hash
            .get(alias)
            .map(|h| *h != hash)
            .unwrap_or(false)
    }

    pub async fn fetch_files_for_alias(&mut self, alias: &str) -> crate::Result<Vec<FileInfo>> {
        log::debug!("querying peer for list of files for alias {}", alias);
        let result = rpc_call!(self, query_file_list(alias), RpcResult<Vec<FileInfo>>)?;

        match result {
            Ok(files) => Ok(files),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn sync_action<'b>(&mut self, action: &'b FileAction) -> crate::Result<()> {
        match action {
            FileAction::Create(file_info) | FileAction::Update(file_info) => {
                self.send_file(&file_info).await?
            }
            FileAction::Move(src, dest) => {
                log::debug!(
                    "asking peer {} to move file {:?} to {:?}",
                    self.address,
                    src.path,
                    dest.path
                );
                rpc_call!(self, move_file(src, dest))?
            }
            FileAction::Remove(file_info) => {
                log::debug!(
                    "asking peer {} to remove file {:?}",
                    self.address,
                    file_info.path
                );
                rpc_call!(self, delete_file(file_info))?
            }
            FileAction::Request(file_info) => {
                log::debug!("asking peer {} for file {:?}", self.address, file_info.path);
                self.event_blocker
                    .send((
                        file_info.get_absolute_path(&self.config)?,
                        self.address.to_owned(),
                    ))
                    .await?;
                self.request_file(&file_info).await?
            }
        }

        Ok(())
    }

    async fn send_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        log::debug!("sending file {:?} to peer {}", file_info.path, self.address);
        let file_handle = rpc_call!(self, create_or_update_file(file_info), u64)?;

        if file_handle > 0 {
            let file_path = file_info.get_absolute_path(&self.config)?;

            let mut file = File::open(file_path).await?;
            self.file_sender.send_file(file_handle, &mut file).await?;
        } else {
            log::debug!("peer refused file");
        }
        Ok(())
    }

    async fn request_file(&mut self, file_info: &FileInfo) -> crate::Result<()> {
        let file_handle = self.file_receiver.prepare_file_transfer(file_info.clone());
        rpc_call!(self, request_file(file_info, file_handle))?;

        self.file_receiver.wait_files().await?;

        Ok(())
    }

    pub async fn start_sync(&mut self) -> crate::Result<()> {
        log::debug!("asking peer {} to start sync", self.address);
        rpc_call!(self, init_sync())?;

        log::debug!("starting sync with peer {}", self.address);
        self.status = PeerStatus::Syncing;
        self.fetch_peer_status().await
    }

    pub async fn finish_sync(&mut self, two_way_sync: bool) -> crate::Result<()> {
        log::debug!("finishing sync with peer {}", self.address);
        rpc_call!(self, finish_sync(two_way_sync))?;

        self.status = PeerStatus::Connected;
        // self.disconnect().await;
        Ok(())
    }
}
