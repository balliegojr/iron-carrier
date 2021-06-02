use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::Sender,
};

use crate::{
    config::Config, fs, fs::FileInfo, sync::BlockingEvent, sync::SyncEvent, IronCarrierError,
};

use crate::network::streaming::{FileReceiver, FileSender, FrameMessage, FrameReader, FrameWriter};

type RpcResult<T> = Result<T, IronCarrierError>;

pub(crate) struct ServerPeerHandler<'a, TReader, TWriter>
where
    TReader: AsyncRead + Unpin,
    TWriter: AsyncWrite + Unpin,
{
    config: &'a Config,
    frame_writer: FrameWriter<TWriter>,
    frame_reader: FrameReader<TReader>,
    file_sender: FileSender<TWriter>,
    file_receiver: FileReceiver<'a, TReader>,
    socket_addr: SocketAddr,
    sync_notifier: Option<Arc<tokio::sync::Notify>>,
    bounce_invalid_messages: bool,
}

impl<'a, TReader, TWriter> ServerPeerHandler<'a, TReader, TWriter>
where
    TReader: AsyncRead + Unpin,
    TWriter: AsyncWrite + Unpin,
{
    pub fn new(
        config: &'a Config,
        frame_reader: FrameReader<TReader>,
        frame_writer: FrameWriter<TWriter>,
        file_receiver: FileReceiver<'a, TReader>,
        file_sender: FileSender<TWriter>,
        socket_addr: SocketAddr,
    ) -> Self {
        Self {
            config,
            frame_reader,
            frame_writer,
            file_sender,
            file_receiver,
            socket_addr,
            sync_notifier: None,
            bounce_invalid_messages: false,
        }
    }

    fn should_sync_file(&self, remote_file: &FileInfo) -> bool {
        !remote_file.is_local_file_newer(&self.config)
    }

    async fn get_file_list(&self, alias: &str) -> RpcResult<Vec<FileInfo>> {
        let path = self
            .config
            .paths
            .get(alias)
            .ok_or_else(|| IronCarrierError::AliasNotAvailable(alias.to_owned()))?;
        crate::fs::walk_path(path, alias)
            .await
            .map_err(|_| IronCarrierError::IOReadingError)
    }

    async fn server_sync_hash(&self) -> RpcResult<HashMap<String, u64>> {
        crate::fs::get_hash_for_alias(&self.config.paths)
            .await
            .map_err(|_| IronCarrierError::IOReadingError)
    }

    pub async fn close(&mut self) {
        if let Some(notifier) = self.sync_notifier.take() {
            notifier.notify_one();
        }
    }

    pub async fn handle_events<'b>(
        &mut self,
        sync_events: Sender<SyncEvent>,
        events_blocker: Sender<BlockingEvent>,
    ) -> crate::Result<()> {
        loop {
            match self.frame_reader.next_frame().await? {
                Some(mut message) => match message.frame_ident() {
                    "set_peer_port" => {
                        let port = message.next_arg::<u16>()?;
                        log::debug!("peer requested to change port to {}", port);
                        self.socket_addr.set_port(port);
                        self.frame_writer
                            .write_frame("set_peer_port".into())
                            .await?;
                    }
                    "server_sync_hash" => {
                        log::debug!("peer requested sync hash");
                        let response = FrameMessage::new("server_sync_hash")
                            .with_arg(&self.server_sync_hash().await)?;

                        self.frame_writer.write_frame(response).await?;
                    }

                    "query_file_list" => {
                        let alias = message.next_arg::<String>()?;
                        log::debug!("peer requested file list for alias {}", alias);
                        let response = FrameMessage::new("query_file_list")
                            .with_arg(&self.get_file_list(&alias).await)?;
                        self.frame_writer.write_frame(response).await?;
                    }

                    "create_or_update_file" => {
                        let remote_file = message.next_arg::<FileInfo>()?;
                        log::debug!("peer request to send file {:?}", remote_file.path);

                        if self.should_sync_file(&remote_file) {
                            let file_path = remote_file.get_absolute_path(&self.config)?;
                            events_blocker
                                .send((file_path, self.socket_addr.clone()))
                                .await;

                            let file_handle = self.file_receiver.prepare_file_transfer(remote_file);
                            let response = FrameMessage::new("create_or_update_file")
                                .with_arg(&file_handle)?;
                            self.frame_writer.write_frame(response).await?;
                            self.file_receiver.wait_files().await?;
                        } else {
                            let response =
                                FrameMessage::new("create_or_update_file").with_arg(&0u64)?;
                            self.frame_writer.write_frame(response).await?;
                        }
                    }

                    "request_file" => {
                        let remote_file = message.next_arg::<FileInfo>()?;
                        let file_handle = message.next_arg::<u64>()?;

                        log::debug!("peer request file {:?}", remote_file.path);

                        let file_path = remote_file.get_absolute_path(&self.config)?;

                        log::debug!("sending file to peer: {}", remote_file.size.unwrap());
                        let mut file = File::open(file_path).await?;

                        self.frame_writer.write_frame("request_file".into()).await?;
                        self.file_sender.send_file(file_handle, &mut file).await?;

                        log::debug!("file sent {:?}", remote_file.path);
                    }

                    "delete_file" => {
                        let remote_file = message.next_arg::<FileInfo>()?;

                        log::debug!("peer requested to delete file {:?}", remote_file.path);

                        let file_path = remote_file.get_absolute_path(&self.config)?;
                        events_blocker
                            .send((file_path, self.socket_addr.clone()))
                            .await;

                        fs::delete_file(&remote_file, &self.config).await?;
                        self.frame_writer.write_frame("delete_file".into()).await?;
                    }

                    "move_file" => {
                        let src_file = message.next_arg::<FileInfo>()?;
                        let dest_file = message.next_arg::<FileInfo>()?;

                        log::debug!(
                            "peer requested to move file {:?} to {:?}",
                            src_file.path,
                            dest_file.path
                        );

                        let file_path = src_file.get_absolute_path(&self.config)?;
                        events_blocker
                            .send((file_path, self.socket_addr.clone()))
                            .await;

                        fs::move_file(&src_file, &dest_file, &self.config).await?;

                        self.frame_writer.write_frame("move_file".into()).await?;
                    }

                    "init_sync" => {
                        log::debug!("peer requested to start sync");

                        let sync_starter = Arc::new(tokio::sync::Notify::new());
                        let sync_ended = Arc::new(tokio::sync::Notify::new());

                        sync_events
                            .send(SyncEvent::PeerRequestedSync(
                                self.socket_addr.clone(),
                                sync_starter.clone(),
                                sync_ended.clone(),
                            ))
                            .await?;

                        log::debug!("waiting for sync schedule");
                        sync_starter.notified().await;

                        log::info!("init sync with peer");
                        self.frame_writer.write_frame("init_sync".into()).await?;
                        self.sync_notifier = Some(sync_ended);
                    }
                    "finish_sync" => {
                        let two_way_sync = message.next_arg::<bool>()?;
                        log::debug!("peer is finishing sync");

                        if let Some(notifier) = self.sync_notifier.take() {
                            notifier.notify_one();
                        }

                        if two_way_sync {
                            log::debug!("schedulling sync back with peer");
                            sync_events
                                .send(SyncEvent::EnqueueSyncToPeer(
                                    self.socket_addr.clone(),
                                    false,
                                ))
                                .await?;
                        }

                        self.frame_writer.write_frame("finish_sync".into()).await?;
                    }
                    message_name => {
                        if self.bounce_invalid_messages {
                            self.frame_writer.write_frame(message_name.into()).await?;
                        } else {
                            return Err(IronCarrierError::ParseCommandError.into());
                        }
                    }
                },
                None => {
                    if let Some(notifier) = self.sync_notifier.take() {
                        notifier.notify_one();
                    }

                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
        time::Duration,
    };
    use tokio::io::DuplexStream;

    use crate::network::streaming::{file_streamers, frame_stream};

    use super::*;

    fn sample_config(test_folder: &str) -> Arc<Config> {
        Arc::new(
            Config::new_from_str(format!(
                "port = 8090
       
        [paths]
        a = \"./tmp/{}\"",
                test_folder
            ))
            .unwrap(),
        )
    }

    fn create_tmp_file(path: &Path, contents: &str) {
        if !path.parent().unwrap().exists() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        }

        std::fs::write(path, contents).unwrap();
    }

    async fn create_peer_handler(
        test_folder: &str,
        command_stream: DuplexStream,
        file_stream: DuplexStream,
    ) {
        let config = sample_config(test_folder);
        let (events_tx, _) = tokio::sync::mpsc::channel(10);
        let (events_blocker_tx, _) = tokio::sync::mpsc::channel(10);

        let (frame_reader, frame_writer) = frame_stream(command_stream);
        let (file_receiver, file_sender) = file_streamers(file_stream, &config);

        let mut server_peer_handler = ServerPeerHandler::new(
            &config,
            frame_reader,
            frame_writer,
            file_receiver,
            file_sender,
            SocketAddr::from_str("127.0.0.1:0").unwrap(),
        );

        server_peer_handler.bounce_invalid_messages = true;
        server_peer_handler
            .handle_events(events_tx, events_blocker_tx)
            .await
            .unwrap();
    }

    #[tokio::test()]
    async fn server_handler_can_reply_messages() {
        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (_, server_file_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            create_peer_handler(
                "server_handler_can_reply_messages",
                server_stream,
                server_file_stream,
            )
            .await;
        });

        let (mut reader, mut writer) = frame_stream(client_stream);
        writer.write_frame("ping".into()).await.unwrap();
        assert_eq!(
            reader.next_frame().await.unwrap().unwrap().frame_ident(),
            "ping"
        );
    }

    #[tokio::test]
    async fn server_reply_query_file_list() -> crate::Result<()> {
        create_tmp_file(
            Path::new("./tmp/server_reply_query_file_list/file_1"),
            "some content",
        );

        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (_, server_file_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            create_peer_handler(
                "server_reply_query_file_list",
                server_stream,
                server_file_stream,
            )
            .await;
        });

        let (mut reader, mut writer) = frame_stream(client_stream);
        let message = FrameMessage::new("query_file_list").with_arg(&"a")?;
        writer.write_frame(message).await?;

        let mut response = reader.next_frame().await?.unwrap();
        assert_eq!(response.frame_ident(), "query_file_list");

        let files = response.next_arg::<RpcResult<Vec<FileInfo>>>()??;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, Path::new("file_1"));

        let message = FrameMessage::new("query_file_list").with_arg(&"b")?;
        writer.write_frame(message).await?;

        let mut response = reader.next_frame().await?.unwrap();
        let files = response.next_arg::<RpcResult<Vec<FileInfo>>>()?;
        assert!(files.is_err());

        std::fs::remove_dir_all("./tmp/server_reply_query_file_list")?;

        Ok(())
    }

    #[tokio::test]
    async fn server_can_receive_files() -> crate::Result<()> {
        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (client_file_stream, server_file_stream) = tokio::io::duplex(10);

        let (mut reader, mut writer) = frame_stream(client_stream);
        let mut file_sender = FileSender::new(client_file_stream);

        tokio::spawn(async move {
            create_peer_handler(
                "server_can_receive_files",
                server_stream,
                server_file_stream,
            )
            .await;
        });

        let mut file_content: &[u8] = b"Some file content";
        let file_size = file_content.len() as u64;

        let modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let file_info = FileInfo {
            alias: "a".to_owned(),
            path: PathBuf::from("subpath/new_file.txt"),
            size: Some(file_size),
            created_at: Some(0),
            modified_at: Some(modified_at),
            deleted_at: None,
            permissions: 0,
        };

        let message = FrameMessage::new("create_or_update_file").with_arg(&file_info)?;
        writer.write_frame(message).await?;

        let mut response = reader.next_frame().await?.unwrap();
        assert_eq!(response.frame_ident(), "create_or_update_file");

        let file_handle: u64 = response.next_arg()?;
        file_sender
            .send_file(file_handle, &mut file_content)
            .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let file_meta = std::fs::metadata("./tmp/server_can_receive_files/subpath/new_file.txt")?;
        assert_eq!(file_meta.len(), file_size);
        std::fs::remove_dir_all("./tmp/server_can_receive_files")?;

        Ok(())
    }

    #[tokio::test]
    async fn server_can_delete_files() -> crate::Result<()> {
        create_tmp_file(Path::new("./tmp/server_can_delete_files/file_1"), "");
        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (_, server_file_stream) = tokio::io::duplex(10);

        let (mut reader, mut writer) = frame_stream(client_stream);

        tokio::spawn(async move {
            create_peer_handler("server_can_delete_files", server_stream, server_file_stream).await;
        });

        {
            let file_info = FileInfo {
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None,
                permissions: 0,
            };

            let message = FrameMessage::new("delete_file").with_arg(&file_info)?;
            writer.write_frame(message).await?;

            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_ident(), "delete_file");

            assert!(!Path::new("./tmp/server_can_delete_files/file_1").exists());
            std::fs::remove_dir_all("./tmp/server_can_delete_files")?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn server_can_move_files() -> crate::Result<()> {
        create_tmp_file(Path::new("./tmp/server_can_move_files/file_1"), "");

        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (_, server_file_stream) = tokio::io::duplex(10);
        let (mut reader, mut writer) = frame_stream(client_stream);

        tokio::spawn(async move {
            create_peer_handler("server_can_move_files", server_stream, server_file_stream).await;
        });

        {
            let src = FileInfo {
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None,
                permissions: 0,
            };

            let dst = FileInfo {
                alias: "a".to_owned(),
                path: PathBuf::from("file_2"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None,
                permissions: 0,
            };

            let message = FrameMessage::new("move_file")
                .with_arg(&src)?
                .with_arg(&dst)?;
            writer.write_frame(message).await?;

            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_ident(), "move_file");

            assert!(!Path::new("./tmp/server_can_move_files/file_1").exists());
            assert!(Path::new("./tmp/server_can_move_files/file_2").exists());

            std::fs::remove_dir_all("./tmp/server_can_move_files")?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn server_can_send_files() -> crate::Result<()> {
        let file_size = b"Some file content".len() as u64;
        create_tmp_file(
            Path::new("./tmp/server_can_send_files/file_1"),
            "Some file content",
        );

        let (client_stream, server_stream) = tokio::io::duplex(10);
        let (client_file_stream, server_file_stream) = tokio::io::duplex(10);
        let (mut reader, mut writer) = frame_stream(client_stream);

        tokio::spawn(async move {
            create_peer_handler("server_can_send_files", server_stream, server_file_stream).await;
        });

        {
            let modified_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();

            let file_info = FileInfo {
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: Some(file_size),
                created_at: None,
                modified_at: Some(modified_at),
                deleted_at: None,
                permissions: 0,
            };

            let config = sample_config("server_can_send_files_2");
            let mut receiver = FileReceiver::new(client_file_stream, &config);

            let message = FrameMessage::new("request_file")
                .with_arg(&file_info)?
                .with_arg(&receiver.prepare_file_transfer(file_info))?;
            writer.write_frame(message).await?;

            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_ident(), "request_file");

            receiver.wait_files().await?;

            assert!(Path::new("./tmp/server_can_send_files_2/file_1").exists());

            std::fs::remove_dir_all("./tmp/server_can_send_files")?;
            std::fs::remove_dir_all("./tmp/server_can_send_files_2")?;
        }

        Ok(())
    }
}
