use std::{collections::HashMap, sync::Arc };
use tokio::{fs::File, net::TcpListener, prelude::*, sync::mpsc::Sender};

use crate::{
    IronCarrierError, 
    config::Config, 
    sync::file_events_buffer::FileEventsBuffer, 
    fs, 
    fs::FileInfo, 
    sync::SyncEvent
};

use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};

pub(crate) struct Server {
    port: u32,
    config: Arc<Config>,
    file_events: Arc<FileEventsBuffer>
}

struct ServerPeerHandler<T : AsyncRead + AsyncWrite + Unpin>  {
    config: Arc<Config>,
    frame_writer: FrameWriter<T>,
    frame_reader: FrameReader<T>,
    direct_stream: DirectStream<T>,
    socket_addr: String,
    sync_notifier: Option<Arc<tokio::sync::Notify>>,
    bounce_invalid_messages: bool
}

impl Server {
    pub fn new(config: Arc<Config>, file_events: Arc<FileEventsBuffer>) -> Self {
        Server {
            port: config.port,
            config,
            file_events
        }
    }

    pub async fn start(&mut self, sync_events: Sender<SyncEvent> ) -> crate::Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|err| IronCarrierError::ServerStartError(format!("{}", err)))?;

        println!("Server listening on port: {}", self.port);

        let config = self.config.clone();
        let file_events = self.file_events.clone();

        tokio::spawn(async move {
            loop {

                let sync_events = sync_events.clone();
                
                match listener.accept().await {
                    Ok((stream, socket)) => { 
                        let config = config.clone();
                        let file_events = file_events.clone();
                        
                        let socket_addr = socket.ip().to_string();
                        // let socket_addr = config.peers.iter().find(|p| p.starts_with(&socket_addr)).unwrap_or_else(|| &socket_addr).to_owned();

                        println!("New connection from {}", &socket_addr);

                        tokio::spawn(async move {
                            let mut handler = ServerPeerHandler::new(
                                config, 
                                stream,
                                socket_addr
                            );
                            
                            match handler.handle_events(sync_events, file_events).await {
                                Ok(()) => { println!("Peer connection closed: {}", handler.socket_addr)}
                                Err(err) => { eprintln!("Some error ocurred:{}", err) }
                            }
                        });
                        
                    }
                    Err(_) => {}
                }
            }
        });      

        Ok(())
    }
}

impl <'a, T : AsyncWrite + AsyncRead + Unpin> ServerPeerHandler<T> {
    fn new(config: Arc<Config>, stream: T, socket_addr: String) -> Self {
        let (direct_stream, frame_reader, frame_writer) = get_streamers(stream);
        
        ServerPeerHandler {
            config,
            frame_reader,
            frame_writer,
            direct_stream,
            socket_addr,
            sync_notifier: None,
            bounce_invalid_messages: false
        }
    }

    fn should_sync_file(&self, remote_file: &FileInfo) -> bool {
        !remote_file.is_local_file_newer(&self.config)
    }

    async fn get_file_list(&self, alias: &str) -> crate::Result<Vec<FileInfo>> {
        let path = self.config.paths.get(alias).ok_or_else(|| IronCarrierError::AliasNotAvailable(alias.to_owned()))?;
        crate::fs::walk_path(path, alias).await
    }

    async fn server_sync_hash(&self) -> crate::Result<HashMap<String, u64>> {
        crate::fs::get_hash_for_alias(&self.config.paths).await
    }

    async fn handle_events<'b>(&'a mut self, sync_events: Sender<SyncEvent>, file_events_buffer: Arc<FileEventsBuffer> ) -> crate::Result<()> {
        loop { 
            match self.frame_reader.next_frame().await?  {
                Some(mut message) => {
                    match message.frame_name() {
                        "set_peer_address" => {
                            self.socket_addr = message.next_arg::<String>()?;
                        }
                        "server_sync_hash" => {
                            let mut response = FrameMessage::new("server_sync_hash".to_owned());
                            response.append_arg(&self.server_sync_hash().await)?;
                            self.frame_writer.write_frame(response).await?;
                        }

                        "query_file_list" => {
                            let alias = message.next_arg::<String>()?;
                            let mut response = FrameMessage::new("query_file_list".to_owned());
                            response.append_arg(&self.get_file_list(&alias).await)?;
                            self.frame_writer.write_frame(response).await?;
                        }
                        
                        "create_or_update_file" => {
                            let remote_file = message.next_arg::<FileInfo>()?;
                            let should_sync = self.should_sync_file(&remote_file);

                            let mut response = FrameMessage::new("create_or_update_file".to_owned());
                            response.append_arg(&should_sync)?;
                            self.frame_writer.write_frame(response).await?;

                            if should_sync {
                                file_events_buffer.add_event(remote_file.get_absolute_path(&self.config)?, &self.socket_addr);
                                if let Err(_) = fs::write_file(&remote_file, &self.config, &mut self.direct_stream).await {
                                    println!("failed to write file");
                                }
                                self.frame_writer.write_frame("create_or_update_file_complete".into()).await?;
                            }
                        }

                        "request_file" => {
                            let remote_file = message.next_arg::<FileInfo>()?;

                            self.frame_writer.write_frame("request_file".into()).await?;

                            let file_path = remote_file.get_absolute_path(&self.config)?;
                            let mut file = File::open(file_path).await.map_err(|_| IronCarrierError::IOReadingError)?;
                            self.direct_stream.write_to_stream(remote_file.size.unwrap(), &mut file)
                            .await
                            .map_err(|_| IronCarrierError::NetworkIOWritingError)?;

                            self.frame_reader.next_frame().await?.unwrap().frame_name();
                        }

                        "delete_file" => {
                            let remote_file = message.next_arg::<FileInfo>()?;

                            println!("deleting file {:?}", remote_file.path);

                            file_events_buffer.add_event(remote_file.get_absolute_path(&self.config)?, &self.socket_addr);

                            fs::delete_file(&remote_file, &self.config).await?;
                            self.frame_writer.write_frame("delete_file".into()).await?;
                        }

                        "move_file" => {
                            let src_file = message.next_arg::<FileInfo>()?;
                            let dest_file = message.next_arg::<FileInfo>()?;

                            file_events_buffer.add_event(src_file.get_absolute_path(&self.config)?, &self.socket_addr);
                            file_events_buffer.add_event(dest_file.get_absolute_path(&self.config)?, &self.socket_addr);

                            fs::move_file(&src_file, &dest_file, &self.config).await?;

                            self.frame_writer.write_frame("move_file".into()).await?;
                        }
                        
                        "init_sync" => {
                            let sync_starter = Arc::new(tokio::sync::Notify::new());
                            let sync_ended = Arc::new(tokio::sync::Notify::new());
                            
                            sync_events.send(SyncEvent::PeerRequestedSync(
                                self.socket_addr.clone(), 
                                sync_starter.clone(),
                                sync_ended.clone()
                            )).await;
                            
                            sync_starter.notified().await;
                            if self.frame_writer.write_frame("init_sync".into()).await.is_ok() {
                                self.sync_notifier = Some(sync_ended);
                            }
                        }
                        "finish_sync" => {
                            let two_way_sync = message.next_arg::<bool>()?;
                            
                            self.sync_notifier.as_ref().unwrap().notify_one();
                            self.sync_notifier = None;
                            
                            if two_way_sync {
                                sync_events.send(SyncEvent::EnqueueSyncToPeer(self.socket_addr.clone(), false)).await;
                            }
                            
                            self.frame_writer.write_frame("finish_sync".into()).await?;
                        }
                        message_name => {
                            if self.bounce_invalid_messages {
                                self.frame_writer.write_frame(message_name.into()).await?;
                            } else {
                                return Err(IronCarrierError::ParseCommandError)
                            }
                        }
                    }
                }
                None => {
                    if self.sync_notifier.is_some() {
                        self.sync_notifier.as_ref().unwrap().notify_one();
                        self.sync_notifier = None;
                    }

                    return Ok(());
                }
                
            }
        }
    }

}


#[cfg(test)] 
mod tests {
    use std::{error::Error, path::{ PathBuf, Path }};
    use crate::network::streaming::frame_stream;

    use super::*;
    
    fn sample_config(test_folder: &str) -> Arc<Config> {
        Arc::new(Config::parse_content(format!("port = 8090
       
        [paths]
        a = \"./tmp/{}\"", test_folder)).unwrap())
    }

    fn create_tmp_file(path: &Path, contents: &str) {
        if !path.parent().unwrap().exists() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        }

        std::fs::write(path, contents).unwrap();
    }

    #[tokio::test()]
    async fn server_handler_can_reply_messages() { 
        let (client_stream, server_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_handler_can_reply_messages")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_handler_can_reply_messages"), server_stream, "".to_owned());
            server_peer_handler.bounce_invalid_messages = true;
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });

        let (mut reader, mut writer) = frame_stream(client_stream);
        writer.write_frame("ping".into()).await.unwrap();
        assert_eq!(reader.next_frame().await.unwrap().unwrap().frame_name(), "ping");
    }

    #[tokio::test]
    async fn server_reply_query_file_list() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);

        create_tmp_file(Path::new("./tmp/server_reply_query_file_list/file_1"), "some content");

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_reply_query_file_list")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_reply_query_file_list"), server_stream, "".to_owned());
            server_peer_handler.bounce_invalid_messages = true;
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });

        

        let (mut reader, mut writer) = frame_stream(client_stream);
        let mut message = FrameMessage::new("query_file_list".to_owned());
        message.append_arg(&"a")?;
        writer.write_frame(message).await?;

        let mut response = reader.next_frame().await?.unwrap();
        assert_eq!(response.frame_name(), "query_file_list");
        
        let files= response.next_arg::<crate::Result<Vec<FileInfo>>>()??;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, Path::new("file_1"));

        
        let mut message = FrameMessage::new("query_file_list".to_owned());
        message.append_arg(&"b")?;
        writer.write_frame(message).await?;


        let mut response = reader.next_frame().await?.unwrap();
        let files= response.next_arg::<crate::Result<Vec<FileInfo>>>()?;
        assert!(files.is_err());

        std::fs::remove_dir_all("./tmp/server_reply_query_file_list")?;

        Ok(())
    }

    #[tokio::test]
    async fn server_can_receive_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);
        
        create_tmp_file(Path::new("./tmp/server_can_receive_files/file_1"), "");

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_can_receive_files")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_can_receive_files"), server_stream, "".to_owned());
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });

        let mut file_content: &[u8] = b"Some file content";
        let file_size = file_content.len() as u64;

        {
            let (mut direct, mut reader, mut writer ) = get_streamers(client_stream);
            let file_info = FileInfo { 
                alias: "a".to_owned(),
                path: PathBuf::from("subpath/new_file.txt"),
                size: Some(file_size),
                created_at: Some(std::time::SystemTime::UNIX_EPOCH),
                modified_at: Some(std::time::SystemTime::UNIX_EPOCH),
                deleted_at: None
            };

            
            let mut message = FrameMessage::new("create_or_update_file".to_owned());
            message.append_arg(&file_info)?;
            writer.write_frame(message).await?;
            
            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "create_or_update_file");
            
            direct.write_to_stream(file_size, &mut file_content).await?;

            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "create_or_update_file_complete");
        }

        let file_meta = std::fs::metadata("./tmp/server_can_receive_files/subpath/new_file.txt")?;
        assert_eq!(file_meta.len(), file_size);
        std::fs::remove_dir_all("./tmp/server_can_receive_files")?;
        
        Ok(())
    }

    #[tokio::test]
    async fn server_can_delete_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);
        
        create_tmp_file(Path::new("./tmp/server_can_delete_files/file_1"), "");

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_can_delete_files")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_can_delete_files"), server_stream, "".to_owned());
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });

        {
            let (_, mut reader, mut writer ) = get_streamers(client_stream);
            let file_info = FileInfo { 
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None
            };

                
            let mut message = FrameMessage::new("delete_file".to_owned());
            message.append_arg(&file_info)?;
            writer.write_frame(message).await?;
                
            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "delete_file");

            assert!(!Path::new("./tmp/server_can_delete_files/file_1").exists());
            std::fs::remove_dir_all("./tmp/server_can_delete_files")?;
            
        }
        Ok(())
    }

    #[tokio::test]
    async fn server_can_move_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);
        
        create_tmp_file(Path::new("./tmp/server_can_move_files/file_1"), "");

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_can_move_files")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_can_move_files"), server_stream, "".to_owned());
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });

        {
            let (_, mut reader, mut writer ) = get_streamers(client_stream);
            let src = FileInfo { 
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None
            };

            let dst = FileInfo { 
                alias: "a".to_owned(),
                path: PathBuf::from("file_2"),
                size: None,
                created_at: None,
                modified_at: None,
                deleted_at: None
            };

                
            let mut message = FrameMessage::new("move_file".to_owned());
            message.append_arg(&src)?;
            message.append_arg(&dst)?;
            writer.write_frame(message).await?;
                
            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "move_file");

            assert!(!Path::new("./tmp/server_can_move_files/file_1").exists());
            assert!(Path::new("./tmp/server_can_move_files/file_2").exists());

            std::fs::remove_dir_all("./tmp/server_can_move_files")?;
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn server_can_send_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);

        let file_size = b"Some file content".len() as u64;
        create_tmp_file(Path::new("./tmp/server_can_send_files/file_1"), "Some file content");

        tokio::spawn(async move {
            let (events_tx, _) = tokio::sync::mpsc::channel(10);
            let files_event_buffer = Arc::new(FileEventsBuffer::new(sample_config("server_can_send_files")));

            let mut server_peer_handler = ServerPeerHandler::new(sample_config("server_can_send_files"), server_stream, "".to_owned());
            server_peer_handler.handle_events(events_tx, files_event_buffer).await.unwrap();
        });
       
        {
            let (mut direct, mut reader, mut writer ) = get_streamers(client_stream);
            let file_info = FileInfo { 
                alias: "a".to_owned(),
                path: PathBuf::from("file_1"),
                size: Some(file_size),
                created_at: None,
                modified_at: None,
                deleted_at: None
            };
    
            
            let mut message = FrameMessage::new("request_file".to_owned());
            message.append_arg(&file_info)?;
            writer.write_frame(message).await?;
            
            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "request_file");
    
            let mut sink = tokio::io::sink();
            direct.read_stream(file_size as usize, &mut sink).await?;
    
            writer.write_frame("request_file_complete".into()).await?;
            std::fs::remove_dir_all("./tmp/server_can_send_files")?;
        }
        
        Ok(())
    }
}