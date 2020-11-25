use serde::{Serialize, Deserialize};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::{fs::File, net::TcpListener, prelude::*, sync::mpsc::Sender};

use crate::{RSyncError, config::Config, fs::{RemoteFile}, sync::SyncEvent};

use super::streaming::{DirectStream, FrameMessage, FrameReader, FrameWriter, get_streamers};

#[derive(Serialize,Deserialize, Debug, PartialEq, Clone)]
pub enum ServerStatus {
    Idle,
    SendingFiles(String),
    ReceivingFiles(String)
}
pub(crate) struct Server {
    port: u32,
    status: ServerStatus,
    config: Arc<Config>
}

struct ServerPeerHandler<T : AsyncRead + AsyncWrite + Unpin>  {
    config: Arc<Config>,
    frame_writer: FrameWriter<T>,
    frame_reader: FrameReader<T>,
    direct_stream: DirectStream<T>,
    socket_addr: String,
    sync_in_progress: bool,
    bounce_invalid_messages: bool
}

impl Server {
    pub fn new(config: Arc<Config>) -> Self {
        Server {
            port: config.port,
            config,
            status: ServerStatus::Idle
        }
    }

    pub fn get_status(&self) -> ServerStatus {
        self.status.clone()
    }

    pub fn set_status(&mut self, status: ServerStatus) {
        self.status = status
    }

    pub async fn start(&mut self, sync_events: Sender<SyncEvent> ) -> Result<(), RSyncError> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|err| RSyncError::CantStartServer(format!("{}", err)))?;

        println!("Server listening on port: {}", self.port);

        let config = self.config.clone();
        tokio::spawn(async move {
            loop {

                let sync_events = sync_events.clone();

                match listener.accept().await {
                    Ok((stream, socket)) => { 
                        let config = config.clone();
                        
                        let socket_addr = socket.ip().to_string();
                        let socket_addr = config.peers.iter().find(|p| p.starts_with(&socket_addr)).unwrap_or_else(|| &socket_addr).to_owned();

                        println!("New connection from {}", &socket_addr);

                        tokio::spawn(async move {
                            let mut handler = ServerPeerHandler::new(
                                config, 
                                stream,
                                socket_addr
                            );
                            handler.handle_events(sync_events.clone()).await
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
            sync_in_progress: false,
            bounce_invalid_messages: false
        }
    }


    async fn write_file<'b>(&mut self, alias: &'b str, file_info: &'b RemoteFile) -> Result<(), RSyncError> {
        let mut temp_path = PathBuf::new();
        let mut final_path = PathBuf::new();

        match self.config.paths.get(alias) {
            Some(path) => { 
                temp_path.push(path);
                final_path.push(path);
            }
            None => { 
                eprintln!("invalid alias");
                return Err(RSyncError::InvalidAlias(alias.to_owned()));
            }
        }
        
        final_path.push(&file_info.path);
        temp_path.push(&file_info.path);

        temp_path.set_extension("iron-carrier");

        if let Some(parent) = temp_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
            }
        }

        let mut file = File::create(&temp_path).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
        self.direct_stream.read_stream(file_info.size as usize, &mut file).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
        
        file.flush().await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;

        tokio::fs::rename(&temp_path, &final_path).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
        filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(file_info.modified_at)).map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;

        Ok(())
    }

    async fn delete_file<'b>(&mut self, alias: &'b str, file_info: &'b RemoteFile) -> Result<(), RSyncError> {
        let mut path = self.config.paths.get(alias)
            .and_then(|p| p.canonicalize().ok())
            .ok_or_else(|| RSyncError::InvalidAlias(alias.to_owned()))?;

        path.extend(&file_info.path);

        tokio::fs::remove_file(path).await
            .map_err(|e| RSyncError::ErrorRemovingFile(format!("{}", e)))
    }

    async fn move_file<'b>(&mut self, alias: &'b str, src_file: &'b RemoteFile, dest_file: &'b RemoteFile) -> Result<(), RSyncError> {
        let mut path = self.config.paths.get(alias)
            .and_then(|p| p.canonicalize().ok())
            .ok_or_else(|| RSyncError::InvalidAlias(alias.to_owned()))?;

        let mut src_path = path.clone();
        let mut dest_path = path.clone();

        src_path.extend(&src_file.path);
        dest_path.extend(&dest_file.path);

        tokio::fs::rename(src_path, dest_path).await
            .map_err(|e| RSyncError::ErrorMovingFile(format!("{}", e)))
    }

    async fn get_file_list(&'a self, alias: &str) -> Option<Vec<RemoteFile>> {
        let path = match self.config.paths.get(alias) {
            Some(path) => path,
            None => { return None; }
        };

        match crate::fs::walk_path(path).await {
            Ok(files) => Some(files.iter().map(RemoteFile::from).collect()),
            Err(_) => None
        }
    }

    async fn server_sync_hash(&self) -> Result<HashMap<String, u64>, RSyncError> {
        crate::fs::get_hash_for_alias(&self.config.paths).await
    }

    async fn handle_events(&mut self, sync_events: Sender<SyncEvent> ) {
        loop { 
            match self.frame_reader.next_frame().await  {
                Ok(command) => {
                    match command {
                        Some(mut message) => {
                            match message.frame_name() {
                                "server_sync_hash" => {
                                    let mut response = FrameMessage::new("server_sync_hash".to_owned());
                                    response.append_arg(&self.server_sync_hash().await);
                                    self.frame_writer.write_frame(response).await;
                                }

                                "query_file_list" => {
                                    let alias: Result<String, RSyncError> = message.next_arg();
                                    let mut response = FrameMessage::new("query_file_list".to_owned());
                                    response.append_arg(&self.get_file_list(&alias.unwrap()).await);
                                    self.frame_writer.write_frame(response).await;
                                }
                                
                                "prepare_file_transfer" => {
                                    let alias: Result<String, RSyncError> = message.next_arg();
                                    let file_info: Result<RemoteFile, RSyncError> = message.next_arg();
                                    
                                    self.frame_writer.write_frame("prepare_file_transfer".into()).await;
                                    if let Err(_) = self.write_file(&alias.unwrap(), &file_info.unwrap()).await {
                                        println!("failed to write file");
                                    }
                                    self.frame_writer.write_frame("file_transfer_complete".into()).await;
                                }

                                "delete_file" => {
                                    let alias: Result<String, RSyncError> = message.next_arg();
                                    let file_info: Result<RemoteFile, RSyncError> = message.next_arg();

                                    self.delete_file(&alias.unwrap(), &file_info.unwrap()).await;
                                }

                                "move_file" => {
                                    let alias: Result<String, RSyncError> = message.next_arg();
                                    let src_file: Result<RemoteFile, RSyncError> = message.next_arg();
                                    let dest_file: Result<RemoteFile, RSyncError> = message.next_arg();

                                    self.move_file(&alias.unwrap(), &src_file.unwrap(), &dest_file.unwrap()).await;
                                }
                                
                                "init_sync" => {
                                    let notify = Arc::new(tokio::sync::Notify::new());
                                    sync_events.send(SyncEvent::PeerRequestedSync(self.socket_addr.clone(), notify.clone())).await;
                                    
                                    notify.notified().await;
                                    if self.frame_writer.write_frame("init_sync".into()).await.is_ok() {
                                        self.sync_in_progress = true;
                                    }
                                }
                                "finish_sync" => {
                                    self.sync_in_progress = false;
                                    sync_events.send(SyncEvent::SyncFromPeerFinished(self.socket_addr.clone(), message.next_arg().unwrap())).await;
                                    self.frame_writer.write_frame("finish_sync".into()).await;
                                }
                                message_name => {
                                    println!("invalid message: {}", message_name);
                                    if self.bounce_invalid_messages {
                                        self.frame_writer.write_frame(message_name.into()).await;
                                    }
                                }
                            }
                        }
                        None => {
                            if self.sync_in_progress {
                                sync_events.send(SyncEvent::SyncFromPeerFinished(self.socket_addr.clone(), HashMap::new())).await;
                            }

                            break;
                        }
                    }
                }
                Err(_) => {
                    eprintln!("error reading stream");
                }
            }
        }
    }

}


#[cfg(test)] 
mod tests {
    use std::error::Error;
    use crate::network::streaming::frame_stream;

    use super::*;
    use tokio::{net::TcpStream, sync::mpsc::Receiver};
    
    
    fn sample_config() -> Arc<Config> {
        Arc::new(Config::parse_content("port = 8090
        peers = []
        
        [paths]
        a = \"./samples/peer_a\"".to_owned()).unwrap())
    }

    fn init_server(cfg: &str) -> Receiver<SyncEvent> {
        let (sender, receiver) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            let mut server = Server::new(sample_config());
            server.start(sender).await.unwrap();
        });

        receiver
    }

    #[tokio::test()]
    async fn server_handler_can_reply_messages() { 
        let (client_stream, server_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            let (sender, _) = tokio::sync::mpsc::channel(10);

            let mut server_peer_handler = ServerPeerHandler::new(sample_config(), server_stream, "".to_owned());
            server_peer_handler.bounce_invalid_messages = true;
            server_peer_handler.handle_events(sender).await;
        });

        let (mut reader, mut writer) = frame_stream(client_stream);
        writer.write_frame("ping".into()).await.unwrap();
        assert_eq!(reader.next_frame().await.unwrap().unwrap().frame_name(), "ping");
    }

    #[tokio::test]
    async fn server_reply_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            let (sender, _) = tokio::sync::mpsc::channel(10);

            let mut server_peer_handler = ServerPeerHandler::new(sample_config(), server_stream, "".to_owned());
            server_peer_handler.bounce_invalid_messages = true;
            server_peer_handler.handle_events(sender).await;
        });

        let (mut reader, mut writer) = frame_stream(client_stream);
        let mut message = FrameMessage::new("query_file_list".to_owned());
        message.append_arg(&"a")?;
        writer.write_frame(message).await?;

        let mut response = reader.next_frame().await?.unwrap();
        assert_eq!(response.frame_name(), "query_file_list");
        
        let files: Option<Vec<RemoteFile>> = response.next_arg()?;
        assert_eq!(files.unwrap().len(), 1);

        
        let mut message = FrameMessage::new("query_file_list".to_owned());
        message.append_arg(&"b")?;
        writer.write_frame(message).await?;


        let mut response = reader.next_frame().await?.unwrap();
        let files: Option<Vec<RemoteFile>> = response.next_arg()?;
        assert!(files.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn server_can_receive_files() -> Result<(), Box<dyn Error>> {
        let (client_stream, server_stream) = tokio::io::duplex(10);

        tokio::spawn(async move {
            let (sender, _) = tokio::sync::mpsc::channel(10);

            let mut server_peer_handler = ServerPeerHandler::new(sample_config(), server_stream, "".to_owned());
            server_peer_handler.handle_events(sender).await;
        });

        let mut file_content: &[u8] = b"Some file content";
        let file_size = file_content.len() as u64;

        {
            let (mut direct, mut reader, mut writer ) = get_streamers(client_stream);
            let file_info = RemoteFile { 
                path: PathBuf::from("subpath/new_file.txt"),
                size: file_size,
                created_at: std::time::SystemTime::UNIX_EPOCH,
                modified_at: std::time::SystemTime::UNIX_EPOCH
            };

            
            let mut message = FrameMessage::new("prepare_file_transfer".to_owned());
            message.append_arg(&"a")?;
            message.append_arg(&file_info)?;
            writer.write_frame(message).await?;
            
            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "prepare_file_transfer");
            
            direct.write_to_stream(file_size, &mut file_content).await?;

            let response = reader.next_frame().await?.unwrap();
            assert_eq!(response.frame_name(), "file_transfer_complete");
        }

        let file_meta = std::fs::metadata("./samples/peer_a/subpath/new_file.txt")?;
        assert_eq!(file_meta.len(), file_size);
        std::fs::remove_dir_all("./samples/peer_a/subpath")?;
        
        Ok(())
    }

    // #[tokio::test]
    // async fn server_enqueue_sync() -> Result<(), Box<dyn Error>> {
    //     let mut receiver = init_server("port = 8093
    //     peers = []
        
    //     [paths]
    //     a = \"./samples/peer_a\"");

    //     let client_one = tokio::spawn(async {
    //         let mut client = CommandStream::new( TcpStream::connect("localhost:8093").await.unwrap());
    //         client.send_command(&Command::TryInitSync).await.unwrap();
    //         assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);

    //         client.send_command(&Command::SyncFinished(HashMap::new())).await.unwrap();
    //         assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);
    //     });
        
    //     let client_two = tokio::spawn(async {
    //         let mut client = CommandStream::new( TcpStream::connect("localhost:8093").await.unwrap());
    //         client.send_command(&Command::TryInitSync).await.unwrap();
    //         assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);

    //         client.send_command(&Command::SyncFinished(HashMap::new())).await.unwrap();
    //         assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);
    //     });

    //     tokio::spawn(async move {
    //         while let Some(x) = receiver.recv().await {
    //             match x {
    //                 SyncEvent::PeerRequestedSync(_, notify) => {
    //                     notify.notify_one()
    //                 }
    //                 _ => {}
    //             }
    //         }
    //     });


    //     client_one.await.unwrap();
    //     client_two.await.unwrap();

    //     Ok(())
    // }

}