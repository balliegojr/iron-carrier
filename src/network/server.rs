use serde::{Serialize, Deserialize};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::{fs::File, net::TcpListener, net::TcpStream, prelude::*, sync::mpsc::Sender};

use crate::{RSyncError, config::Config, fs::FileInfo, network::stream::CommandStream, sync::SyncEvent};

use super::stream::Command;


#[derive(Serialize,Deserialize, Debug, PartialEq, Clone)]
pub enum ServerStatus {
    Idle,
    SyncInProgress(String)
}
pub(crate) struct Server {
    port: u32,
    status: ServerStatus,
    config: Arc<Config>
}

struct ServerPeerHandler  {
    config: Arc<Config>,
    stream: CommandStream<TcpStream>,
    socket_addr: String,
    sync_in_progress: bool
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
                                CommandStream::new(stream), 
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

impl <'a> ServerPeerHandler {
    fn new(config: Arc<Config>, stream: CommandStream<TcpStream>, socket_addr: String) -> Self {
        ServerPeerHandler {
            config,
            stream,
            socket_addr,
            sync_in_progress: false
        }
    }

    async fn reply(&mut self, resp: Command) {
        match self.stream.send_command(&resp).await {
            Ok(_) => { }
            Err(_) => { eprintln!("failed to serialize response"); }
        }
    }

    async fn write_file(&mut self, alias: String, file_info: FileInfo) -> Result<(), RSyncError> {
        let mut temp_path = PathBuf::new();
        let mut final_path = PathBuf::new();

        match self.config.paths.get(&alias) {
            Some(path) => { 
                temp_path.push(path);
                final_path.push(path);
            }
            None => { 
                eprintln!("invalid alias");
                return Err(RSyncError::InvalidAlias(alias));
            }
        }
        
        final_path.push(&file_info.path);
        temp_path.push(&file_info.path);

        temp_path.set_extension("r-synctemp");

        if let Some(parent) = temp_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
            }
        }

        let mut file = File::create(&temp_path).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
        self.stream.write_to_buffer(file_info.size as usize, &mut file).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;

        file.flush().await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;

        tokio::fs::rename(&temp_path, &final_path).await.map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;
        filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(file_info.modified_at)).map_err(|e| RSyncError::ErrorWritingFile(format!("{}", e)))?;

        Ok(())
    }

    async fn get_file_list(&self, alias: &str) -> Option<Vec<FileInfo>> {
        let path = match self.config.paths.get(alias) {
            Some(path) => path,
            None => { return None; }
        };

        match crate::fs::get_files_with_hash(path.clone()).await {
            Ok((_, files)) => Some(files),
            Err(_) => None
        }
    }

    async fn handle_events(&mut self, sync_events: Sender<SyncEvent> ) {
        loop { 
            match self.stream.next_command().await  {
                Ok(command) => {
                    match command {
                        Some(cmd) => {
                            match cmd {
                                super::stream::Command::Ping => { self.reply(Command::Pong).await }
                                super::stream::Command::QueryFileList(alias) => {
                                    let response = self.get_file_list(&alias).await;
                                    match response {
                                        Some(files) => { self.reply(Command::ReplyFileList(alias, files)).await }
                                        None => {  self.reply(Command::CommandFailed).await }
                                    }
                                }
                                super::stream::Command::InitFileTransfer(alias, file_info) => {
                                    match self.write_file(alias, file_info).await {
                                        Ok(_) => { self.reply(Command::FileTransferSuccess).await; }
                                        Err(_) => { self.reply(Command::FileTransferFailed).await; }
                                    };
                                }
                                Command::QueryServerSyncHash => {
                                    //TODO: cache sync_hash into server
                                    let sync_hash = crate::fs::get_hash_for_alias(&self.config.paths).await;
                                    let command = match sync_hash {
                                        Ok(sync_hash) => { Command::ReplyServerSyncHash(sync_hash) }
                                        Err(_) => { Command::CommandFailed }
                                    };
                                    
                                    self.reply(command).await;
                                }
                                Command::TryInitSync => {
                                    let notify = Arc::new(tokio::sync::Notify::new());
                                    sync_events.send(SyncEvent::PeerRequestedSync(self.socket_addr.clone(), notify.clone())).await;
                                    
                                    notify.notified().await;

                                    self.sync_in_progress = true;
                                    self.reply(Command::CommandSuccess).await;
                                }
                                Command::SyncFinished(peer_sync_hash) => {
                                    self.sync_in_progress = false;

                                    sync_events.send(SyncEvent::SyncFromPeerFinished(self.socket_addr.clone(), peer_sync_hash)).await;
                                    self.reply(Command::CommandSuccess).await;
                                }
                                _ => {
                                    println!("server invalid command");
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
    use super::*;
    use tokio::{net::TcpStream, sync::mpsc::Receiver};
    
    fn init_server(cfg: &str) -> Receiver<SyncEvent> {
        let config = Arc::new(Config::parse_content(cfg.to_owned()).unwrap());
        let (sender, receiver) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            let mut server = Server::new(config);
            server.start(sender).await.unwrap();
        });

        receiver
    }

    #[tokio::test()]
    async fn server_reply_ping() { 
        init_server("port = 8090
        peers = []
        
        [paths]
        a = \"./samples/peer_a\"");

        let client_one = tokio::spawn(async {
            let mut client = CommandStream::new( TcpStream::connect("localhost:8090").await.unwrap() );
            client.send_command(&Command::Ping).await.unwrap();
            let resp_one = client.next_command().await.unwrap();
            assert_eq!(resp_one.unwrap(), Command::Pong);

        });

        let client_two = tokio::spawn(async {
            let mut client = CommandStream::new( TcpStream::connect("localhost:8090").await.unwrap() );
            client.send_command(&Command::Ping).await.unwrap();
            let resp_one = client.next_command().await.unwrap();
            assert_eq!(resp_one.unwrap(), Command::Pong);
        });

        client_one.await.unwrap();
        client_two.await.unwrap();
    }

    #[tokio::test]
    async fn server_reply_files() -> Result<(), Box<dyn Error>> {
        init_server("port = 8091
        peers = []
        
        [paths]
        a = \"./samples/peer_b\"");

        let mut client = CommandStream::new( TcpStream::connect("localhost:8091").await? );

        client.send_command(&Command::QueryFileList("a".to_owned())).await?;
        let response = client.next_command().await?.unwrap();
        
        match response {
            Command::ReplyFileList(alias, files) => {
                assert_eq!(alias, "a");
                assert_eq!(files.len(), 1);

            },
            _ => panic!("wrong response")
        };

        Ok(())
    }

    #[tokio::test]
    async fn server_can_receive_files() -> Result<(), Box<dyn Error>> {
        init_server("port = 8092
        peers = []
        
        [paths]
        a = \"./samples/peer_a\"");

        let mut file_content: &[u8] = b"Some file content";
        let file_size = file_content.len() as u64;

        let mut client = CommandStream::new( TcpStream::connect("localhost:8092").await? );
        let file_info = FileInfo { 
            path: std::path::PathBuf::from("subpath/new_file.txt"),
            size: file_size,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            modified_at: std::time::SystemTime::UNIX_EPOCH
        };

        client.send_command(&Command::InitFileTransfer("a".to_string(), file_info)).await?;
        client.send_data_from_buffer(file_size, &mut file_content).await?;
        
        let response = client.next_command().await?.unwrap();
        assert_eq!(response, Command::FileTransferSuccess);

        drop(client);

        let file_meta = std::fs::metadata("./samples/peer_a/subpath/new_file.txt")?;
        assert_eq!(file_meta.len(), file_size);
        std::fs::remove_dir_all("./samples/peer_a/subpath")?;
        
        Ok(())
    }

    #[tokio::test]
    async fn server_enqueue_sync() -> Result<(), Box<dyn Error>> {
        let mut receiver = init_server("port = 8093
        peers = []
        
        [paths]
        a = \"./samples/peer_a\"");

        let client_one = tokio::spawn(async {
            let mut client = CommandStream::new( TcpStream::connect("localhost:8093").await.unwrap());
            client.send_command(&Command::TryInitSync).await.unwrap();
            assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);

            client.send_command(&Command::SyncFinished(HashMap::new())).await.unwrap();
            assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);
        });
        
        let client_two = tokio::spawn(async {
            let mut client = CommandStream::new( TcpStream::connect("localhost:8093").await.unwrap());
            client.send_command(&Command::TryInitSync).await.unwrap();
            assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);

            client.send_command(&Command::SyncFinished(HashMap::new())).await.unwrap();
            assert_eq!(client.next_command().await.unwrap().unwrap(), Command::CommandSuccess);
        });

        tokio::spawn(async move {
            while let Some(x) = receiver.recv().await {
                match x {
                    SyncEvent::PeerRequestedSync(_, notify) => {
                        notify.notify_one()
                    }
                    _ => {}
                }
            }
        });


        client_one.await.unwrap();
        client_two.await.unwrap();

        Ok(())
    }

}