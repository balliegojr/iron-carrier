use std::path::PathBuf;
use tokio::{fs::File, net::TcpListener, net::TcpStream, prelude::*};

use crate::{
    DEFAULT_PORT, 
    RSyncError, 
    config::Config, 
    fs::FileInfo,
    network::stream::CommandStream
};

use super::stream::Command;


pub enum ServerStatus {
    Idle,
    Listening
}

pub struct Server<'a> {
    port: u32,
    status: ServerStatus,
    config: &'a Config
}

struct ServerPeerHandler<'a>  {
    config: &'a Config,
    stream: CommandStream<TcpStream>
}

impl <'a> Server<'a> {
    pub fn new(config: &'a Config) -> Self {
        let port = match config.port {
            Some(port) => port,
            None => DEFAULT_PORT
        };

        Server {
            port,
            config,
            status: ServerStatus::Idle
        }
    }


    pub async fn wait_commands(&mut self) -> Result<(), RSyncError> {
        
        let listener = match TcpListener::bind(format!("127.0.0.1:{}", self.port)).await {
            Ok(listener) => listener,
            Err(err) => return Err(RSyncError::CantStartServer(format!("{}", err)))
        };

        self.status = ServerStatus::Listening;
        while let ServerStatus::Listening = self.status  {
            match listener.accept().await {
                Ok((stream, _socket)) => { 
                    let config = self.config.clone();
                    println!("New connection from {}", stream.peer_addr().unwrap());

                    tokio::spawn(async move {
                        let mut handler = ServerPeerHandler::new(&config, CommandStream::new(stream));
                        handler.handle_events().await
                    });
                    
                }
                Err(_) => {}
            }
        }

        Ok(())
    }
}

impl <'a> ServerPeerHandler<'a> {
    fn new(config: &'a Config, stream: CommandStream<TcpStream>) -> Self {
        ServerPeerHandler {
            config,
            stream
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

    async fn get_file_list(&self, alias_hash: Vec<(String, u64)>) -> Vec<(String, Vec<FileInfo>)> {
        let mut response = Vec::new();
        for ( alias, hash) in alias_hash {
            let path = match self.config.paths.get(&alias) {
                Some(path) => path,
                None => { continue; }
            };

            match crate::fs::get_files_with_hash(path).await {
                Ok((local_hash, files)) => { 
                    if local_hash != hash {
                        response.push((alias, files)); 
                    }
                }
                Err(_) => {

                }
            }
        }

        return response;
    }

    async fn handle_events(&mut self) {
        loop { 
            match self.stream.next_command().await  {
                Ok(command) => {
                    match command {
                        Some(cmd) => {
                            match cmd {
                                super::stream::Command::Ping => { self.reply(Command::Pong).await }
                                super::stream::Command::FetchUnsyncedFileList(alias_hash) => {
                                    let response = self.get_file_list(alias_hash).await;
                                    self.reply(Command::FileList(response)).await;
                                }
                                super::stream::Command::InitFileTransfer(alias, file_info) => {
                                    match self.write_file(alias, file_info).await {
                                        Ok(_) => { self.reply(Command::FileTransferSuccess).await; }
                                        Err(_) => { self.reply(Command::FileTransferFailed).await; }
                                    };
                                }
                                _ => {
                                    println!("server invalid command");
                                }
                            }
                        }
                        None => {
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
    use tokio::net::TcpStream;
    
    fn init_server(cfg: &str) {
        let config = Config::parse_content(cfg.to_owned()).unwrap();
        tokio::spawn(async move {
            let mut server = Server::new(&config);
            server.wait_commands().await.unwrap();
        });

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
        let command_payload  = vec![("a".to_owned(), 0u64), ("b".to_owned(), 0)];

        client.send_command(&Command::FetchUnsyncedFileList(command_payload)).await?;
        let response = client.next_command().await?.unwrap();
        
        match response {
            Command::FileList(path_files) => {
                assert_eq!(path_files.len(), 1);
                let (name, files) = path_files.get(0).unwrap();
                assert_eq!(name, "a");
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

}