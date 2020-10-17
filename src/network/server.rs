use std::{error::Error, io::{Write, Read}, net::{TcpListener, TcpStream, Shutdown}, path::PathBuf, thread};
use crate::{DEFAULT_PORT, RSyncError, config::Config, fs::FileInfo};

use super::{BUFFER_SIZE, Command, CommandResult, Response};

// pub enum ServerStatus {
//     Idle,
//     Listening
// }

pub struct Server<'a> {
    port: u32,
    // status: ServerStatus,
    config: &'a Config
}

struct ServerPeerHandler<'a>  {
    config: &'a Config,
    stream: TcpStream
}

impl <'a> Server<'a> {
    pub fn new(config: &'a Config) -> Self {
        let port = match config.port {
            Some(port) => port,
            None => DEFAULT_PORT
        };

        Server {
            port,
            // status: ServerStatus::Idle,
            config
        }
    }

    pub fn wait_commands(&self) -> Result<(), RSyncError> {
        let listener = match TcpListener::bind(format!("127.0.0.1:{}", self.port)) {
            Ok(listener) => listener,
            Err(err) => return Err(RSyncError::CantStartServer(Box::new(err)))
        };

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => { 
                    println!("New connection from {}", stream.peer_addr().unwrap());

                    let config= self.config.clone();
                    thread::spawn(move || {
                        let mut handler = ServerPeerHandler::new(&config, stream);
                        handler.handle_events()
                    }); 
                }
                Err(_) => {}
            }
        }

        Ok(())
    }
}

impl <'a> ServerPeerHandler<'a> {
    fn new(config: &'a Config, stream: TcpStream) -> Self {
        ServerPeerHandler {
            config,
            stream
        }
    }

    fn reply(&self, resp: Response) {
        match bincode::serialize_into(&self.stream, &resp) {
            Ok(_) => {}
            Err(_) => { eprintln!("Some error ocurred replying to peer"); }
        }
    }

    fn write_file(&mut self, alias: String, file_info: FileInfo) -> Result<(), Box<dyn Error>> {
        let mut temp_path = PathBuf::new();
        let mut final_path = PathBuf::new();

        match self.config.paths.get(&alias) {
            Some(path) => { 
                temp_path.push(path);
                final_path.push(path);
            }
            None => { 
                eprintln!("invalid alias");
                return Err(Box::new(RSyncError::InvalidAlias(alias)));
            }
        }
        
        final_path.push(&file_info.path);
        temp_path.push(&file_info.path);

        temp_path.set_extension("r-synctemp");

        let mut file = std::fs::File::create(&temp_path)?;
        let mut buf = [0u8; BUFFER_SIZE];
        let mut curr_size = 0u64;

        loop {
            match self.stream.read(&mut buf) {
                Ok(size) => {
                    curr_size += size as u64;
                    if size > 0 && curr_size <= file_info.size {
                        file.write(&buf[..size])?;
                    } else {
                        break;
                    }
                    
                }
                Err(_) => { break; }
            }
        }

        file.flush()?;

        drop(file);

        std::fs::rename(&temp_path, &final_path)?;
        filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(file_info.modified_at))?;

        Ok(())
    }


    fn handle_events(&mut self) {
        self.stream.set_read_timeout(Some(std::time::Duration::from_secs(1)));
        // self.stream.set_nonblocking(true);

        loop { 
            let incoming: CommandResult = bincode::deserialize_from(&self.stream);
            match incoming {
                Ok(cmd) => {
                    println!("Received Command {:?}", cmd);
                    match cmd {
                        Command::Ping => { self.reply(Response::Pong); }
                        Command::Close => { 
                            self.stream.shutdown(Shutdown::Both );
                            break;
                        }
                        Command::UnsyncedFileList(alias_hash) => {
                            let mut response = Vec::new();
                            for ( alias, hash) in alias_hash {
                                let path = match self.config.paths.get(&alias) {
                                    Some(path) => path,
                                    None => { continue; }
                                };

                                match crate::fs::get_files_with_hash(path) {
                                    Ok((local_hash, files)) => { 
                                        if local_hash != hash {
                                            response.push((alias, files)); 
                                        }
                                    }
                                    Err(_) => {

                                    }
                                }
                            }

                            self.reply(Response::FileList(response));
                        }
                        Command::PrepareFile(alias, file_info) => { 
                            match self.write_file(alias, file_info) {
                                Ok(_) => { println!("File synced") }
                                Err(err) => { eprintln!("Failed to sync file - {}", err) }
                            }

                            self.reply(Response::Success);
                        }
                    }
                }
                Err(err) => {
                    // eprintln!("{:?}", err);
                    // break;
                }
            }
        }
    }

}


#[cfg(test)] 
mod tests {
    use std::error::Error;
    use thread::JoinHandle;

    use crate::network::ResponseResult;
    use super::*;
    
    fn start_test_server(config_path: String) -> JoinHandle<()> {
        let config = Config::new(config_path).unwrap();
        let handle = thread::spawn(move || {
            let server = Server::new(&config);
            match server.wait_commands() {
                Ok(_) => {}
                Err(_) => {}
            }
        });
        
        thread::sleep(std::time::Duration::from_millis(10));

        handle
    }
    #[test]
    fn server_reply_ping() { 
        start_test_server("./samples/config_peer_a.toml".to_string());
       
        let stream = TcpStream::connect("localhost:8090").unwrap();
        bincode::serialize_into(&stream, &Command::Ping).expect("failed to send command");

        let response: ResponseResult = bincode::deserialize_from(&stream);
        match response {
            Ok(response) => { assert_eq!(response, Response::Pong) }
            Err(err) => { panic!(err) }
        }

        bincode::serialize_into(&stream, &Command::Close).expect("failed to send command");
    }

    #[test]
    fn server_reply_files() -> Result<(), Box<dyn Error>> {
        start_test_server("./samples/config_peer_b.toml".to_string());

        let stream = TcpStream::connect("localhost:8091").unwrap();
        let command_payload  = vec![("a".to_owned(), 0u64), ("b".to_owned(), 0)];

        bincode::serialize_into(&stream, &Command::UnsyncedFileList(command_payload)).expect("failed to send command");
        let response: Response = bincode::deserialize_from(&stream)?;
        match response {
            Response::FileList(path_files) => {
                assert_eq!(path_files.len(), 1);
                let (name, files) = path_files.get(0).unwrap();
                assert_eq!(name, "a");
                assert_eq!(files.len(), 1);

            },
            _ => panic!("wrong response")
        };

        Ok(())
    }

    #[test]
    fn server_can_receive_files() -> Result<(), Box<dyn Error>> {
        start_test_server("./samples/config_peer_a.toml".to_string());

        let mut stream = TcpStream::connect("localhost:8090").unwrap();
        let file_info = FileInfo { 
            path: std::path::PathBuf::from("new_file.txt"),
            size: 1024,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            modified_at: std::time::SystemTime::UNIX_EPOCH
        };

        bincode::serialize_into(&stream, &Command::PrepareFile("a".to_string(),  file_info)).expect("failed to send command");
        let buf = [255u8; 1024];
        
        stream.write(&buf)?;

        let response: ResponseResult = bincode::deserialize_from(&stream);
        match response? {
            Response::Success => {  }
            _ => panic!("failed")
        }

        bincode::serialize_into(&stream, &Command::Close).expect("failed to send command");

        let file_meta = std::fs::metadata("./samples/peer_a/new_file.txt")?;
        assert_eq!(file_meta.len(), 1024);
        std::fs::remove_file("./samples/peer_a/new_file.txt")?;
        
        Ok(())
    }

}