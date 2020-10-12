use std::{net::{TcpListener, TcpStream, Shutdown}, thread};
use crate::{DEFAULT_PORT, RSyncError, config::Config};

use super::{ Command, Response, CommandResult };

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

    fn handle_events(&mut self) {
        self.stream.set_read_timeout(Some(std::time::Duration::from_secs(1)));

        loop { 
            let incoming: CommandResult = bincode::deserialize_from(&self.stream);
            match incoming {
                Ok(cmd) => {
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
                    }
                }
                Err(err) => {
                    // eprintln!("{:?}", err);
                    break;
                }
            }
        }
    }

}


#[cfg(test)] 
mod tests {
    use std::error::Error;
    use crate::network::ResponseResult;
    use super::*;
    
    fn start_test_server() {
        let config = Config::new("./samples/sample_config.toml".to_owned()).unwrap();
        thread::spawn(move || {
            let server = Server::new(&config);
            match server.wait_commands() {
                Ok(_) => {}
                Err(_) => {}
            }
        });
        
        thread::sleep(std::time::Duration::from_millis(10));
    }
    #[test]
    fn server_reply_ping() { 
        start_test_server();
       
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
        start_test_server();

        let stream = TcpStream::connect("localhost:8090").unwrap();
        let command_payload  = vec![("a".to_owned(), 0u64), ("b".to_owned(), 0)];

        bincode::serialize_into(&stream, &Command::UnsyncedFileList(command_payload)).expect("failed to send command");
        let response: Response = bincode::deserialize_from(&stream)?;
        match response {
            Response::FileList(path_files) => {
                assert_eq!(path_files.len(), 1);
                let (name, files) = path_files.get(0).unwrap();
                assert_eq!(name, "a");
                assert_eq!(files.len(), 5);

            },
            _ => panic!("wrong response")
        };

        Ok(())
    }

}