use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpListener, net::TcpStream, sync::mpsc::Sender, sync::Mutex};

use crate::{config::Config, sync::file_events_buffer::FileEventsBuffer, sync::SyncEvent};

use self::server_peer_handler::ServerPeerHandler;

use super::streaming::{file_streamers, frame_stream};

mod server_peer_handler;

pub(crate) struct Server {
    port: u32,
    config: Arc<Config>,
    file_events: Arc<FileEventsBuffer>,
    handlers: Arc<Mutex<HashMap<String, TcpStream>>>,
}

impl Server {
    pub fn new(config: Arc<Config>, file_events: Arc<FileEventsBuffer>) -> Self {
        Server {
            port: config.port,
            config,
            file_events,
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&mut self, sync_events: Sender<SyncEvent>) -> crate::Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;

        log::info!("Server listening on port: {}", self.port);

        let config = self.config.clone();
        let file_events = self.file_events.clone();
        let handlers = self.handlers.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, socket)) => {
                        let sync_events = sync_events.clone();
                        let config = config.clone();
                        let file_events = file_events.clone();

                        let socket_addr = socket.ip().to_string();
                        log::info!("New connection from {}", &socket_addr);

                        let mut handlers = handlers.lock().await;
                        if !handlers.contains_key(&socket_addr) {
                            handlers.insert(socket_addr, stream);
                        } else {
                            let command_stream = handlers.remove(&socket_addr).unwrap();
                            let file_stream = stream;

                            tokio::spawn(async move {
                                let (frame_reader, frame_writer) = frame_stream(command_stream);
                                let (file_receiver, file_sender) =
                                    file_streamers(file_stream, &config, socket_addr.clone());

                                let mut handler = ServerPeerHandler::new(
                                    &config,
                                    frame_reader,
                                    frame_writer,
                                    file_receiver,
                                    file_sender,
                                    socket_addr.clone(),
                                );

                                match handler.handle_events(sync_events, &file_events).await {
                                    Ok(()) => {
                                        log::info!("Peer connection closed: {}", socket_addr)
                                    }
                                    Err(err) => {
                                        log::error!("Some error ocurred while handling events from peer: {}", err)
                                    }
                                }

                                handler.close().await;
                            });
                        }
                    }
                    Err(_) => {}
                }
            }
        });

        Ok(())
    }
}
