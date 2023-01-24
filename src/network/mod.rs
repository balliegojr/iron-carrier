use std::{
    borrow::BorrowMut, collections::HashMap, io::ErrorKind, net::SocketAddr, pin::Pin, sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{
        broadcast,
        mpsc::{Receiver, Sender},
        Mutex,
    },
};
use tokio_stream::{Stream, StreamExt};

use crate::{config::Config, hash_helper, network_events::NetworkEvents};

pub mod service_discovery;

type Connections = Arc<Mutex<HashMap<ConnectionId, Pin<Box<dyn AsyncWrite + Send>>>>>;
type PeerConnections = Arc<Mutex<HashMap<u64, ConnectionId>>>;

pub struct ConnectionHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    config: &'static Config,
    connections: Connections,
    peer_connection: PeerConnections,

    inbound_sender: broadcast::Sender<(u64, T)>,
    disconnected: Sender<ConnectionId>,
}

impl ConnectionHandler<NetworkEvents> {
    pub async fn new(config: &'static Config) -> crate::Result<ConnectionHandler<NetworkEvents>> {
        let (inbound_sender, _inbound_receiver) = tokio::sync::broadcast::channel(100);
        // let (inbound_sender, inbound_receiver) = tokio::sync::mpsc::channel(100);
        let connections: Connections = Default::default();
        let peer_connection: PeerConnections = Default::default();

        let (disconnected, on_disconnected) = tokio::sync::mpsc::channel(1);

        let inbound_fut = connection_handler_inbound(
            config,
            inbound_sender.clone(),
            connections.clone(),
            peer_connection.clone(),
            on_disconnected,
            disconnected.clone(),
        );

        tokio::spawn(async move {
            if let Err(err) = inbound_fut.await {
                log::error!("{err}");
            }
        });

        Ok(Self {
            config,
            connections,
            peer_connection,
            inbound_sender,
            disconnected,
        })
    }

    pub async fn connect(&self, addr: &SocketAddr) -> crate::Result<u64> {
        let transport_stream = if self.config.transport_encryption {
            // TODO: add transport layer encryption
            tokio::net::TcpStream::connect(addr).await?
        } else {
            tokio::net::TcpStream::connect(addr).await?
        };

        let (read, write) = transport_stream.into_split();
        let connection = UnidentifiedConnection {
            inner: Connection {
                read: Box::pin(read),
                write: Box::pin(write),
            },
        };

        let connection = connection.handshake(self.config).await?;
        let peer_id = connection.peer_id;
        append_connection(
            self.connections.lock().await.borrow_mut(),
            self.peer_connection.lock().await.borrow_mut(),
            connection,
            self.inbound_sender.clone(),
            self.disconnected.clone(),
        );

        Ok(peer_id)
    }

    pub async fn disconnect(&self, peer_id: u64) {
        let mut peer_connection = self.peer_connection.lock().await;
        let mut connections = self.connections.lock().await;

        if let Some(connection_id) = peer_connection.remove(&peer_id) {
            connections.remove(&connection_id);
        }
    }

    pub async fn send_to(&self, event: NetworkEvents, peer_id: u64) -> crate::Result<()> {
        if let Some(connection_id) = self.peer_connection.lock().await.get(&peer_id) {
            if let Some(connection) = self.connections.lock().await.get_mut(connection_id) {
                let bytes = bincode::serialize(&event)?;
                connection.write_u64(bytes.len() as u64).await?;
                connection.write_all(&bytes).await?;
            }
        }

        Ok(())
    }

    pub async fn events_stream(&self) -> impl Stream<Item = (u64, NetworkEvents)> {
        let receiver = self.inbound_sender.subscribe();

        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);
        stream.filter_map(|ev| ev.ok())
    }

    pub async fn connected_peers(&self) -> Vec<u64> {
        self.peer_connection.lock().await.keys().copied().collect()
    }

    pub async fn broadcast(&self, event: NetworkEvents) -> crate::Result<usize> {
        let bytes = bincode::serialize(&event)?;
        let mut connections = self.connections.lock().await;
        for connection in connections.values_mut() {
            connection.write_u64(bytes.len() as u64).await?;
            connection.write_all(&bytes).await?;
        }

        Ok(connections.len())
    }

    pub async fn broadcast_to(&self, event: NetworkEvents, nodes: &[u64]) -> crate::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        let bytes = bincode::serialize(&event)?;
        let peer_connections = self.peer_connection.lock().await;
        let mut connections = self.connections.lock().await;
        for node in nodes {
            if let Some(connection_id) = peer_connections.get(node) {
                if let Some(connection) = connections.get_mut(connection_id) {
                    connection.write_u64(bytes.len() as u64).await?;
                    connection.write_all(&bytes).await?;
                }
            }
        }

        Ok(())
    }
}

async fn connection_handler_inbound(
    config: &Config,
    event_stream: broadcast::Sender<(u64, NetworkEvents)>,
    connections: Connections,
    peer_connection: PeerConnections,
    mut on_disconnected: Receiver<ConnectionId>,
    disconnected: Sender<ConnectionId>,
) -> crate::Result<()> {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    loop {
        tokio::select! {
            new_connection = listener.accept() => {
                match new_connection {
                    Ok((stream, _addr)) => {
                        let (read, write) = stream.into_split();
                        let connection = UnidentifiedConnection {
                            inner: Connection {
                                read: Box::pin(read),
                                write: Box::pin(write),
                            },
                        };

                        let connection = match connection.handshake(config).await {
                            Ok(connection) => connection,
                            Err(_) => continue,
                        };

                        append_connection(
                            connections.lock().await.borrow_mut(),
                            peer_connection.lock().await.borrow_mut(),
                            connection,
                            event_stream.clone(),
                            disconnected.clone()
                        );
                    }
                    Err(_) => continue,
                }
            }
            connection_id = on_disconnected.recv() => {
                if let Some(connection_id) = connection_id {
                    connections.lock().await.remove(&connection_id);
                    peer_connection.lock().await.retain(|_p, c| c.0 != connection_id.0)
                }
            }
        }
    }
}

fn append_connection(
    connections: &mut HashMap<ConnectionId, Pin<Box<dyn AsyncWrite + Send>>>,
    peer_connection: &mut HashMap<u64, ConnectionId>,
    connection: IdenfiedConnection,
    event_stream: broadcast::Sender<(u64, NetworkEvents)>,
    disconnected: Sender<ConnectionId>,
) {
    if peer_connection.contains_key(&connection.peer_id) {
        return;
    }

    peer_connection.insert(connection.peer_id, connection.connection_id);
    connections.insert(
        connection.connection_id,
        connection.listen_events(event_stream, disconnected),
    );
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
struct ConnectionId(u32);

struct Connection {
    read: Pin<Box<dyn AsyncRead + Send>>,
    write: Pin<Box<dyn AsyncWrite + Send>>,
}

struct UnidentifiedConnection {
    inner: Connection,
}

// TODO: do this properly
static mut CONNECTION_ID_COUNT: u32 = 0;

impl UnidentifiedConnection {
    pub async fn handshake(mut self, config: &Config) -> crate::Result<IdenfiedConnection> {
        let group = config
            .group
            .as_ref()
            .map(|group| hash_helper::calculate_checksum(group.as_bytes()))
            .unwrap_or_default();

        self.inner.write.write_u64(group).await?;
        if self.inner.read.read_u64().await? != group {
            todo!("invalid group error")
        }

        self.inner.write.write_u64(config.node_id_hashed).await?;
        let peer_id = self.inner.read.read_u64().await?;

        let connection_id = unsafe {
            CONNECTION_ID_COUNT += 1;
            CONNECTION_ID_COUNT
        };

        Ok(IdenfiedConnection {
            peer_id,
            connection_id: ConnectionId(connection_id),
            inner: self.inner,
        })
    }
}

struct IdenfiedConnection {
    connection_id: ConnectionId,
    peer_id: u64,
    inner: Connection,
}

impl IdenfiedConnection {
    pub fn listen_events(
        self,
        event_stream: broadcast::Sender<(u64, NetworkEvents)>,
        disconnected: Sender<ConnectionId>,
    ) -> Pin<Box<dyn AsyncWrite + Send>> {
        let mut read = self.inner.read;
        tokio::spawn(async move {
            let mut read_buf = [0u8; 1024];
            loop {
                match read.read_u64().await {
                    Ok(to_read) => {
                        // TODO: handle reading properly
                        if let Err(err) = read.read_exact(&mut read_buf[..to_read as usize]).await {
                            if err.kind() == ErrorKind::UnexpectedEof {
                                log::debug!("Peer {} disconnected", self.peer_id);
                                disconnected.send(self.connection_id).await;
                                break;
                            } else {
                                log::error!("{err}");
                            }
                        }

                        if let Ok(event) = bincode::deserialize(&read_buf[..to_read as usize]) {
                            match event_stream.send((self.peer_id, event)) {
                                Ok(sent_to) => {
                                    // log::debug!("Sent event to {sent_to} listeners");
                                }
                                Err(err) => {
                                    log::error!("Failed to broadcast internal event {err}");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if err.kind() == ErrorKind::UnexpectedEof {
                            log::debug!("Peer {} disconnected", self.peer_id);
                            disconnected.send(self.connection_id).await;
                            break;
                        } else {
                            log::error!("{err}");
                        }
                    }
                }
            }
        });

        self.inner.write
    }
}

// struct ReceiverStream<T> {
//     receiver: broadcast::Receiver<T>,
// }
//
// impl<T> Stream for ReceiverStream<T>
// where
//     T: Clone + std::fmt::Debug,
// {
//     type Item = T;
//
//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         let mut fut = Box::pin(self.receiver.recv());
//         fut.as_mut().poll(cx).map(|value| dbg!(value).ok())
//     }
// }
