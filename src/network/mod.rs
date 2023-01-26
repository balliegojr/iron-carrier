use std::{io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{
        broadcast,
        mpsc::{Receiver, Sender},
        Mutex,
    },
};
use tokio_stream::{Stream, StreamExt};

use crate::{
    config::Config, constants::PEER_IDENTIFICATION_TIMEOUT, network_events::NetworkEvents,
    IronCarrierError,
};
use connection::{Connection, ConnectionId, ReadHalf};

use self::connection_storage::ConnectionStorage;

mod connection;
mod connection_storage;
pub mod service_discovery;

#[derive(Debug)]
enum ConnectionEvent {
    Connected(Connection),
    Disconnected(ConnectionId),
}

pub struct ConnectionHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    config: &'static Config,
    connections: Arc<Mutex<connection_storage::ConnectionStorage>>,

    inbound_sender: broadcast::Sender<(u64, T)>,
    connection_events: Sender<ConnectionEvent>,
}

impl ConnectionHandler<NetworkEvents> {
    pub async fn new(config: &'static Config) -> crate::Result<ConnectionHandler<NetworkEvents>> {
        let (inbound_sender, _inbound_receiver) = tokio::sync::broadcast::channel(100);
        let connections: Arc<Mutex<ConnectionStorage>> = Default::default();

        tokio::spawn(cleanup_stale_connections(connections.clone()));

        let (connection_events_tx, connection_events_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(connection_events(
            connection_events_rx,
            inbound_sender.clone(),
            connection_events_tx.clone(),
            connections.clone(),
        ));

        let inbound_fut = listen_connections(config, connection_events_tx.clone());

        tokio::spawn(async move {
            if let Err(err) = inbound_fut.await {
                log::error!("{err}");
            }
        });

        Ok(Self {
            config,
            connections,
            inbound_sender,
            connection_events: connection_events_tx,
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
        let connection = tokio::time::timeout(
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connection::identify_outgoing_connection(self.config, Box::pin(read), Box::pin(write)),
        )
        .await
        .map_err(|_| IronCarrierError::ConnectionTimeout)??;

        let peer_id = connection.peer_id;
        self.connection_events
            .send(ConnectionEvent::Connected(connection))
            .await?;

        Ok(peer_id)
    }

    pub async fn send_to(&self, event: NetworkEvents, peer_id: u64) -> crate::Result<()> {
        if let Some(connection) = self.connections.lock().await.get_mut(&peer_id) {
            let bytes = bincode::serialize(&event)?;
            connection.write_u64(bytes.len() as u64).await?;
            connection.write_all(&bytes).await?;
        }

        Ok(())
    }

    pub async fn events_stream(&self) -> impl Stream<Item = (u64, NetworkEvents)> {
        let receiver = self.inbound_sender.subscribe();

        let stream = tokio_stream::wrappers::BroadcastStream::new(receiver);
        stream.filter_map(|ev| ev.ok())
    }

    pub async fn broadcast(&self, event: NetworkEvents) -> crate::Result<usize> {
        let bytes = bincode::serialize(&event)?;
        let mut connections = self.connections.lock().await;
        for connection in connections.connections_mut() {
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
        let mut connections = self.connections.lock().await;
        for node in nodes {
            if let Some(connection) = connections.get_mut(node) {
                connection.write_u64(bytes.len() as u64).await?;
                connection.write_all(&bytes).await?;
            }
        }

        Ok(())
    }
}

async fn connection_events(
    mut connection_events: Receiver<ConnectionEvent>,
    event_stream: broadcast::Sender<(u64, NetworkEvents)>,
    connection_events_sender: Sender<ConnectionEvent>,
    connections: Arc<Mutex<ConnectionStorage>>,
) {
    while let Some(event) = connection_events.recv().await {
        match event {
            ConnectionEvent::Connected(connection) => {
                let mut connections = connections.lock().await;
                if connections.contains_peer(&connection.peer_id) {
                    continue;
                }

                let (write, read) = connection.split();
                tokio::spawn(read_network_data(
                    read,
                    event_stream.clone(),
                    connection_events_sender.clone(),
                ));

                connections.insert(write);
            }
            ConnectionEvent::Disconnected(connection_id) => {
                connections.lock().await.remove(&connection_id);
            }
        }
    }
}

async fn cleanup_stale_connections(connections: Arc<Mutex<ConnectionStorage>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        connections.lock().await.remove_stale()
    }
}

async fn listen_connections(
    config: &Config,
    connection_events: Sender<ConnectionEvent>,
) -> crate::Result<()> {
    // TODO: handle re-connection
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    while let Ok((stream, _addr)) = listener.accept().await {
        let (read, write) = stream.into_split();
        let connection = match tokio::time::timeout(
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connection::identify_incoming_connection(config, Box::pin(read), Box::pin(write)),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            _ => {
                log::error!("{}", IronCarrierError::ConnectionTimeout);
                continue;
            }
        };

        connection_events
            .send(ConnectionEvent::Connected(connection))
            .await?
    }

    Ok(())
}

async fn read_network_data(
    mut read_connection: ReadHalf,
    event_stream: broadcast::Sender<(u64, NetworkEvents)>,
    connection_events: Sender<ConnectionEvent>,
) {
    let mut read_buf = [0u8; 1024];
    loop {
        match read_connection.read_u64().await {
            Ok(to_read) => {
                // TODO: handle reading properly
                if let Err(err) = read_connection
                    .read_exact(&mut read_buf[..to_read as usize])
                    .await
                {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        log::debug!("Peer {} disconnected", read_connection.peer_id);
                        break;
                    } else {
                        log::error!("{err}");
                    }
                }

                if let Ok(event) = bincode::deserialize(&read_buf[..to_read as usize]) {
                    match event_stream.send((read_connection.peer_id, event)) {
                        Ok(_sent_to) => {
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
                    log::debug!("Peer {} disconnected", read_connection.peer_id);
                    break;
                } else {
                    log::error!("{err}");
                }
            }
        }
    }

    let _ = connection_events
        .send(ConnectionEvent::Disconnected(read_connection.connection_id))
        .await;
}
