use std::{net::SocketAddr, sync::Arc, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::AsyncWriteExt,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
};
use tokio_stream::StreamExt;

use crate::{
    config::Config, constants::PEER_IDENTIFICATION_TIMEOUT, network_events::NetworkEvents,
    IronCarrierError,
};
use connection::{Connection, ReadHalf};

use self::{connection_storage::ConnectionStorage, network_event_decoder::NetWorkEventDecoder};

mod connection;
mod connection_storage;
mod network_event_decoder;
pub mod service_discovery;

pub struct ConnectionHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    config: &'static Config,
    connections: Arc<Mutex<connection_storage::ConnectionStorage>>,

    stream_sender: Sender<(u64, T)>,
    stream_receiver: Arc<Mutex<Receiver<(u64, T)>>>,
}

impl ConnectionHandler<NetworkEvents> {
    pub async fn new(config: &'static Config) -> crate::Result<ConnectionHandler<NetworkEvents>> {
        let (stream_sender, stream_receiver) = tokio::sync::mpsc::channel(100);
        let connections: Arc<Mutex<ConnectionStorage>> = Default::default();

        tokio::spawn(cleanup_stale_connections(connections.clone()));

        let inbound_fut = listen_connections(config, stream_sender.clone(), connections.clone());

        tokio::spawn(async move {
            if let Err(err) = inbound_fut.await {
                log::error!("{err}");
            }
        });
        let stream_receiver = Mutex::new(stream_receiver).into();

        Ok(Self {
            config,
            connections,
            stream_sender,
            stream_receiver,
        })
    }

    pub async fn connect(&self, addr: SocketAddr, peer_id: Option<u64>) -> crate::Result<u64> {
        if let Some(peer_id) = peer_id {
            if self.connections.lock().await.contains_peer(&peer_id) {
                log::info!("Already connected to {peer_id}");
                return Ok(peer_id);
            }
        }

        async fn connect_inner(
            config: &'static Config,
            addr: SocketAddr,
        ) -> crate::Result<Connection> {
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(Some(Duration::from_secs(3)))
                .build();

            let transport_stream = backoff::future::retry(backoff, || async {
                // TODO: add transport layer encryption
                tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(backoff::Error::from)
            })
            .await?;

            connection::identify_outgoing_connection(config, transport_stream)
                .await
                .map_err(Box::from)
        }

        let connection = tokio::time::timeout(
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connect_inner(self.config, addr),
        )
        .await
        .map_err(|_| IronCarrierError::ConnectionTimeout)??;

        let peer_id = connection.peer_id;
        append_connection(
            self.stream_sender.clone(),
            self.connections.clone(),
            connection,
        )
        .await;

        Ok(peer_id)
    }

    pub async fn send_to(&self, event: NetworkEvents, peer_id: u64) -> crate::Result<()> {
        if let Some(connection) = self.connections.lock().await.get_mut(&peer_id) {
            let bytes = bincode::serialize(&event)?;
            connection.write_u8(0).await?;
            connection.write_u32(bytes.len() as u32).await?;
            connection.write_all(&bytes).await?;
        }

        Ok(())
    }

    pub async fn next_event(&self) -> Option<(u64, NetworkEvents)> {
        self.stream_receiver.lock().await.recv().await
    }

    pub async fn broadcast(&self, event: NetworkEvents) -> crate::Result<usize> {
        let bytes = bincode::serialize(&event)?;
        let mut connections = self.connections.lock().await;
        for connection in connections.connections_mut() {
            connection.write_u8(0).await?;
            connection.write_u32(bytes.len() as u32).await?;
            connection.write_all(&bytes).await?;
        }

        Ok(connections.len())
    }

    pub async fn broadcast_to(
        &self,
        event: NetworkEvents,
        nodes: impl Iterator<Item = &u64>,
    ) -> crate::Result<()> {
        let bytes = bincode::serialize(&event)?;
        let mut connections = self.connections.lock().await;
        for node in nodes {
            if let Some(connection) = connections.get_mut(node) {
                connection.write_u8(0).await?;
                connection.write_u32(bytes.len() as u32).await?;
                connection.write_all(&bytes).await?;
            }
        }

        Ok(())
    }

    pub async fn stream_to(
        &self,
        transfer_id: u64,
        block_index: u64,
        block: &[u8],
        nodes: impl Iterator<Item = &u64>,
    ) -> crate::Result<()> {
        let mut connections = self.connections.lock().await;
        for node in nodes {
            if let Some(connection) = connections.get_mut(node) {
                connection.write_u8(1).await?;
                connection.write_u64(transfer_id).await?;
                connection.write_u64(block_index).await?;
                connection.write_u32(block.len() as u32).await?;
                connection.write_all(block).await?;
            }
        }

        Ok(())
    }

    pub async fn close_all_connections(&self) -> crate::Result<()> {
        self.connections.lock().await.clear();
        Ok(())
    }
}

async fn append_connection(
    event_stream: Sender<(u64, NetworkEvents)>,
    connections: Arc<Mutex<ConnectionStorage>>,
    connection: Connection,
) {
    let mut connections_guard = connections.lock().await;
    if connections_guard.contains_peer(&connection.peer_id) {
        log::info!("Already connected to {}", connection.peer_id);
        return;
    }

    let (write, read) = connection.split();
    tokio::spawn(read_network_data(
        read,
        event_stream.clone(),
        connections.clone(),
    ));

    log::info!("Connected to {}", write.peer_id);
    connections_guard.insert(write);
}

async fn cleanup_stale_connections(connections: Arc<Mutex<ConnectionStorage>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        connections.lock().await.remove_stale()
    }
}

async fn listen_connections(
    config: &Config,
    event_stream: Sender<(u64, NetworkEvents)>,
    connections: Arc<Mutex<ConnectionStorage>>,
) -> crate::Result<()> {
    log::debug!("Listening on {}", config.port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    while let Ok((stream, _addr)) = listener.accept().await {
        let connection = match tokio::time::timeout(
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connection::identify_incoming_connection(config, stream),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            _ => {
                log::error!("{}", IronCarrierError::ConnectionTimeout);
                continue;
            }
        };

        append_connection(event_stream.clone(), connections.clone(), connection).await;
    }

    Ok(())
}

async fn read_network_data(
    read_connection: ReadHalf,
    event_stream: Sender<(u64, NetworkEvents)>,
    connections: Arc<Mutex<ConnectionStorage>>,
) {
    let peer_id = read_connection.peer_id;
    let connection_id = read_connection.connection_id;

    let mut stream = tokio_util::codec::FramedRead::new(read_connection, NetWorkEventDecoder {});
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                if let Err(err) = event_stream.send((peer_id, event)).await {
                    log::error!("Error sending event to event stream {err}");
                    break;
                }
            }
            Err(err) => {
                log::error!("error reading from peer {err}");
            }
        }
    }

    if let Err(err) = event_stream
        .send((peer_id, NetworkEvents::Disconnected))
        .await
    {
        log::error!("Error sending event to event stream {err}");
    }

    connections.lock().await.remove(&connection_id);
}
