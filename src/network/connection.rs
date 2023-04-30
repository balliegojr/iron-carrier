use std::{pin::Pin, sync::atomic::AtomicU32};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::Instant,
};

use crate::{
    config::Config,
    constants::{PEER_STALE_CONNECTION, VERSION},
    hash_helper,
    node_id::NodeId,
    IronCarrierError,
};

static CONNECTION_ID_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub struct ConnectionId(u32);

pub struct Connection {
    stream: tokio::net::TcpStream,
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
}

impl Connection {
    pub fn new(stream: tokio::net::TcpStream, node_id: NodeId) -> Self {
        let connection_id = CONNECTION_ID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Connection {
            node_id,
            connection_id: ConnectionId(connection_id),
            stream,
        }
    }

    pub fn split(self) -> (WriteHalf, ReadHalf) {
        let (read, write) = self.stream.into_split();

        (
            WriteHalf {
                inner: Box::pin(write),
                connection_id: self.connection_id,
                node_id: self.node_id,
                last_access: Instant::now(),
            },
            ReadHalf {
                inner: Box::pin(read),
                connection_id: self.connection_id,
                node_id: self.node_id,
            },
        )
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("connection_id", &self.connection_id)
            .field("node_id", &self.node_id)
            .finish()
    }
}

pub struct WriteHalf {
    inner: Pin<Box<dyn AsyncWrite + Send + Sync>>,
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
    last_access: Instant,
}

impl WriteHalf {
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }

    pub fn is_stale(&self) -> bool {
        (Instant::now() - self.last_access).as_secs() > PEER_STALE_CONNECTION
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        AsyncWrite::poll_write(self.inner.as_mut(), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        AsyncWrite::poll_flush(self.inner.as_mut(), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        AsyncWrite::poll_shutdown(self.inner.as_mut(), cx)
    }
}

pub struct ReadHalf {
    inner: Pin<Box<dyn AsyncRead + Send + Sync>>,
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncRead::poll_read(self.inner.as_mut(), cx, buf)
    }
}

pub async fn identify_outgoing_connection(
    config: &Config,
    mut stream: tokio::net::TcpStream,
) -> crate::Result<Connection> {
    let version = hash_helper::hashed_str(VERSION);
    if !round_trip_compare(&mut stream, version).await? {
        return Err(Box::new(IronCarrierError::VersionMismatch));
    }

    let group = config
        .group
        .as_ref()
        .map(hash_helper::hashed_str)
        .unwrap_or_default();

    if !round_trip_compare(&mut stream, group).await? {
        return Err(Box::new(IronCarrierError::GroupMismatch));
    }

    stream.write_u64(config.node_id_hashed.into()).await?;
    let node_id = stream.read_u64().await?;

    Ok(Connection::new(stream, node_id.into()))
}

pub async fn identify_incoming_connection(
    config: &Config,
    mut stream: tokio::net::TcpStream,
) -> crate::Result<Connection> {
    let version = hash_helper::hashed_str(VERSION);
    wait_and_compare(&mut stream, version, || IronCarrierError::VersionMismatch).await?;

    let group = config
        .group
        .as_ref()
        .map(hash_helper::hashed_str)
        .unwrap_or_default();

    wait_and_compare(&mut stream, group, || IronCarrierError::GroupMismatch).await?;

    stream.write_u64(config.node_id_hashed.into()).await?;
    let node_id = stream.read_u64().await?;

    Ok(Connection::new(stream, node_id.into()))
}

async fn round_trip_compare(stream: &mut tokio::net::TcpStream, value: u64) -> crate::Result<bool> {
    stream.write_u64(value).await?;
    stream
        .read_u64()
        .await
        .map(|result| result == value)
        .map_err(Box::from)
}

async fn wait_and_compare(
    stream: &mut tokio::net::TcpStream,
    value: u64,
    err: fn() -> IronCarrierError,
) -> crate::Result<()> {
    if stream.read_u64().await? == value {
        stream.write_u64(value).await.map_err(Box::from)
    } else {
        Err(Box::new(err()))
    }
}
