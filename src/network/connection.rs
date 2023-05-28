use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc,
    },
    time::SystemTime,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    config::Config,
    constants::{PEER_STALE_CONNECTION, VERSION},
    hash_helper,
    node_id::NodeId,
    time::system_time_to_secs,
    IronCarrierError,
};

static CONNECTION_ID_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub struct ConnectionId(u32);

pub struct Identified<T> {
    inner: T,
    node_id: NodeId,
}

impl<T> Identified<T> {
    pub fn new(inner: T, node_id: NodeId) -> Self {
        Self { inner, node_id }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for Identified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Identified<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct Connection {
    read_half: Pin<Box<dyn AsyncRead + Send>>,
    write_half: Pin<Box<dyn AsyncWrite + Send + Sync>>,
    pub connection_id: ConnectionId,
}

impl Connection {
    pub fn new(
        read_half: Pin<Box<dyn AsyncRead + Send>>,
        write_half: Pin<Box<dyn AsyncWrite + Send + Sync>>,
    ) -> Self {
        let read_half = tokio::io::BufReader::new(read_half);
        let write_half = tokio::io::BufWriter::new(write_half);

        let connection_id = CONNECTION_ID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Connection {
            connection_id: ConnectionId(connection_id),
            read_half: Box::pin(read_half),
            write_half: Box::pin(write_half),
        }
    }

    pub fn into_identified(self, node_id: NodeId) -> Identified<Self> {
        Identified::new(self, node_id)
    }
}

impl Identified<Connection> {
    pub fn split(self) -> (Identified<WriteHalf>, Identified<ReadHalf>) {
        let node_id = self.node_id;
        let connection_id = self.connection_id;
        let last_access = Arc::new(AtomicU64::new(system_time_to_secs(SystemTime::now())));

        (
            Identified::new(
                WriteHalf {
                    inner: self.inner.write_half,
                    connection_id,
                    last_access: last_access.clone(),
                },
                node_id,
            ),
            Identified::new(
                ReadHalf {
                    inner: Box::pin(self.inner.read_half),
                    connection_id,
                    last_access,
                },
                node_id,
            ),
        )
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("connection_id", &self.connection_id)
            .finish()
    }
}

pin_project_lite::pin_project! {
    pub struct WriteHalf {
        #[pin]
        inner: Pin<Box<dyn AsyncWrite + Send + Sync>>,
        pub connection_id: ConnectionId,
        last_access: Arc<AtomicU64>
    }
}

impl WriteHalf {
    fn touch(self: Pin<&mut Self>) {
        self.last_access.store(
            system_time_to_secs(SystemTime::now()),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    pub fn is_stale(&self) -> bool {
        let last_access = self.last_access.load(std::sync::atomic::Ordering::SeqCst);
        let now = system_time_to_secs(SystemTime::now());
        (now - last_access) > PEER_STALE_CONNECTION
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.as_mut().project().inner.poll_write(cx, buf) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.as_mut().project().inner.poll_flush(cx) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

pin_project_lite::pin_project! {
    pub struct ReadHalf {
        #[pin]
        inner: Pin<Box<dyn AsyncRead + Send>>,
        pub connection_id: ConnectionId,
        last_access: Arc<AtomicU64>
    }
}

impl ReadHalf {
    fn touch(self: Pin<&mut Self>) {
        self.last_access.store(
            system_time_to_secs(SystemTime::now()),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.as_mut().project();
        match me.inner.poll_read(cx, buf) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }
}

pub async fn identify_outgoing_connection(
    config: &Config,
    stream: tokio::net::TcpStream,
) -> crate::Result<Identified<Connection>> {
    let mut connection = match &config.encryption_key {
        Some(plain_key) => {
            let group_name = config
                .group
                .as_deref()
                .unwrap_or("no group for this session");
            let (read, write) = super::crypto_stream::crypto_stream(stream, plain_key, group_name);

            Connection::new(Box::pin(read), Box::pin(write))
        }
        None => {
            let (read, write) = stream.into_split();
            Connection::new(Box::pin(read), Box::pin(write))
        }
    };

    let version = hash_helper::hashed_str(VERSION);
    if !round_trip_compare(&mut connection, version).await? {
        return Err(Box::new(IronCarrierError::VersionMismatch));
    }

    let group = config
        .group
        .as_ref()
        .map(hash_helper::hashed_str)
        .unwrap_or_default();

    if !round_trip_compare(&mut connection, group).await? {
        return Err(Box::new(IronCarrierError::GroupMismatch));
    }

    connection
        .write_half
        .write_u64(config.node_id_hashed.into())
        .await?;
    connection.write_half.flush().await?;
    let node_id = connection.read_half.read_u64().await?;

    Ok(connection.into_identified(node_id.into()))
}

pub async fn identify_incoming_connection(
    config: &Config,
    stream: tokio::net::TcpStream,
) -> crate::Result<Identified<Connection>> {
    let mut connection = match &config.encryption_key {
        Some(plain_key) => {
            let group_name = config
                .group
                .as_deref()
                .unwrap_or("no group for this session");
            let (read, write) = super::crypto_stream::crypto_stream(stream, plain_key, group_name);

            Connection::new(Box::pin(read), Box::pin(write))
        }
        None => {
            let (read, write) = stream.into_split();
            Connection::new(Box::pin(read), Box::pin(write))
        }
    };
    let version = hash_helper::hashed_str(VERSION);

    wait_and_compare(&mut connection, version, || {
        IronCarrierError::VersionMismatch
    })
    .await?;

    let group = config
        .group
        .as_ref()
        .map(hash_helper::hashed_str)
        .unwrap_or_default();

    wait_and_compare(&mut connection, group, || IronCarrierError::GroupMismatch).await?;

    connection
        .write_half
        .write_u64(config.node_id_hashed.into())
        .await?;
    connection.write_half.flush().await?;
    let node_id = connection.read_half.read_u64().await?;

    Ok(connection.into_identified(node_id.into()))
}

async fn round_trip_compare(connection: &mut Connection, value: u64) -> crate::Result<bool> {
    connection.write_half.write_u64(value).await?;
    connection.write_half.flush().await?;
    connection
        .read_half
        .read_u64()
        .await
        .map(|result| result == value)
        .map_err(Box::from)
}

async fn wait_and_compare(
    connection: &mut Connection,
    value: u64,
    err: fn() -> IronCarrierError,
) -> crate::Result<()> {
    if connection.read_half.read_u64().await? == value {
        connection.write_half.write_u64(value).await?;
        connection.write_half.flush().await.map_err(Box::from)
    } else {
        Err(Box::new(err()))
    }
}
