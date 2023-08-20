use std::{pin::Pin, sync::atomic::AtomicU32};

use chacha20poly1305::{
    aead::stream::{DecryptorLE31, EncryptorLE31},
    XChaCha20Poly1305,
};
use pbkdf2::pbkdf2_hmac_array;
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::{config::Config, constants::VERSION, hash_helper, node_id::NodeId, IronCarrierError};

mod read_half;
pub use read_half::ReadHalf;

mod write_half;
pub use write_half::WriteHalf;

mod identified;
pub use identified::Identified;

static CONNECTION_ID_COUNT: AtomicU32 = AtomicU32::new(0);

type EReadHalf = async_encrypted_stream::ReadHalf<OwnedReadHalf, DecryptorLE31<XChaCha20Poly1305>>;
type EWriteHalf =
    async_encrypted_stream::WriteHalf<OwnedWriteHalf, EncryptorLE31<XChaCha20Poly1305>>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub struct ConnectionId(u32);

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

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("connection_id", &self.connection_id)
            .finish()
    }
}

pub async fn handshake_and_identify_connection(
    config: &Config,
    stream: tokio::net::TcpStream,
) -> crate::Result<Identified<Connection>> {
    let (read, write) = stream.into_split();
    let mut connection = if config.encryption_key.is_some() {
        get_encrypted_connection(read, write, config).await?
    } else {
        Connection::new(Box::pin(read), Box::pin(write))
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

async fn get_encrypted_connection(
    mut read: OwnedReadHalf,
    mut write: OwnedWriteHalf,
    config: &Config,
) -> crate::Result<Connection> {
    use rand_core::OsRng;
    use x25519_dalek::{EphemeralSecret, PublicKey};
    let secret_key = EphemeralSecret::random_from_rng(OsRng);
    let public_key = PublicKey::from(&secret_key);

    if write.write(public_key.as_bytes()).await? != 32 {
        return Err(IronCarrierError::ConnectionTimeout.into());
    }

    let mut peer_public_key = [0u8; 32];
    read.read_exact(&mut peer_public_key).await?;

    let peer_public_key = PublicKey::from(peer_public_key);
    let shared_key = secret_key.diffie_hellman(&peer_public_key);

    let shared_key = match config.encryption_key.as_ref() {
        Some(config_key) => get_key(config_key.as_bytes(), shared_key.as_bytes()),
        None => shared_key.to_bytes(),
    };

    let (read, write): (EReadHalf, EWriteHalf) = async_encrypted_stream::encrypted_stream(
        read,
        write,
        shared_key.as_ref().into(),
        [0u8; 20].as_ref().into(),
    );

    Ok(Connection::new(Box::pin(read), Box::pin(write)))
}

fn get_key(plain_key: &[u8], salt: &[u8]) -> [u8; 32] {
    const ITERATIONS: u32 = 4096;
    pbkdf2_hmac_array::<Sha256, 32>(plain_key, salt, ITERATIONS)
}
