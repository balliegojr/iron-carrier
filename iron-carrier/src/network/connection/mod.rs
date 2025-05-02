use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64},
    },
    time::{Duration, SystemTime},
};

use chacha20poly1305::{
    XChaCha20Poly1305,
    aead::stream::{DecryptorLE31, EncryptorLE31},
};
use pbkdf2::pbkdf2_hmac_array;
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use super::backoff_retry;
use crate::{
    config::Config,
    constants::{DEFAULT_NETWORK_TIMEOUT, VERSION},
    hash_helper,
    node_id::NodeId,
};

mod read_half;
pub use read_half::ReadHalf;

mod write_half;
pub use write_half::WriteHalf;

type ReadStream = Pin<Box<dyn AsyncRead + Send + Sync>>;
type WriteStream = Pin<Box<dyn AsyncWrite + Send + Sync>>;

/// Counter used for connection deduplication.
///
/// Each connection will receive a number. When two connections between
/// the same nodes are found, the connection dedup number will be used
/// to decide which connection will be dropped, since the number is shared
/// between nodes, the same connection will be dropped by both nodes
static CONNECTION_DEDUP_CONTROL: AtomicU8 = AtomicU8::new(0);

pub struct Connection {
    read_half: Pin<Box<dyn AsyncRead + Send + Sync>>,
    write_half: Pin<Box<dyn AsyncWrite + Send + Sync>>,
    node_id: NodeId,
    /// Dedup control is used to decide which connection will be dropped
    /// when a duplicated connection is found
    dedup_control: u8,
}

impl Connection {
    pub fn new(
        read_half: Pin<Box<dyn AsyncRead + Send + Sync>>,
        write_half: Pin<Box<dyn AsyncWrite + Send + Sync>>,
        node_id: NodeId,
        dedup_control: u8,
    ) -> Self {
        Connection {
            read_half,
            write_half,
            node_id,
            dedup_control,
        }
    }

    pub fn split(self) -> (WriteHalf, ReadHalf) {
        let node_id = self.node_id;
        let last_access = Arc::new(AtomicU64::new(crate::time::system_time_to_secs(
            SystemTime::now(),
        )));
        let read_dropped = Arc::new(AtomicBool::new(false));

        (
            WriteHalf::new(
                self.write_half,
                node_id,
                last_access.clone(),
                self.dedup_control,
                read_dropped.clone(),
            ),
            ReadHalf::new(self.read_half, node_id, last_access, read_dropped),
        )
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("node_id", &self.node_id)
            .finish()
    }
}

pub async fn try_connect_and_identify(
    config: &'static Config,
    addr: SocketAddr,
) -> anyhow::Result<Connection> {
    let connect_and_identify = async {
        let transport_stream = backoff_retry(DEFAULT_NETWORK_TIMEOUT / 2, || async {
            tokio::net::TcpStream::connect(addr)
                .await
                .map_err(backoff::Error::from)
        })
        .await?;

        let (read, write) = transport_stream.into_split();

        handshake(config, Box::pin(read), Box::pin(write)).await
    };

    tokio::time::timeout(
        Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
        connect_and_identify,
    )
    .await
    .map_err(|_| anyhow::anyhow!("Timeout when connecting to node"))?
}

/// Perform a handshake with a node.
///
/// The handshake consists of verifying:
/// - Encryption configuration
/// - Version
/// - Group
/// - Node Id
///
/// If there is a encryption configuration mismatch, there will be an attempt to promote the
/// connection to an encrypted connection. This attempt only works if there is no predefined
/// encryption key for any of the nodes.
pub async fn handshake(
    config: &'static Config,
    mut read: ReadStream,
    mut write: WriteStream,
) -> anyhow::Result<Connection> {
    // Check if the connection is encrypted
    let encryption_enabled = (config.encryption.is_enabled() as u8).to_be();
    write.write_u8(encryption_enabled).await?;
    write.flush().await?;

    let peer_encryption_enabled = read.read_u8().await?;
    if peer_encryption_enabled != encryption_enabled {
        log::warn!("Encryption config mismatch between nodes");
    }

    // Promote the connection to encrypted if any of the nodes wants an encrypted connection
    if config.encryption.is_enabled() || peer_encryption_enabled != 0 {
        (read, write) =
            get_encrypted_connection(read, write, config.encryption.encryption_key()).await?;
    }

    let version = hash_helper::hashed_str(VERSION).to_be_bytes();
    let group = config
        .group
        .as_ref()
        .map(hash_helper::hashed_str)
        .unwrap_or_default()
        .to_be_bytes();

    write.write_all(&version).await?;
    write.write_all(&group).await?;
    write
        .write_all(&u64::from(config.node_id_hashed).to_be_bytes())
        .await?;
    write.flush().await?;

    let mut buf = [0u8; 24];
    if let Err(err) = read.read_exact(&mut buf).await {
        if err.kind() == std::io::ErrorKind::InvalidData {
            anyhow::bail!("Encryption mismatch");
        }

        Err(err)?;
    }

    // read.read_exact(&mut buf).await?;
    if version.ne(&buf[0..8]) {
        anyhow::bail!("Version mismatch");
    }

    if group.ne(&buf[8..16]) {
        anyhow::bail!("Group mismatch");
    }

    let node_id = NodeId::from(u64::from_be_bytes(buf[16..].try_into()?));
    // Docker network interface will have the same ip on different nodes (172.17.0.1)
    // Trying to connect to it, with docker running, have the same effect of a loopback
    if node_id == config.node_id_hashed {
        anyhow::bail!("Tried to connect to same node");
    }

    // Exchange the dedup control byte.
    // The node with the lower id will send the byte
    let control = match config.node_id_hashed.cmp(&node_id) {
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
            let control =
                CONNECTION_DEDUP_CONTROL.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            write.write_u8(control.to_be()).await?;
            write.flush().await?;

            control
        }
        std::cmp::Ordering::Greater => u8::from_be(read.read_u8().await?),
    };

    Ok(Connection::new(read, write, node_id, control))
}

async fn get_encrypted_connection<R, W>(
    mut read: R,
    mut write: W,
    pre_defined_key: Option<&str>,
) -> anyhow::Result<(ReadStream, WriteStream)>
where
    R: AsyncRead + Send + Unpin + Sync + 'static,
    W: AsyncWrite + Send + Unpin + Sync + 'static,
{
    use rand_core::OsRng;
    use x25519_dalek::{EphemeralSecret, PublicKey};
    let secret_key = EphemeralSecret::random_from_rng(OsRng);
    let public_key = PublicKey::from(&secret_key);

    if write.write(public_key.as_bytes()).await? != 32 {
        anyhow::bail!("Connection handshake failed");
    }

    let mut peer_public_key = [0u8; 32];
    read.read_exact(&mut peer_public_key).await?;

    let peer_public_key = PublicKey::from(peer_public_key);
    let shared_key = secret_key.diffie_hellman(&peer_public_key);

    let shared_key = match pre_defined_key {
        Some(config_key) => get_key(config_key.as_bytes(), shared_key.as_bytes()),
        None => shared_key.to_bytes(),
    };

    let (read, write): (
        async_encrypted_stream::ReadHalf<R, DecryptorLE31<XChaCha20Poly1305>>,
        async_encrypted_stream::WriteHalf<W, EncryptorLE31<XChaCha20Poly1305>>,
    ) = async_encrypted_stream::encrypted_stream(
        read,
        write,
        shared_key.as_ref().into(),
        [0u8; 20].as_ref().into(),
    );

    Ok((Box::pin(BufReader::new(read)), Box::pin(write)))
}

fn get_key(plain_key: &[u8], salt: &[u8]) -> [u8; 32] {
    const ITERATIONS: u32 = 4096;
    pbkdf2_hmac_array::<Sha256, 32>(plain_key, salt, ITERATIONS)
}

#[cfg(test)]
mod tests {
    use crate::leak::Leak;

    use super::*;

    #[tokio::test]
    async fn test_handshake_success_non_encrypted() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        let hs_one = hs_one.expect("Handshake one failed");
        let hs_two = hs_two.expect("Handshake two failed");

        assert_eq!(hs_one.node_id, cfg_two.node_id_hashed);
        assert_eq!(hs_two.node_id, cfg_one.node_id_hashed);
    }

    #[tokio::test]
    async fn test_handshake_success_both_encrypted() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            encryption: crate::config::Encryption::Enabled,
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            encryption: crate::config::Encryption::Enabled,
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        let hs_one = hs_one.expect("Handshake one failed");
        let hs_two = hs_two.expect("Handshake two failed");

        assert_eq!(hs_one.node_id, cfg_two.node_id_hashed);
        assert_eq!(hs_two.node_id, cfg_one.node_id_hashed);
    }

    #[tokio::test]
    async fn test_handshake_success_both_encrypted_with_pre_defined_key() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            encryption: crate::config::Encryption::EnabledWithKey("secret".to_owned()),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            encryption: crate::config::Encryption::EnabledWithKey("secret".to_owned()),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        let hs_one = hs_one.expect("Handshake one failed");
        let hs_two = hs_two.expect("Handshake two failed");

        assert_eq!(hs_one.node_id, cfg_two.node_id_hashed);
        assert_eq!(hs_two.node_id, cfg_one.node_id_hashed);
    }

    #[tokio::test]
    async fn test_handshake_success_one_encrypted() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            encryption: crate::config::Encryption::Enabled,
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            encryption: crate::config::Encryption::Disabled,
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        let hs_one = hs_one.expect("Handshake one failed");
        let hs_two = hs_two.expect("Handshake two failed");

        assert_eq!(hs_one.node_id, cfg_two.node_id_hashed);
        assert_eq!(hs_two.node_id, cfg_one.node_id_hashed);
    }

    #[tokio::test]
    async fn test_handshake_success_same_defined_group() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            group: Some("group".to_owned()),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            group: Some("group".to_owned()),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        let hs_one = hs_one.expect("Handshake one failed");
        let hs_two = hs_two.expect("Handshake two failed");

        assert_eq!(hs_one.node_id, cfg_two.node_id_hashed);
        assert_eq!(hs_two.node_id, cfg_one.node_id_hashed);
    }

    #[tokio::test]
    async fn test_handshake_fail_different_groups() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            group: Some("group".to_owned()),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            group: Some("group_two".to_owned()),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        assert_eq!(hs_one.unwrap_err().to_string(), "Group mismatch");
        assert_eq!(hs_two.unwrap_err().to_string(), "Group mismatch");
    }

    #[tokio::test]
    async fn test_handshake_fail_encryption_mismatch() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            encryption: crate::config::Encryption::EnabledWithKey("secret".to_owned()),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 2.into(),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        assert_eq!(hs_one.unwrap_err().to_string(), "Encryption mismatch");
        assert_eq!(hs_two.unwrap_err().to_string(), "Encryption mismatch");
    }

    #[tokio::test]
    async fn test_handshake_fail_same_node() {
        let cfg_one = Config {
            node_id_hashed: 1.into(),
            ..Default::default()
        }
        .leak();

        let cfg_two = Config {
            node_id_hashed: 1.into(),
            ..Default::default()
        }
        .leak();

        let (hs_one, hs_two) = perform_handshake(cfg_one, cfg_two).await;
        assert_eq!(
            hs_one.unwrap_err().to_string(),
            "Tried to connect to same node"
        );
        assert_eq!(
            hs_two.unwrap_err().to_string(),
            "Tried to connect to same node"
        );
    }

    async fn perform_handshake(
        cfg_one: &'static Config,
        cfg_two: &'static Config,
    ) -> (anyhow::Result<Connection>, anyhow::Result<Connection>) {
        let (one_rx, one_tx) = tokio::io::duplex(1024);
        let (two_rx, two_tx) = tokio::io::duplex(1024);

        let fut_hs_one = tokio::spawn(async {
            let (rx, tx) = (Box::pin(one_rx), Box::pin(two_tx));
            handshake(cfg_one, rx, tx).await
        });

        let fut_hs_two = tokio::spawn(async {
            let (rx, tx) = (Box::pin(two_rx), Box::pin(one_tx));
            handshake(cfg_two, rx, tx).await
        });

        let hs_one = fut_hs_one.await.unwrap();
        let hs_two = fut_hs_two.await.unwrap();

        (hs_one, hs_two)
    }
}
