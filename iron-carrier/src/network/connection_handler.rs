use std::{net::SocketAddr, time::Duration};

use tokio::sync::mpsc::Sender;

use crate::{
    config::Config,
    constants::DEFAULT_NETWORK_TIMEOUT,
    network::connection::{self, Connection},
    node_id::NodeId,
};

#[derive(Debug, Clone)]
pub struct ConnectionHandler {
    config: &'static Config,
    on_connect: Sender<Connection>,
}

impl ConnectionHandler {
    pub fn new(config: &'static Config, on_connect: Sender<Connection>) -> Self {
        Self { config, on_connect }
    }

    pub fn start_listening(&self) {
        let inbound_fut = listen_connections(self.config, self.on_connect.clone());
        tokio::spawn(async move {
            if let Err(err) = inbound_fut.await {
                log::error!("{err}");
            }
        });
    }

    #[cfg(test)]
    pub async fn connect_context(&self, other: &crate::context::Context) {
        log::info!(
            "Connecting {} to {}",
            self.config.node_id_hashed,
            other.config.node_id_hashed
        );
        let (self_conn, other_conn) =
            local_connection_pair(self.config.node_id_hashed, other.config.node_id_hashed);

        let _ = self.on_connect.send(other_conn).await;
        let _ = other.connection_handler.on_connect.send(self_conn).await;
    }

    pub async fn connect(&self, addr: SocketAddr) -> anyhow::Result<NodeId> {
        let connect_and_identify = async {
            let backoff = backoff::ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(Some(Duration::from_secs(3)))
                .build();

            let transport_stream = backoff::future::retry(backoff, || async {
                tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(backoff::Error::from)
            })
            .await?;

            connection::handshake_and_identify_connection(self.config, transport_stream).await
        };

        let connection = tokio::time::timeout(
            Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
            connect_and_identify,
        )
        .await
        .map_err(|_| anyhow::anyhow!("Timeout when connecting to node"))??;

        let node_id = connection.node_id();
        if let Err(err) = self.on_connect.send(connection).await {
            log::error!("{err}")
        }

        Ok(node_id)
    }
}

async fn listen_connections(
    config: &'static Config,
    on_connect: Sender<Connection>,
) -> anyhow::Result<()> {
    log::debug!("Listening on {}", config.port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    while let Ok((stream, _addr)) = listener.accept().await {
        let connection = match tokio::time::timeout(
            Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
            connection::handshake_and_identify_connection(config, stream),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            Ok(Err(err)) => {
                log::error!("{err}");
                continue;
            }
            Err(_) => {
                log::error!("Timeout when connecting to node");
                continue;
            }
        };

        if let Err(err) = on_connect.send(connection).await {
            log::error!("{err}");
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
fn local_connection_pair(
    first: crate::node_id::NodeId,
    second: crate::node_id::NodeId,
) -> (connection::Connection, connection::Connection) {
    let (one_rx, one_tx) = tokio::io::duplex(4096);
    let (two_rx, two_tx) = tokio::io::duplex(4096);

    (
        connection::Connection::new(Box::pin(one_rx), Box::pin(two_tx), first, 0),
        connection::Connection::new(Box::pin(two_rx), Box::pin(one_tx), second, 0),
    )
}
