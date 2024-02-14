use std::{net::SocketAddr, time::Duration};

use crate::{
    config::Config,
    constants::DEFAULT_NETWORK_TIMEOUT,
    network::connection::{self},
    node_id::NodeId,
};

use super::rpc::RPCHandler;

#[derive(Debug, Clone)]
pub struct ConnectionHandler {
    config: &'static Config,
    rpc: RPCHandler,
}

impl ConnectionHandler {
    pub fn new(config: &'static Config, rpc: RPCHandler) -> Self {
        Self { config, rpc }
    }

    pub fn start_accepting_connections(&self) {
        let inbound_fut = accept_connections(self.config, self.rpc.clone());
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

        let _ = self.rpc.add_connection(other_conn).await;
        let _ = other.connection_handler.rpc.add_connection(self_conn).await;
    }

    pub async fn connect(&self, addr: SocketAddr) -> anyhow::Result<NodeId> {
        let connection = connection::try_connect_and_identify(self.config, addr).await?;

        let node_id = connection.node_id();
        if let Err(err) = self.rpc.add_connection(connection).await {
            log::error!("{err}")
        }

        Ok(node_id)
    }
}

async fn accept_connections(config: &'static Config, rpc: RPCHandler) -> anyhow::Result<()> {
    log::debug!("Listening on {}", config.port);
    let listener =
        tokio::net::TcpListener::bind(format!("{}:{}", config.bind, config.port)).await?;

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

        if let Err(err) = rpc.add_connection(connection).await {
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
