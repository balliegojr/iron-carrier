use std::{net::SocketAddr, time::Duration};

use tokio::sync::mpsc::Sender;

use crate::{
    config::Config,
    constants::PEER_IDENTIFICATION_TIMEOUT,
    network::connection::{self, Connection, Identified},
    node_id::NodeId,
    IronCarrierError,
};

#[derive(Debug, Clone)]
pub struct ConnectionHandler {
    config: &'static Config,
    on_connect: Sender<Identified<Connection>>,
}

impl ConnectionHandler {
    pub async fn new(
        config: &'static Config,
        on_connect: Sender<Identified<Connection>>,
    ) -> crate::Result<Self> {
        let inbound_fut = listen_connections(config, on_connect.clone());

        tokio::spawn(async move {
            if let Err(err) = inbound_fut.await {
                log::error!("{err}");
            }
        });

        Ok(Self { config, on_connect })
    }

    pub async fn connect(&self, addr: SocketAddr) -> crate::Result<NodeId> {
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
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connect_and_identify,
        )
        .await
        .map_err(|_| IronCarrierError::ConnectionTimeout)??;

        let node_id = connection.node_id();
        if let Err(err) = self.on_connect.send(connection).await {
            log::error!("{err}")
        }

        Ok(node_id)
    }
}

async fn listen_connections(
    config: &'static Config,
    on_connect: Sender<Identified<Connection>>,
) -> crate::Result<()> {
    log::debug!("Listening on {}", config.port);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;

    while let Ok((stream, _addr)) = listener.accept().await {
        let connection = match tokio::time::timeout(
            Duration::from_secs(PEER_IDENTIFICATION_TIMEOUT),
            connection::handshake_and_identify_connection(config, stream),
        )
        .await
        {
            Ok(Ok(connection)) => connection,
            _ => {
                log::error!("{}", IronCarrierError::ConnectionTimeout);
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
