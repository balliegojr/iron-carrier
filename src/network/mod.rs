use std::{marker::PhantomData, net::SocketAddr};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::mpsc::Receiver,
};

use crate::config::Config;

pub mod service_discovery;

pub struct ConnectionHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    _marker: PhantomData<T>,
}

impl<T> ConnectionHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    pub async fn new(config: &Config) -> crate::Result<Self> {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        dbg!(addr);
                    }
                    Err(_) => todo!(),
                }
            }
        });
        Ok(Self {
            _marker: PhantomData::default(),
        })
    }

    pub async fn connect(&self, addr: &SocketAddr, protocol: Protocol) -> ConnectionId {
        dbg!(tokio::net::TcpStream::connect(addr).await);
        ConnectionId
    }

    pub async fn disconnect(&self, connection_id: ConnectionId) {
        todo!()
    }

    pub async fn send_to(&self, connection_id: ConnectionId, event: T) {
        todo!();
    }
}

pub enum Protocol {
    Tcp,
    EncryptedTcp,
}

#[derive(Debug)]
pub struct ConnectionId;

struct Connection {
    read: Box<dyn AsyncRead>,
    write: Box<dyn AsyncWrite>,
}

struct IdentifiedConnection {
    inner: Connection,
}

impl Connection {
    pub fn new(mut stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        Self {
            read: Box::new(read),
            write: Box::new(write),
        }
    }
    pub fn is_identified(&self) -> bool {
        todo!()
    }

    async fn handshake(mut self) -> IdentifiedConnection {
        todo!()
    }
}
