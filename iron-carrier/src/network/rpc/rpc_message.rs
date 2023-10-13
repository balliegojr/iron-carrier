use crate::hash_type_id::{HashTypeId, TypeId};
use serde::{de::Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{network_message::NetworkMessage, OutboundNetworkMessageType};

/// RPC Message received from a network node.
#[derive(Debug)]
pub struct RPCMessage {
    inner: NetworkMessage,
    node_id: NodeId,
    reply_sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
}
impl RPCMessage {
    pub fn new(
        inner: NetworkMessage,
        node_id: NodeId,
        reply_sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    ) -> Self {
        Self {
            inner,
            node_id,
            reply_sender,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn type_id(&self) -> TypeId {
        self.inner.type_id()
    }

    pub fn data<'a, T: HashTypeId + Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        self.inner.data()
    }

    pub async fn ack(self) -> anyhow::Result<()> {
        let reply = NetworkMessage::ack_message(self.inner.id());
        self.send(reply).await
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let ping_message = NetworkMessage::ping_message(self.inner.id());
        self.send(ping_message).await
    }

    pub async fn cancel(self) -> anyhow::Result<()> {
        let cancel_message = NetworkMessage::cancel_message(self.inner.id());
        self.send(cancel_message).await
    }

    pub async fn reply<U: HashTypeId + Serialize>(self, message: U) -> anyhow::Result<()> {
        let reply = NetworkMessage::reply_message(self.inner.id(), message)?;
        self.send(reply).await
    }

    async fn send(&self, message: NetworkMessage) -> anyhow::Result<()> {
        self.reply_sender
            .send((message, OutboundNetworkMessageType::Response(self.node_id)))
            .await?;

        Ok(())
    }
}

impl From<RPCMessage> for NetworkMessage {
    fn from(value: RPCMessage) -> Self {
        value.inner
    }
}
