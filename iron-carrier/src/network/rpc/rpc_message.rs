use std::fmt;

use crate::protocol::{MessageTypes, Protocol};
use anyhow::anyhow;
use serde::{Serialize, de::Deserialize};
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{OutboundNetworkMessageType, network_message::NetworkMessage};

/// RPC Message received from a network node.
pub struct RPCMessage {
    inner: NetworkMessage,
    node_id: NodeId,
    reply_sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
}

impl fmt::Debug for RPCMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RPCMessage")
            .field("inner", &self.inner)
            .field("sender", &self.node_id)
            .finish()
    }
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

    pub fn message_type(&self) -> anyhow::Result<MessageTypes> {
        self.inner
            .type_id()
            .ok_or_else(|| anyhow!("request messages must have type"))
    }

    pub fn data<'a, T: Protocol + Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        self.inner.data()
    }

    pub async fn ack(self) -> anyhow::Result<()> {
        self.send(self.inner.ack_message()).await
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        self.send(self.inner.ping_message()).await
    }

    pub async fn cancel(self) -> anyhow::Result<()> {
        self.send(self.inner.cancel_message()).await
    }

    pub async fn reply<U: Protocol + Serialize>(self, message: U) -> anyhow::Result<()> {
        self.send(self.inner.reply_message(message)?).await
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
