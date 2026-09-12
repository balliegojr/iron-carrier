use std::{fmt, marker::PhantomData};

use crate::protocol::{MessageTypes, Protocol, ProtocolAck, ProtocolPayload, ProtocolQuery};
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

    pub fn into_event<T>(self) -> RPCEvent<T>
    where
        T: Protocol,
    {
        RPCEvent {
            inner: self,
            _marker: Default::default(),
        }
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        self.send(self.inner.ping_message()).await
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

/// Typed wrapper around RPCMessage
pub struct RPCEvent<T> {
    _marker: PhantomData<T>,
    inner: RPCMessage,
}

impl<T> fmt::Debug for RPCEvent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RPCMessage")
            .field("inner", &self.inner.inner)
            .field("sender", &self.inner.node_id)
            .finish()
    }
}

impl<T> RPCEvent<T> {
    pub fn inner(&self) -> &RPCMessage {
        &self.inner
    }
}

impl<'a, T> RPCEvent<T>
where
    T: ProtocolPayload + Deserialize<'a>,
{
    pub fn data(&'a self) -> anyhow::Result<T> {
        self.inner.inner.data()
    }
}

impl<T> RPCEvent<T>
where
    T: ProtocolQuery,
    T::ResponseType: Serialize,
{
    pub async fn reply(self, message: T::ResponseType) -> anyhow::Result<()> {
        self.inner
            .send(self.inner.inner.reply_message(message)?)
            .await
    }
}

impl<T> RPCEvent<T>
where
    T: ProtocolAck,
{
    pub async fn ack(self) -> anyhow::Result<()> {
        self.inner.send(self.inner.inner.ack_message()).await
    }
}
