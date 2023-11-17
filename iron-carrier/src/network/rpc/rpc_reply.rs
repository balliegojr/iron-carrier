use crate::message_types::MessageType;
use serde::de::Deserialize;

use crate::node_id::NodeId;

use super::network_message::NetworkMessage;

/// A message reply from a Node
#[derive(Debug)]
pub struct RPCReply {
    inner: NetworkMessage,
    node_id: NodeId,
}

impl RPCReply {
    pub fn new(inner: NetworkMessage, node_id: NodeId) -> Self {
        Self { inner, node_id }
    }

    pub fn is_ack(&self) -> bool {
        self.inner.is_ack()
    }

    pub fn data<'a, T>(&'a self) -> anyhow::Result<T>
    where
        T: MessageType + Deserialize<'a>,
    {
        self.inner.data()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}
