use crate::hash_type_id::{HashTypeId, TypeId};
use serde::{de::Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{network_message::NetworkMessage, OutputMessageType};

pub struct RPCMessage {
    inner: NetworkMessage,
    node_id: NodeId,
    reply_sender: Sender<(NetworkMessage, OutputMessageType)>,
}
impl RPCMessage {
    pub fn new(
        inner: NetworkMessage,
        node_id: NodeId,
        reply_sender: Sender<(NetworkMessage, OutputMessageType)>,
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
    pub fn data<'a, T: HashTypeId + Deserialize<'a>>(&'a self) -> crate::Result<T> {
        self.inner.data()
    }
    pub async fn ack(self) -> crate::Result<()> {
        let reply = NetworkMessage::ack_message(self.inner.id());

        self.reply_sender
            .send((reply, OutputMessageType::Reply(self.node_id)))
            .await?;

        Ok(())
    }

    pub async fn reply<U: HashTypeId + Serialize>(self, message: U) -> crate::Result<()> {
        let reply = NetworkMessage::reply_message(self.inner.id(), message)?;
        self.reply_sender
            .send((reply, OutputMessageType::Reply(self.node_id)))
            .await?;

        Ok(())
    }
}

impl From<RPCMessage> for NetworkMessage {
    fn from(value: RPCMessage) -> Self {
        value.inner
    }
}