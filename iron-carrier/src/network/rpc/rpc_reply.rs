use std::marker::PhantomData;

use crate::protocol::{ProtocolAck, ProtocolPayload};
use serde::de::Deserialize;

use crate::node_id::NodeId;

use super::network_message::NetworkMessage;

/// A message reply from a Node
#[derive(Debug)]
pub struct RPCReply<T> {
    _marker: PhantomData<T>,
    inner: NetworkMessage,
    node_id: NodeId,
}

impl<T> RPCReply<T> {
    pub fn new(inner: NetworkMessage, node_id: NodeId) -> Self {
        Self {
            inner,
            node_id,
            _marker: Default::default(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl<'a, T> RPCReply<T>
where
    T: ProtocolPayload + Deserialize<'a>,
{
    pub fn data(&'a self) -> anyhow::Result<T> {
        self.inner.data::<T>()
    }
}

impl<T> RPCReply<T>
where
    T: ProtocolAck,
{
    pub fn is_ack(&self) -> bool {
        self.inner.is_ack()
    }
}
