use std::collections::HashSet;

use hash_type_id::{HashTypeId, TypeId};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{
    call::Call, group_call::GroupCall, network_message::NetworkMessage, rpc_message::RPCMessage,
    OutputMessageType,
};

#[derive(Debug, Clone)]
pub struct RPCHandler {
    network_output: Sender<(NetworkMessage, OutputMessageType)>,
    consumers: Sender<(TypeId, Sender<RPCMessage>)>,
}

impl RPCHandler {
    pub fn new(
        network_output: Sender<(NetworkMessage, OutputMessageType)>,
        consumers: Sender<(TypeId, Sender<RPCMessage>)>,
    ) -> Self {
        Self {
            network_output,
            consumers,
        }
    }

    pub fn call<T>(&self, data: T, target: NodeId) -> Call<T>
    where
        T: HashTypeId + Serialize,
    {
        Call::new(data, self.network_output.clone(), target)
    }

    pub fn multi_call<T>(&self, data: T, targets: HashSet<NodeId>) -> GroupCall<T>
    where
        T: HashTypeId + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), Some(targets))
    }

    pub fn broadcast<T>(&self, data: T) -> GroupCall<T>
    where
        T: HashTypeId + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), None)
    }

    // TODO: impl event subscriber
    pub async fn consume_events<T: HashTypeId>(
        &self,
    ) -> impl tokio_stream::Stream<Item = RPCMessage> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        self.consumers.send((T::id(), tx)).await;

        tokio_stream::wrappers::ReceiverStream::new(rx)
    }
}
