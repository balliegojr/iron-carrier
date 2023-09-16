use std::collections::HashSet;

use crate::hash_type_id::{HashTypeId, TypeId};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{
    call::Call, group_call::GroupCall, network_message::NetworkMessage, rpc_message::RPCMessage,
    subscriber::Subscriber, OutputMessageType,
};

#[derive(Debug, Clone)]
pub struct RPCHandler {
    network_output: Sender<(NetworkMessage, OutputMessageType)>,
    consumers: Sender<(Vec<TypeId>, Sender<RPCMessage>)>,
    remove_consumers: Sender<Vec<TypeId>>,
}

impl RPCHandler {
    pub fn new(
        network_output: Sender<(NetworkMessage, OutputMessageType)>,
        consumers: Sender<(Vec<TypeId>, Sender<RPCMessage>)>,
        remove_consumers: Sender<Vec<TypeId>>,
    ) -> Self {
        Self {
            network_output,
            consumers,
            remove_consumers,
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

    pub async fn subscribe_many(&self, types: Vec<TypeId>) -> crate::Result<Subscriber> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.consumers.send((types.clone(), tx)).await?;
        Ok(Subscriber::new(types, rx, self.remove_consumers.clone()))
    }
}
