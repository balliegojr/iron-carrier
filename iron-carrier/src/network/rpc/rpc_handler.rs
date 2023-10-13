use std::{collections::HashSet, sync::Arc};

use crate::hash_type_id::{HashTypeId, TypeId};
use serde::Serialize;
use tokio::sync::{mpsc::Sender, Semaphore};

use crate::node_id::NodeId;

use super::{
    call::Call, group_call::GroupCall, network_message::NetworkMessage, rpc_message::RPCMessage,
    subscription::Subscription, OutboundNetworkMessageType,
};

/// Handler to the RPC service. If all copies of this are dropped, the service will shutdown.
#[derive(Debug, Clone)]
pub struct RPCHandler {
    network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    consumers: Sender<(Vec<TypeId>, Sender<RPCMessage>, Arc<Semaphore>)>,
}

impl RPCHandler {
    pub fn new(
        network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        consumers: Sender<(Vec<TypeId>, Sender<RPCMessage>, Arc<Semaphore>)>,
    ) -> Self {
        Self {
            network_output,
            consumers,
        }
    }

    /// Make a RPC to a single `target`
    pub fn call<T>(&self, data: T, target: NodeId) -> Call<T>
    where
        T: HashTypeId + Serialize,
    {
        Call::new(data, self.network_output.clone(), target)
    }

    /// Make a RPC to multiple `targets`
    pub fn multi_call<T>(&self, data: T, targets: HashSet<NodeId>) -> GroupCall<T>
    where
        T: HashTypeId + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), Some(targets))
    }

    /// Make a RPC broadcast to every connected Node
    pub fn broadcast<T>(&self, data: T) -> GroupCall<T>
    where
        T: HashTypeId + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), None)
    }

    /// Subscribe to events from the network
    pub async fn subscribe(&self, types: &[TypeId]) -> anyhow::Result<Subscription> {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.consumers.send((types.to_vec(), tx, semaphore)).await?;
        Ok(Subscription::new(rx, permit))
    }
}
