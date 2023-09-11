use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

use crate::hash_type_id::TypeId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use crate::{node_id::NodeId, IronCarrierError};

use super::{
    connection::{Connection, Identified, ReadHalf},
    connection_storage::ConnectionStorage,
};

mod network_event_decoder;
use network_event_decoder::NetWorkEventDecoder;

mod network_message;
use network_message::NetworkMessage;

mod rpc_message;
pub use rpc_message::RPCMessage;

mod rpc_reply;
use rpc_reply::RPCReply;

mod call;
mod group_call;
mod rpc_handler;
pub use rpc_handler::RPCHandler;

pub fn rpc_service(new_connection: Receiver<Identified<Connection>>) -> RPCHandler {
    let (net_out_tx, net_out_rx) = tokio::sync::mpsc::channel(10);
    let (consumer_tx, consumer_rx) = tokio::sync::mpsc::channel(10);

    tokio::spawn(rpc_loop(
        net_out_rx,
        net_out_tx.clone(),
        new_connection,
        consumer_rx,
    ));

    RPCHandler::new(net_out_tx, consumer_tx)
}

async fn rpc_loop(
    mut net_out: Receiver<(NetworkMessage, OutputMessageType)>,
    net_out_sender: Sender<(NetworkMessage, OutputMessageType)>,
    mut new_connection: Receiver<Identified<Connection>>,
    mut new_consumer: Receiver<(TypeId, Sender<RPCMessage>)>,
) {
    let (net_in_tx, mut net_in_rx) = tokio::sync::mpsc::channel::<(NodeId, NetworkMessage)>(10);
    let mut received_messages: HashMap<TypeId, VecDeque<(NodeId, NetworkMessage)>> =
        Default::default();
    let mut consumers: HashMap<TypeId, Sender<RPCMessage>> = Default::default();
    let mut waiting_reply: HashMap<(u16, NodeId), Sender<RPCReply>> = Default::default();
    let mut connections = ConnectionStorage::default();

    loop {
        tokio::select! {
             request = net_in_rx.recv() => {
                let Some((node_id, message)) = request else { break; };
                if message.is_reply() {
                    process_reply(message, node_id, &mut waiting_reply).await;
                } else {
                    process_rpc_call(
                        message,
                        node_id,
                        &mut consumers,
                        &mut received_messages,
                        &net_out_sender,
                    )
                    .await;
                }
            }

            request = new_connection.recv() =>  {
                let Some(connection) = request else { break; };
                if connections.contains_node(&connection.node_id()) {
                    log::warn!("Already connected to {}", connection.node_id());
                } else {
                    // TODO: send unsent messages to this node
                    let (write, read) = connection.split();
                    connections.insert(write);
                    tokio::spawn(read_network_data(read, net_in_tx.clone()));
                }
            }

            request = net_out.recv() => {
                let Some((message, send_type)) = request else { break; };
                send_output_message(message, send_type, &mut connections, &mut waiting_reply).await;
            }

            request = new_consumer.recv() => {
                let Some((consumer_type, consumer)) = request else { break; };
                add_new_consumer(
                    consumer_type,
                    consumer,
                    &mut received_messages,
                    &mut consumers,
                    &net_out_sender,
                )
                .await;

            }
        }
    }
}

pub enum OutputMessageType {
    Reply(NodeId),
    SingleNode(NodeId, Sender<RPCReply>),
    MultiNode(HashSet<NodeId>, Sender<RPCReply>),
    Broadcast(Sender<RPCReply>),
}

async fn process_rpc_call(
    message: NetworkMessage,
    node_id: NodeId,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
    received_messages: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage)>>,
    net_out_sender: &Sender<(NetworkMessage, OutputMessageType)>,
) {
    if let Entry::Occupied(mut entry) = consumers.entry(message.type_id()) {
        let consumer = entry.get_mut();
        if consumer.is_closed() {
            entry.remove_entry();
        } else {
            consumer
                .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
                .await;
            return;
        }
    }

    received_messages
        .entry(message.type_id())
        .or_default()
        .push_back((node_id, message));
}

async fn process_reply(
    message: NetworkMessage,
    node_id: NodeId,
    waiting_reply: &mut HashMap<(u16, NodeId), Sender<RPCReply>>,
) {
    let sent_handler = match waiting_reply.remove(&(message.id(), node_id)) {
        Some(sent_handler) => sent_handler,
        None => {
            log::error!("Received an unexpected message from {node_id}");
            return;
        }
    };

    if sent_handler.is_closed() {
        log::error!("Received reply from {node_id} after timeout");
        return;
    }

    sent_handler.send(RPCReply::new(message, node_id)).await;
}

async fn send_output_message(
    message: NetworkMessage,
    send_type: OutputMessageType,
    connections: &mut ConnectionStorage,
    waiting_reply: &mut HashMap<(u16, NodeId), Sender<RPCReply>>,
) {
    async fn send_to(
        message: &NetworkMessage,
        node_id: NodeId,
        connections: &mut ConnectionStorage,
    ) -> crate::Result<()> {
        match connections.get_mut(&node_id) {
            Some(connection) => {
                message.write_into(connection).await.inspect_err(|err| {
                    log::error!("Failed to write to connection {err}");
                })?;
            }
            None => {
                log::error!("Not connected to node {node_id}");
                return Err(IronCarrierError::UnknownNode(node_id).into());
            }
        }

        Ok(())
    }

    // TODO: keep track of unsent replies
    match send_type {
        OutputMessageType::Reply(node_id) => {
            let _ = send_to(&message, node_id, connections).await;
        }
        OutputMessageType::SingleNode(node_id, callback) => {
            if send_to(&message, node_id, connections).await.is_ok() {
                waiting_reply.insert((message.id(), node_id), callback);
            }
        }
        OutputMessageType::MultiNode(nodes, callback) => {
            for node_id in nodes {
                if send_to(&message, node_id, connections).await.is_ok() {
                    waiting_reply.insert((message.id(), node_id), callback.clone());
                }
            }
        }
        OutputMessageType::Broadcast(callback) => {
            for node_id in connections.connected_nodes().collect::<Vec<_>>() {
                if send_to(&message, node_id, connections).await.is_ok() {
                    waiting_reply.insert((message.id(), node_id), callback.clone());
                }
            }
        }
    }
}

async fn add_new_consumer(
    consumer_type: TypeId,
    consumer: Sender<RPCMessage>,
    received_messages: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage)>>,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
    net_out_sender: &Sender<(NetworkMessage, OutputMessageType)>,
) {
    if let Entry::Occupied(mut entry) = received_messages.entry(consumer_type) {
        while let Some((node_id, message)) = entry.get_mut().pop_front() {
            if consumer
                .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
                .await
                .is_err()
            {
                // TODO: add message back
                // entry.get_mut().push_front((node_id, message));
                return;
            }
        }

        entry.remove_entry();
    }

    consumers.insert(consumer_type, consumer);
}

async fn read_network_data(
    read_connection: Identified<ReadHalf>,
    event_stream: Sender<(NodeId, NetworkMessage)>,
) {
    let peer_id = read_connection.node_id();

    let mut stream =
        tokio_util::codec::FramedRead::new(read_connection.into_inner(), NetWorkEventDecoder {});
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                if let Err(err) = event_stream.send((peer_id, event)).await {
                    log::error!("Error sending event to event stream {err}");
                    break;
                }
            }
            Err(err) => {
                log::error!("error reading from peer {err}");
            }
        }
    }
}
