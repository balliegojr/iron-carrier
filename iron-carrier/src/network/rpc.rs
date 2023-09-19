use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

use crate::hash_type_id::TypeId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use crate::{node_id::NodeId, IronCarrierError};

use super::{
    connection::{Connection, ReadHalf},
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
mod subscriber;

mod rpc_handler;
pub use rpc_handler::RPCHandler;

pub fn rpc_service(new_connection: Receiver<Connection>) -> RPCHandler {
    let (net_out_tx, net_out_rx) = tokio::sync::mpsc::channel(10);
    let (add_consumer_tx, add_consumer_rx) = tokio::sync::mpsc::channel(10);
    let (remove_consumer_tx, remove_consumer_rx) = tokio::sync::mpsc::channel(10);

    tokio::spawn(rpc_loop(
        net_out_rx,
        net_out_tx.clone(),
        new_connection,
        add_consumer_rx,
        remove_consumer_rx,
    ));

    RPCHandler::new(net_out_tx, add_consumer_tx, remove_consumer_tx)
}

async fn rpc_loop(
    mut net_out: Receiver<(NetworkMessage, OutputMessageType)>,
    net_out_sender: Sender<(NetworkMessage, OutputMessageType)>,
    mut new_connection: Receiver<Connection>,
    mut add_consumers: Receiver<(Vec<TypeId>, Sender<RPCMessage>)>,
    mut remove_consumers: Receiver<Vec<TypeId>>,
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
                let Some((node_id, message)) = request else {
                    break;
                };

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
                let Some(connection) = request else {
                    break;
                };

                // TODO: send unsent messages to this node
                if let Some(read) = connections.insert(connection) {
                    tokio::spawn(read_network_data(read, net_in_tx.clone()));
                }
            }

            request = net_out.recv() => {
                let Some((message, send_type)) = request else { break; };
                send_output_message(message, send_type, &mut connections, &mut waiting_reply).await;
            }

            request = add_consumers.recv() => {
                let Some((consumer_types, consumer_channel)) = request else {
                    break;
                };
                add_new_consumer(
                    consumer_types,
                    consumer_channel,
                    &mut received_messages,
                    &mut consumers,
                    &net_out_sender,
                )
                .await;
            }

            request = remove_consumers.recv() => {
                let Some(consumer_types) = request else {
                    break;
                };
                for consumer_type in consumer_types {
                    consumers.remove(&consumer_type);
                }
            }
        }
    }

    // It is necessary to ensure that all output messages are sent before exiting
    while let Some((message, send_type)) = net_out.recv().await {
        send_output_message(message, send_type, &mut connections, &mut waiting_reply).await;
    }
}

#[derive(Debug)]
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
        if let Err(err) = entry
            .get_mut()
            .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
            .await
        {
            let message: NetworkMessage = err.0.into();
            received_messages
                .entry(message.type_id())
                .or_default()
                .push_back((node_id, message));
            entry.remove_entry();
        }
    } else {
        received_messages
            .entry(message.type_id())
            .or_default()
            .push_back((node_id, message));
    }
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

    if let Err(err) = sent_handler.send(RPCReply::new(message, node_id)).await {
        log::error!("Failed to send reply {err}");
    }
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
        let write_result = match connections.get_mut(&node_id) {
            Some(connection) => message.write_into(connection).await,
            None => {
                log::error!("Not connected to node {node_id}");
                return Err(IronCarrierError::UnknownNode(node_id).into());
            }
        };

        if let Err(err) = write_result {
            log::error!("Failed to write to connection {err}");
            connections.remove(&node_id);
        }

        Ok(())
    }

    // TODO: keep track of unsent replies
    match send_type {
        OutputMessageType::Reply(node_id) => {
            if let Err(err) = send_to(&message, node_id, connections).await {
                log::error!("Failed to send reply {err}");
            }
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
    consumer_types: Vec<TypeId>,
    consumer: Sender<RPCMessage>,
    received_messages: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage)>>,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
    net_out_sender: &Sender<(NetworkMessage, OutputMessageType)>,
) {
    for consumer_type in consumer_types {
        if let Entry::Occupied(mut entry) = received_messages.entry(consumer_type) {
            while let Some((node_id, message)) = entry.get_mut().pop_front() {
                if let Err(err) = consumer
                    .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
                    .await
                {
                    entry.get_mut().push_front((node_id, err.0.into()));
                    return;
                }
            }

            entry.remove_entry();
        }

        consumers.insert(consumer_type, consumer.clone());
    }
}

async fn read_network_data(
    read_connection: ReadHalf,
    event_stream: Sender<(NodeId, NetworkMessage)>,
) {
    let peer_id = read_connection.node_id();

    let mut stream = tokio_util::codec::FramedRead::new(read_connection, NetWorkEventDecoder {});
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
