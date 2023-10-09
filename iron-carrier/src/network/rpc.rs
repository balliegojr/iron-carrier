use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    time::Duration,
};

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, hash_type_id::TypeId};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::Instant,
};
use tokio_stream::StreamExt;

use crate::node_id::NodeId;

use self::message_waiting_reply::ReplyType;

use super::{
    connection::{Connection, ReadHalf},
    connection_storage::ConnectionStorage,
};

mod message_waiting_reply;
use message_waiting_reply::MessageWaitingReply;

mod network_event_decoder;
use network_event_decoder::NetWorkEventDecoder;

mod network_message;
use network_message::NetworkMessage;

mod rpc_message;
pub use rpc_message::RPCMessage;

mod call;
mod rpc_reply;
mod subscriber;

mod group_call;
pub use group_call::GroupCallResponse;

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
    // Holds messages that arrived but didn't had any consumer ready to process it
    let mut unprocessed_message_queue: HashMap<
        TypeId,
        VecDeque<(NodeId, NetworkMessage, Deadline)>,
    > = Default::default();
    let mut sent_requests: HashMap<u16, MessageWaitingReply> = Default::default();

    let mut consumers: HashMap<TypeId, Sender<RPCMessage>> = Default::default();
    let mut connections = ConnectionStorage::default();

    let mut cleanup = tokio::time::interval(Duration::from_secs(1));

    loop {
        let has_cleanup = !(unprocessed_message_queue.is_empty()
            // && consumers.is_empty()
            && sent_requests.is_empty()
            && connections.is_empty());

        tokio::select! {
            biased;
            request = new_connection.recv() =>  {
                let Some(connection) = request else {
                    break;
                };

                // TODO: send unsent messages to this node
                if let Some(read) = connections.insert(connection) {
                    tokio::spawn(read_network_data(read, net_in_tx.clone()));
                }
            }
            request = net_in_rx.recv() => {
                let Some((node_id, message)) = request else {
                    break;
                };

                if message.is_reply() {
                    process_reply(message, node_id, &mut sent_requests).await;
                } else {
                    process_rpc_call(
                        message,
                        node_id,
                        &mut consumers,
                        &mut unprocessed_message_queue,
                        &net_out_sender,
                    )
                    .await;
                }
            }

            request = net_out.recv() => {
                let Some((message, send_type)) = request else { break; };
                send_output_message(message, send_type, &mut connections, &mut sent_requests).await;
            }

            request = add_consumers.recv() => {
                let Some((consumer_types, consumer_channel)) = request else {
                    break;
                };
                add_new_consumer(
                    consumer_types,
                    consumer_channel,
                    &mut unprocessed_message_queue,
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
            _ = cleanup.tick(), if has_cleanup => {
                cleanup_resources(&mut connections, &mut unprocessed_message_queue, &mut sent_requests, &mut consumers).await;
            }
        }
    }

    // It is necessary to ensure that all output messages are sent before exiting
    while let Some((message, send_type)) = net_out.recv().await {
        send_output_message(message, send_type, &mut connections, &mut sent_requests).await;
    }
}

struct Deadline {
    deadline: Instant,
}

impl Deadline {
    pub fn new(timeout: Duration) -> Self {
        let deadline = Instant::now() + timeout;
        Self { deadline }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    pub fn extend(&mut self, secs: u64) {
        self.deadline = Instant::now() + Duration::from_secs(secs)
    }
}

#[derive(Debug)]
pub enum OutputMessageType {
    Response(NodeId),
    SingleNode(NodeId, Sender<ReplyType>, Duration),
    MultiNode(HashSet<NodeId>, Sender<ReplyType>, Duration),
    Broadcast(Sender<ReplyType>, Duration),
}

async fn process_rpc_call(
    message: NetworkMessage,
    node_id: NodeId,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
    message_queue: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    net_out_sender: &Sender<(NetworkMessage, OutputMessageType)>,
) {
    if let Entry::Occupied(mut entry) = consumers.entry(message.type_id()) {
        if let Err(err) = entry
            .get_mut()
            .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
            .await
        {
            let message: NetworkMessage = err.0.into();
            message_queue
                .entry(message.type_id())
                .or_default()
                .push_back((
                    node_id,
                    message,
                    Deadline::new(Duration::from_secs(DEFAULT_NETWORK_TIMEOUT)),
                ));
            entry.remove_entry();
        }
    } else {
        message_queue
            .entry(message.type_id())
            .or_default()
            .push_back((
                node_id,
                message,
                Deadline::new(Duration::from_secs(DEFAULT_NETWORK_TIMEOUT)),
            ));
    }
}

async fn process_reply(
    message: NetworkMessage,
    node_id: NodeId,
    sent_requests: &mut HashMap<u16, MessageWaitingReply>,
) {
    match sent_requests.entry(message.id()) {
        Entry::Occupied(mut entry) => {
            let sent_request = entry.get_mut();
            if let Err(err) = sent_request.process_reply(node_id, message).await {
                log::error!("Failed to send reply {err}");
            }

            if sent_request.received_all_replies() {
                entry.remove_entry();
            }
        }
        Entry::Vacant(_) => {
            log::error!("Received an unexpected message from {node_id}");
        }
    }
}

async fn send_output_message(
    message: NetworkMessage,
    send_type: OutputMessageType,
    connections: &mut ConnectionStorage,
    sent_requests: &mut HashMap<u16, MessageWaitingReply>,
) {
    async fn send_to(
        message: &NetworkMessage,
        node_id: NodeId,
        connections: &mut ConnectionStorage,
    ) -> anyhow::Result<()> {
        let write_result = match connections.get_mut(&node_id) {
            Some(connection) => message.write_into(connection).await,
            None => {
                anyhow::bail!("Not connected to node {node_id}");
            }
        };

        if let Err(err) = write_result {
            connections.remove(&node_id);
            anyhow::bail!("Failed to write to connection {err}");
        }

        Ok(())
    }

    // FIXME: keep track of unsent messages, they must be sent if the node connects again
    match send_type {
        OutputMessageType::Response(node_id) => {
            if let Err(err) = send_to(&message, node_id, connections).await {
                log::error!("{err}");
            }
        }
        OutputMessageType::SingleNode(node_id, callback, timeout) => {
            if let Err(err) = send_to(&message, node_id, connections).await {
                log::error!("{err}");
            } else {
                sent_requests.insert(
                    message.id(),
                    MessageWaitingReply::new(message.id(), [node_id].into(), callback, timeout),
                );
            }
        }
        OutputMessageType::MultiNode(nodes, callback, timeout) => {
            // FIXME: nodes that are offline should already go in the "failed" list in the
            // MessageWaitingReply. Or they should be in a "waiting to send" list, since they were
            // online
            let mut nodes_sent = HashSet::new();
            for node_id in nodes {
                if let Err(err) = send_to(&message, node_id, connections).await {
                    log::error!("{err}");
                } else {
                    nodes_sent.insert(node_id);
                }
            }

            sent_requests.insert(
                message.id(),
                MessageWaitingReply::new(message.id(), nodes_sent, callback, timeout),
            );
        }
        OutputMessageType::Broadcast(callback, timeout) => {
            let mut nodes = HashSet::new();
            for node_id in connections.connected_nodes().collect::<Vec<_>>() {
                if let Err(err) = send_to(&message, node_id, connections).await {
                    log::error!("{err}");
                } else {
                    nodes.insert(node_id);
                }
            }

            sent_requests.insert(
                message.id(),
                MessageWaitingReply::new(message.id(), nodes, callback, timeout),
            );
        }
    }
}

async fn add_new_consumer(
    consumer_types: Vec<TypeId>,
    consumer: Sender<RPCMessage>,
    received_messages: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
    net_out_sender: &Sender<(NetworkMessage, OutputMessageType)>,
) {
    for consumer_type in consumer_types {
        if let Entry::Occupied(mut entry) = received_messages.entry(consumer_type) {
            while let Some((node_id, message, deadline)) = entry.get_mut().pop_front() {
                if let Err(err) = consumer
                    .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
                    .await
                {
                    entry
                        .get_mut()
                        .push_front((node_id, err.0.into(), deadline));
                    return;
                }
            }

            entry.remove_entry();
        }

        consumers.insert(consumer_type, consumer.clone());
    }
}

async fn cleanup_resources(
    connections: &mut ConnectionStorage,
    received_messages: &mut HashMap<TypeId, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    sent_requests: &mut HashMap<u16, MessageWaitingReply>,
    consumers: &mut HashMap<TypeId, tokio::sync::mpsc::Sender<RPCMessage>>,
) {
    connections.remove_stale();
    consumers.retain(|_, consumer| !consumer.is_closed());

    for (_, expired_message) in sent_requests.extract_if(|_, waiting| waiting.is_expired()) {
        if let Err(err) = expired_message.send_timeout().await {
            log::warn!("Failed to send timeout message {err}");
        }
    }

    for messages in received_messages.values_mut() {
        messages.retain(|(_, _, deadline)| !deadline.is_expired());
    }

    received_messages.retain(|_, messages| !messages.is_empty());
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::hash_type_id::HashTypeId;
    use iron_carrier_macros::HashTypeId;
    use serde::{Deserialize, Serialize};

    #[tokio::test]
    pub async fn ensure_rpc_single_call_times_out() {
        let (new_connection_tx, new_connection_rx) = tokio::sync::mpsc::channel(1);
        let rpc = rpc_service(new_connection_rx);
        let ping = ping_rpc(Duration::from_secs(1));

        let (ping_connection, connection) =
            crate::network::connection::local_connection_pair(0.into());

        let _ = ping.send(ping_connection).await;
        let _ = new_connection_tx.send(connection).await;

        assert!(rpc.call(Ack, 0.into()).ack().await.is_ok());
        assert!(rpc.call(Reply, 0.into()).ack().await.is_err());
    }

    #[tokio::test]
    pub async fn ensure_rpc_multi_call_times_out() {
        let (new_connection_tx, new_connection_rx) = tokio::sync::mpsc::channel(1);
        let rpc = rpc_service(new_connection_rx);

        let fast_server = ping_rpc(Duration::from_secs(1));
        let slow_server = ping_rpc(Duration::from_secs(10));

        {
            let (ping_connection, connection) =
                crate::network::connection::local_connection_pair(0.into());
            let _ = fast_server.send(ping_connection).await;
            let _ = new_connection_tx.send(connection).await;
        }

        {
            let (ping_connection, connection) =
                crate::network::connection::local_connection_pair(1.into());
            let _ = slow_server.send(ping_connection).await;
            let _ = new_connection_tx.send(connection).await;
        }

        assert_eq!(
            rpc.multi_call(Ack, [0.into()].into())
                .ack()
                .await
                .expect("Should not error"),
            HashSet::from([NodeId::from(0)])
        );
        assert_eq!(
            rpc.multi_call(Ack, [0.into(), 1.into()].into())
                .ack()
                .await
                .expect("Should not error"),
            HashSet::from([NodeId::from(0)])
        );

        assert_eq!(
            rpc.multi_call(Reply, [0.into()].into())
                .result()
                .await
                .expect("Should not error")
                .replies()
                .len(),
            1
        );

        match rpc
            .multi_call(Reply, [0.into()].into())
            .result()
            .await
            .expect("Should not error")
        {
            GroupCallResponse::Complete(replies) => assert_eq!(replies.len(), 1),
            GroupCallResponse::Partial(_, _) => unreachable!("Unexpected response"),
        }

        match rpc
            .multi_call(Reply, [0.into(), 1.into()].into())
            .result()
            .await
            .expect("Should not error")
        {
            GroupCallResponse::Complete(_) => unreachable!("Unexpected response"),
            GroupCallResponse::Partial(replies, nodes) => {
                assert_eq!(replies.len(), 1);
                assert_eq!(nodes, HashSet::from([NodeId::from(1)]))
            }
        }
    }

    #[tokio::test]
    pub async fn ensure_rpc_broadcast_times_out() {
        let (new_connection_tx, new_connection_rx) = tokio::sync::mpsc::channel(1);
        let rpc = rpc_service(new_connection_rx);

        let fast_server = ping_rpc(Duration::from_secs(1));
        let slow_server = ping_rpc(Duration::from_secs(10));

        {
            let (ping_connection, connection) =
                crate::network::connection::local_connection_pair(0.into());
            let _ = fast_server.send(ping_connection).await;
            let _ = new_connection_tx.send(connection).await;
        }

        {
            let (ping_connection, connection) =
                crate::network::connection::local_connection_pair(1.into());
            let _ = slow_server.send(ping_connection).await;
            let _ = new_connection_tx.send(connection).await;
        }

        assert_eq!(
            rpc.broadcast(Ack).ack().await.expect("Should not error"),
            HashSet::from([NodeId::from(0)])
        );

        match rpc
            .broadcast(Reply)
            .result()
            .await
            .expect("Should not error")
        {
            GroupCallResponse::Complete(_) => unreachable!("Unexpected response"),
            GroupCallResponse::Partial(replies, nodes) => {
                assert_eq!(replies.len(), 1);
                assert_eq!(nodes, HashSet::from([NodeId::from(1)]))
            }
        }
    }

    fn ping_rpc(wait_time: Duration) -> Sender<Connection> {
        let (new_connection_tx, new_connection_rx) = tokio::sync::mpsc::channel(1);
        let rpc = rpc_service(new_connection_rx);

        tokio::spawn(async move {
            let mut sub = rpc
                .subscribe_many([Reply::ID, Ack::ID].into())
                .await
                .unwrap();

            while let Some(message) = sub.next().await {
                tokio::spawn(async move {
                    tokio::time::sleep(wait_time).await;

                    match message.type_id() {
                        Reply::ID => {
                            let _ = message.reply(Reply).await;
                        }
                        Ack::ID => {
                            let _ = message.ack().await;
                        }
                        _ => unreachable!(),
                    }
                });
            }
        });

        new_connection_tx
    }

    #[derive(Debug, HashTypeId, Serialize, Deserialize)]
    struct Ack;

    #[derive(Debug, HashTypeId, Serialize, Deserialize)]
    struct Reply;
}
