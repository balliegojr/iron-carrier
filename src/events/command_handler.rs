use message_io::{
    network::{Endpoint, NetEvent, Transport},
    node::{self, NodeHandler, NodeListener},
};
// use rand::Rng;
use std::{
    collections::LinkedList,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    config::Config,
    connection::ConnectionHandler,
    constants::{
        CLEAR_TIMEOUT,
        PING_CONNECTIONS,
        START_CONNECTIONS_RETRIES,
        START_CONNECTIONS_RETRY_WAIT,
        // START_NEGOTIATION_MAX, START_NEGOTIATION_MIN,
    },
    debouncer::{debounce_action, Debouncer},
};

use super::{CommandDispatcher, Commands, HandlerEvent, RawMessageType};

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;

pub struct CommandHandler {
    config: &'static Config,

    event_queue: Arc<Mutex<LinkedList<Commands>>>,
    can_start_negotiations: bool,

    handler: NodeHandler<HandlerEvent>,
    dispatcher: CommandDispatcher,
    listener: Option<NodeListener<HandlerEvent>>,

    connection_handler: Arc<Mutex<ConnectionHandler>>,
}

impl CommandHandler {
    pub fn new(config: &'static Config) -> crate::Result<Self> {
        let (handler, listener) = node::split::<HandlerEvent>();
        let connection_handler =
            Arc::new(Mutex::new(ConnectionHandler::new(config, handler.clone())?));

        let event_queue = Arc::new(Mutex::new(LinkedList::new()));
        let dispatcher = CommandDispatcher::new(
            handler.clone(),
            connection_handler.clone(),
            event_queue.clone(),
        );

        Ok(Self {
            handler,
            listener: Some(listener),
            config,
            event_queue,
            dispatcher,
            connection_handler,
            can_start_negotiations: true,
        })
    }

    fn clear(&mut self) {
        self.event_queue.lock().unwrap().clear()
    }

    pub fn command_dispatcher(&self) -> CommandDispatcher {
        self.dispatcher.clone()
    }

    pub fn on_command(
        mut self,
        mut command_callback: impl FnMut(Commands, Option<String>) -> crate::Result<bool>,
    ) -> crate::Result<()> {
        log::trace!("Starting on_command");

        self.send_initial_events();
        self.handler.network().listen(
            TRANSPORT_PROTOCOL,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.config.port),
        )?;

        let dispatcher = self.command_dispatcher();
        let clear = debounce_action(Duration::from_secs(CLEAR_TIMEOUT), move || {
            dispatcher.now(Commands::Clear);
        });

        self.listener
            .take()
            .expect("on_command can only be called once")
            .for_each(|event| {
                let result = match event {
                    node::NodeEvent::Network(net_event) => match net_event {
                        NetEvent::Message(endpoint, data) => {
                            self.handle_network_message(endpoint, data)
                        }
                        event => {
                            self.connection_handler
                                .lock()
                                .unwrap()
                                .handle_net_event(event);
                            Ok(None)
                        }
                    },
                    node::NodeEvent::Signal(signal_event) => self.handle_signal(signal_event),
                };

                match result {
                    Ok(maybe_command) => {
                        if let Some((command, peer_id)) = maybe_command {
                            self.execute_command(command, peer_id, &mut command_callback, &clear);
                        }
                    }
                    Err(err) => {
                        log::error!("Failed to process event: {}", err);
                    }
                }
            });

        Ok(())
    }

    fn execute_command(
        &mut self,
        command: Commands,
        peer_id: Option<String>,
        command_callback: &mut impl FnMut(Commands, Option<String>) -> crate::Result<bool>,
        clear: &Debouncer,
    ) {
        match &peer_id {
            Some(peer_id) => {
                log::trace!(
                    "{} - Executing command from {}: {}",
                    self.config.node_id,
                    peer_id,
                    &command
                );
            }
            None => {
                log::trace!("{} - Executing command: {} ", self.config.node_id, &command);
            }
        }

        if !matches!(command, Commands::Clear) {
            clear.invoke();
        } else {
            self.clear();
        }

        match command_callback(command, peer_id) {
            Ok(consume_queue) => {
                if consume_queue {
                    self.handler.signals().send(HandlerEvent::ConsumeQueue);
                }
            }
            Err(err) => {
                log::error!("Failed to process command: {}", err);
            }
        }
    }

    fn handle_signal(
        &mut self,
        event: HandlerEvent,
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        match event {
            HandlerEvent::Command(signal_event) => Ok(Some((signal_event, None))),
            HandlerEvent::Connection(control_event) => {
                self.handle_connection_flow(control_event)?;
                Ok(None)
            }
            HandlerEvent::ConsumeQueue => Ok(self
                .event_queue
                .lock()
                .expect("Poisoned lock")
                .pop_front()
                .map(|command| (command, None))),
        }
    }

    fn send_initial_events(&self) {
        self.handler.signals().send_with_timer(
            ConnectionFlow::StartConnections(START_CONNECTIONS_RETRIES).into(),
            Duration::from_secs(3),
        );
        self.handler.signals().send_with_timer(
            ConnectionFlow::PingConnections.into(),
            Duration::from_secs(PING_CONNECTIONS),
        );
    }

    // fn start_negotiations(&mut self) {
    //     if !self.can_start_negotiations
    //         || self
    //             .connection_handler
    //             .lock()
    //             .unwrap()
    //             .has_waiting_identification()
    //     {
    //         return;
    //     }
    //
    //     self.can_start_negotiations = false;
    //
    //     let mut rng = rand::thread_rng();
    //     let timeout = rng.gen_range(START_NEGOTIATION_MIN..START_NEGOTIATION_MAX);
    //
    //     self.handler
    //         .signals()
    //         .send_with_timer(Negotiation::Start.into(), Duration::from_secs_f64(timeout));
    // }

    fn handle_network_message(
        &mut self,
        endpoint: Endpoint,
        data: &[u8],
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        self.connection_handler
            .lock()
            .unwrap()
            .touch_connection(&endpoint);

        let message_type = match RawMessageType::try_from(data[0]) {
            Ok(message_type) => message_type,
            Err(err) => {
                log::error!("Received invalid message {} from {endpoint:?}", data[0]);
                return Err(err.into());
            }
        };

        match message_type {
            RawMessageType::SetId => {
                let (node_id, group): (String, Option<String>) = bincode::deserialize(&data[1..])?;
                self.connection_handler
                    .lock()
                    .unwrap()
                    .set_node_id(node_id, endpoint, group);

                // self.start_negotiations();
            }
            RawMessageType::Command => {
                let message = bincode::deserialize(&data[1..])?;
                match self
                    .connection_handler
                    .lock()
                    .unwrap()
                    .get_peer_id(&endpoint)
                {
                    Some(peer_id) => {
                        return Ok(Some((message, Some(peer_id))));
                    }
                    None => {
                        log::error!("Received message from unknown endpoint");
                    }
                }
            }
            RawMessageType::Stream => match self
                .connection_handler
                .lock()
                .unwrap()
                .get_peer_id(&endpoint)
            {
                Some(peer_id) => {
                    return Ok(Some((
                        Commands::Stream(data[1..].to_owned()),
                        Some(peer_id),
                    )))
                }
                None => {
                    log::error!("Received message from unknown endpoint");
                }
            },
            RawMessageType::Ping => {
                // There is no need to do anything, the connection was already touched by this point
            }
        }

        Ok(None)
    }

    fn handle_connection_flow(&mut self, event: ConnectionFlow) -> crate::Result<()> {
        match event {
            ConnectionFlow::StartConnections(0) => {
                self.can_start_negotiations = false;
                self.event_queue.lock().expect("Poisoned lock").clear();
            }
            ConnectionFlow::StartConnections(attempt_left) => {
                if !self
                    .connection_handler
                    .lock()
                    .unwrap()
                    .start_connections()?
                {
                    let wait_secs =
                        (START_CONNECTIONS_RETRIES - attempt_left) * START_CONNECTIONS_RETRY_WAIT;
                    self.handler.signals().send_with_timer(
                        ConnectionFlow::StartConnections(attempt_left - 1).into(),
                        Duration::from_secs(wait_secs as u64),
                    );
                }
            }
            ConnectionFlow::CheckConnectionLiveness => {
                let should_start_negotiations = {
                    let mut connection_handler = self.connection_handler.lock().unwrap();
                    connection_handler.liveness();
                    !connection_handler.has_waiting_identification()
                        && connection_handler.has_connections()
                };

                if should_start_negotiations {
                    // self.start_negotiations();
                }
            }
            ConnectionFlow::PingConnections => {
                self.connection_handler.lock().unwrap().ping_connections();
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ConnectionFlow {
    StartConnections(u8),
    CheckConnectionLiveness,
    PingConnections,
}

impl From<ConnectionFlow> for HandlerEvent {
    fn from(v: ConnectionFlow) -> Self {
        HandlerEvent::Connection(v)
    }
}
