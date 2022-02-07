mod commands;
pub use commands::Commands;

mod connection_manager;
pub use connection_manager::ConnectionManager;

mod dispatcher;
pub use dispatcher::CommandDispatcher;

mod peer_connection;
pub use peer_connection::PeerConnection;

use crate::IronCarrierError;

use self::connection_manager::ConnectionFlow;

pub enum RawMessageType {
    SetId = 0,
    Command,
    Stream,
    Ping,
}

impl TryFrom<u8> for RawMessageType {
    type Error = crate::IronCarrierError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RawMessageType::SetId),
            1 => Ok(RawMessageType::Command),
            2 => Ok(RawMessageType::Stream),
            _ => Err(IronCarrierError::InvalidMessage),
        }
    }
}

#[derive(Debug)]
pub enum HandlerEvent {
    Command(Commands),
    Connection(ConnectionFlow),
    ConsumeQueue,
}
