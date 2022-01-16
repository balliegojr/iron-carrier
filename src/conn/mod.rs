mod connection_manager;
mod peer_connection;

pub use connection_manager::{CommandDispatcher, CommandType, ConnectionManager};

use crate::IronCarrierError;

pub enum RawMessageType {
    SetId = 0,
    Command,
    Stream,
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
