use crate::IronCarrierError;

mod commands;
pub use commands::Commands;

mod dispatcher;
pub use dispatcher::CommandDispatcher;

mod command_handler;
pub use command_handler::{CommandHandler, ConnectionFlow};

#[derive(Debug)]
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
            3 => Ok(RawMessageType::Ping),
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
