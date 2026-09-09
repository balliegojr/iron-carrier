use crate::file_transfer::events::*;
use crate::states::consensus::{ConsensusReached, RequestVote, StartConsensus, TermVote};
use crate::states::daemon::Follow;
use crate::states::sync::events::{
    DeleteFile, ListStorageNames, ListStorageNamesReply, MoveFile, QueryStorageIndex, ReceiveFile,
    SaveSyncStatus, SendFileTo, StorageIndex, SyncCompleted,
};

use num_derive::FromPrimitive;

macro_rules! message_types_enum {
    ($($i:tt,)+) => {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Hash, FromPrimitive)]
        #[repr(u8)]
        pub enum MessageTypes {
            $(
                $i,
            )+
        }

        impl MessageTypes {
            pub fn has_payload(self) -> bool {
                match self {
                    $(
                        MessageTypes::$i => $i::HAS_PAYLOAD,
                    )+
                }
            }
        }
    }
}

message_types_enum! {
    StartConsensus,
    ConsensusReached,
    TermVote,
    RequestVote,

    Follow,

    SendFileTo,
    MoveFile,
    DeleteFile,
    ReceiveFile,
    SyncCompleted,
    ListStorageNames,
    ListStorageNamesReply,
    QueryStorageIndex,
    StorageIndex,
    SaveSyncStatus,

    QueryTransferType,
    TransferType,
    QueryRequiredBlocks,
    RequiredBlocks,
    TransferBlock,
    TransferComplete,
    TransferResult,

}

pub trait Protocol {
    const MESSAGE_TYPE: MessageTypes;
    const HAS_PAYLOAD: bool;
}

pub trait ProtocolAck: Protocol {}

pub trait ProtocolPayload: Protocol {}

pub trait ProtocolQuery: Protocol {
    type ResponseType: ProtocolPayload;
}

extern crate iron_carrier_macros;
pub use iron_carrier_macros::Protocol;
