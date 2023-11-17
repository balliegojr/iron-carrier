use num_derive::FromPrimitive;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash, FromPrimitive)]
#[repr(u8)]
pub enum MessageTypes {
    StartConsensus,
    ConsensusReached,
    TermVote,
    RequestVote,

    TransferFilesStart,
    TransferFilesCompleted,
    SendFileTo,
    MoveFile,
    DeleteFile,
    SyncCompleted,
    QueryStorageIndex,
    StorageIndex,

    QueryTransferType,
    TransferType,
    QueryRequiredBlocks,
    RequiredBlocks,
    TransferBlock,
    TransferComplete,
    TransferResult,

    TestingMessage = 255,
}

pub trait MessageType {
    const MESSAGE_TYPE: MessageTypes;
}

extern crate iron_carrier_macros;
pub use iron_carrier_macros::MessageType;
