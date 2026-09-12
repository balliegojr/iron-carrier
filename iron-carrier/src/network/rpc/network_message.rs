use std::{io::Cursor, sync::atomic::AtomicU16};

use crate::protocol::{MessageTypes, Protocol};
use bytes::{Buf, BytesMut};
use num_traits::FromPrimitive;
use serde::{Serialize, de::Deserialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};

const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;
/// Atomic counter to generate unique message IDs for sender node.  
///
/// The message id is only used to map replies to the correct channel and future, it doesn't need
/// to be globaly unique and it is fine if the id overflows (unless there are more than u16::MAX
/// messages at the same time)
static MESSAGE_ID: AtomicU16 = AtomicU16::new(1);

mod flags {
    pub const REPLY: u8 = 0b0000_0001;
    pub const ACK: u8 = 0b0000_0010;
    pub const PING: u8 = 0b000_0100;
    pub const CANCEL: u8 = 0b000_1000;
    pub const SUB_PROCESS: u8 = 0b100_0000;

    pub const TYPELESS_MASK: u8 = ACK | PING | CANCEL;
}

/// Represents a network message in wire format.
///
/// This type makes no assumptions regarding the Data content.
#[derive(Clone)]
pub struct NetworkMessage {
    id: u16,
    flags: u8,
    message_type: Option<MessageTypes>,
    sub_process: Option<u64>,
    content: Option<Vec<u8>>,
}

impl std::fmt::Debug for NetworkMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("NetworkMessage");

        d.field("content_len", &self.content.as_ref().map_or(0, |c| c.len()))
            .field("id", &self.id());

        if let Some(message_type) = &self.message_type {
            d.field("message_type", message_type);
        }

        if let Some(sub_process) = &self.sub_process {
            d.field("sub_process", sub_process);
        }

        if self.is_reply() {
            d.field("is_reply", &self.is_reply());
        }
        if self.is_ack() {
            d.field("is_ack", &self.is_ack());
        }
        if self.is_ping() {
            d.field("is_ping", &self.is_ping());
        }
        if self.is_cancel() {
            d.field("is_cancel", &self.is_cancel());
        }
        d.finish()
    }
}

/*
 * TODO: update message format
 Message Format:

    ID          A 16 bit message id
    FLAGS       A 8 bit field representing message flags
    TYPE_ID     A 8 bit field representing the message type
    DATA_LENGTH A 32 bit field representing the length of the DATA
    DATA        Message DATA

    Messages with the flags ACK, PING and CANCEL are only 3 bytes long
*/

impl NetworkMessage {
    /// Try to read a full message from the provider `src` buffer, if there are enough bytes
    /// Some(Self) will be returned and the buffer advanced to the next message
    pub fn try_decode(cursor: &mut Cursor<&mut BytesMut>) -> Option<Self> {
        let id = cursor.try_get_u16().ok()?;
        let flags = cursor.try_get_u8().ok()?;

        let is_typeless_message = flags & flags::TYPELESS_MASK > 0;
        if is_typeless_message {
            return Some(Self {
                id,
                flags,
                message_type: None,
                sub_process: None,
                content: None,
            });
        }

        let message_type = cursor.try_get_u8().ok().and_then(MessageTypes::from_u8)?;
        let sub_process = if flags & flags::SUB_PROCESS > 0 {
            Some(cursor.try_get_u64().ok()?)
        } else {
            None
        };

        let content = if message_type.has_payload() {
            let len = cursor.try_get_u16().ok()? as usize;
            if len > MAX_MESSAGE_SIZE {
                panic!("Network message exceeds max allowed size");
            }

            if cursor.remaining() >= len {
                let content = cursor.get_ref()
                    [cursor.position() as usize..cursor.position() as usize + len]
                    .to_vec();
                cursor.advance(len);

                Some(content)
            } else {
                return None;
            }
        } else {
            None
        };

        Some(Self {
            id,
            flags,
            message_type: Some(message_type),
            sub_process,
            content,
        })
    }

    /// Encode `data` into network wire format
    pub fn new<T>(data: T, sub_process: Option<u64>) -> anyhow::Result<Self>
    where
        T: Protocol + Serialize,
    {
        let id = MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let content = Self::encode_payload(data)?;

        Ok(Self {
            content,
            id,
            flags: if sub_process.is_some() {
                flags::SUB_PROCESS
            } else {
                0
            },
            message_type: Some(T::MESSAGE_TYPE),
            sub_process,
        })
    }

    fn encode_payload<T>(data: T) -> anyhow::Result<Option<Vec<u8>>>
    where
        T: Protocol + Serialize,
    {
        if T::MESSAGE_TYPE.has_payload() {
            let payload = postcard::to_allocvec(&data)?;
            if payload.len() > MAX_MESSAGE_SIZE {
                anyhow::bail!("message content exceedes max message size");
            }

            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    fn new_with_flag(&self, flags: u8) -> Self {
        Self {
            id: self.id,
            flags,
            message_type: None,
            sub_process: None,
            content: None,
        }
    }

    /// Creates an ACK NetworkMessage with `id`
    pub fn ack_message(&self) -> Self {
        self.new_with_flag(flags::REPLY | flags::ACK)
    }

    /// Creates a PING NetworkMessage with `id`
    pub fn ping_message(&self) -> Self {
        self.new_with_flag(flags::REPLY | flags::PING)
    }

    /// Creates a reply message with `id` and encoded `data`
    pub fn reply_message<T>(&self, data: T) -> anyhow::Result<Self>
    where
        T: Protocol + Serialize,
    {
        Self::encode_payload(data).map(|content| Self {
            id: self.id,
            flags: flags::REPLY,
            message_type: Some(T::MESSAGE_TYPE),
            sub_process: None,
            content,
        })
    }

    /// Try to deserialize the message data into `T`, it fails the current message is of different
    /// type
    pub fn data<'a, T: Protocol + Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        if self.message_type.is_none_or(|t| t != T::MESSAGE_TYPE) {
            anyhow::bail!("Requested message type does not match message received");
        }

        let Some(payload) = self.content.as_ref() else {
            anyhow::bail!("Request message type does not have any content")
        };

        postcard::from_bytes(payload).map_err(|err| anyhow::anyhow!("Received invalid reply {err}"))
    }

    /// Writes the current message bytes into `writer`
    pub async fn write_into(
        &self,
        writer: &mut (impl AsyncWrite + std::marker::Unpin),
    ) -> anyhow::Result<()> {
        writer.write_u16(self.id).await?;

        writer.write_u8(self.flags).await?;
        if let Some(message_type) = self.message_type {
            writer.write_u8(message_type as u8).await?;
        }

        if let Some(sub_process) = self.sub_process {
            writer.write_u64(sub_process).await?;
        }

        if let Some(payload) = self.content.as_ref() {
            writer.write_u16(payload.len() as u16).await?;
            writer.write_all(payload).await?;
        }

        writer.flush().await.map_err(anyhow::Error::from)
    }

    /// Returns the message id
    pub fn id(&self) -> u16 {
        self.id
    }

    /// Returns the `TypeId` of this message, if a message doesn't contain a TypeId, returns 0
    /// instead
    pub fn type_id(&self) -> Option<MessageTypes> {
        self.message_type
    }

    pub fn sub_process(&self) -> Option<u64> {
        self.sub_process
    }

    /// Returns true if message has the REPLY flag
    pub fn is_reply(&self) -> bool {
        self.flags & flags::REPLY == flags::REPLY
    }

    /// Returns true if message has the ACK flag
    pub fn is_ack(&self) -> bool {
        self.flags & flags::ACK == flags::ACK
    }

    /// Returns true if message has the PING flag
    pub fn is_ping(&self) -> bool {
        self.flags & flags::PING == flags::PING
    }

    /// Returns true if message has the CANCEL flag
    pub fn is_cancel(&self) -> bool {
        self.flags & flags::CANCEL == flags::CANCEL
    }
}

#[cfg(test)]
mod tests {
    use crate::states::consensus::{RequestVote, StartConsensus};

    use super::*;

    #[tokio::test]
    async fn ensure_encoded_message_can_be_decoded() {
        let original_data = RequestVote { term: 10 };
        let message = NetworkMessage::new(original_data, None).expect("Failed to encode message");

        let decoded_message = encode_and_decode(&message).await;

        assert_eq!(message.content, decoded_message.content);
        assert_eq!(message.message_type, decoded_message.message_type);

        let decoded_content = decoded_message
            .data::<RequestVote>()
            .expect("Failed to extract data");

        assert_eq!(decoded_content.term, 10);
    }

    #[tokio::test]
    async fn ensure_contentless_message_can_be_decoded() {
        let message = NetworkMessage::new(StartConsensus, None).expect("Failed to encode message");

        let decoded_message = encode_and_decode(&message).await;
        assert!(message.content.is_none());
        assert!(decoded_message.content.is_none());

        assert_eq!(message.type_id(), Some(StartConsensus::MESSAGE_TYPE));
    }

    #[tokio::test]
    async fn ensure_encoded_message_with_sub_process_can_be_decoded() {
        let original_data = RequestVote { term: 10 };
        let message =
            NetworkMessage::new(original_data, Some(10)).expect("Failed to encode message");

        let decoded_message = encode_and_decode(&message).await;

        assert_eq!(decoded_message.sub_process, Some(10));
    }

    #[tokio::test]
    async fn ensure_ack_can_be_decoded() {
        let original_data = RequestVote { term: 10 };
        let message =
            NetworkMessage::new(original_data, Some(10)).expect("Failed to encode message");

        let ack = message.ack_message();
        assert!(ack.content.is_none());
        assert!(ack.is_ack());
        assert!(ack.sub_process.is_none());
        assert!(ack.message_type.is_none());

        let decoded_message = encode_and_decode(&ack).await;

        assert_eq!(ack.id(), decoded_message.id());
        assert!(decoded_message.is_ack());
        assert!(decoded_message.sub_process.is_none());
        assert!(decoded_message.content.is_none());
        assert!(decoded_message.message_type.is_none());
    }

    #[tokio::test]
    async fn ensure_ping_can_be_decoded() {
        let original_data = RequestVote { term: 10 };
        let message =
            NetworkMessage::new(original_data, Some(10)).expect("Failed to encode message");

        let ping = message.ping_message();
        assert!(ping.is_ping());
        assert!(ping.sub_process.is_none());
        assert!(ping.content.is_none());
        assert!(ping.message_type.is_none());

        let decoded_message = encode_and_decode(&ping).await;

        assert_eq!(ping.id(), decoded_message.id());
        assert!(decoded_message.is_ping());
        assert!(decoded_message.sub_process.is_none());
        assert!(decoded_message.content.is_none());
        assert!(decoded_message.message_type.is_none());
    }

    #[tokio::test]
    async fn ensure_reply_can_be_decoded() {
        let original_data = RequestVote { term: 10 };
        let message =
            NetworkMessage::new(original_data, Some(10)).expect("Failed to encode message");

        let reply = message
            .reply_message(RequestVote { term: 11 })
            .expect("Failed to encode reply");

        assert!(reply.is_reply());
        assert!(reply.sub_process.is_none());
        assert!(reply.content.is_some());
        assert_eq!(reply.message_type, Some(RequestVote::MESSAGE_TYPE));

        let decoded_message = encode_and_decode(&reply).await;

        assert_eq!(reply.id(), decoded_message.id());
        assert!(decoded_message.is_reply());
        assert!(decoded_message.sub_process.is_none());
        assert!(decoded_message.content.is_some());
        assert_eq!(
            decoded_message.message_type,
            Some(RequestVote::MESSAGE_TYPE)
        );

        let reply_content: RequestVote = decoded_message.data().expect("failed to decode content");

        assert_eq!(reply_content.term, 11);
    }

    async fn encode_and_decode(message: &NetworkMessage) -> NetworkMessage {
        let mut buffer = Vec::new();
        message
            .write_into(&mut buffer)
            .await
            .expect("Failed to write message");

        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&buffer);

        let mut cursor = Cursor::new(&mut bytes);

        NetworkMessage::try_decode(&mut cursor)
            .unwrap_or_else(|| panic!("Failed to decode message"))
    }
}
