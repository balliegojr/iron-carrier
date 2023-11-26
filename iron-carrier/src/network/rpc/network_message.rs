use std::{sync::atomic::AtomicU16, vec};

use crate::message_types::{MessageType, MessageTypes};
use bytes::{Buf, BytesMut};
use num_traits::FromPrimitive;
use serde::{de::Deserialize, Serialize};
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
}

/// Represents a network message in wire format.
///
/// This type makes no assumptions regarding the Data content.
pub struct NetworkMessage {
    content: Vec<u8>,
}

impl std::fmt::Debug for NetworkMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkMessage")
            .field("content_len", &self.content.len())
            .field("id", &self.id())
            .field("type_id", &self.type_id())
            .field("is_reply", &self.is_reply())
            .field("is_ack", &self.is_ack())
            .field("is_ping", &self.is_ping())
            .field("is_cancel", &self.is_cancel())
            // .field("content", &&self.content[..self.content.len().min(10)])
            .finish()
    }
}

/*
 Message Format:

                                    1  1  1  1  1  1
      0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                      ID                       |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |          FLAGS           |      TYPE_ID       |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                  DATA_LENGTH                  |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                  DATA_LENGTH                  |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                     DATA                      |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

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
    pub fn try_decode(src: &mut BytesMut) -> anyhow::Result<Option<Self>> {
        if src.len() < 3 {
            return Ok(None);
        }

        if src[2].to_be() > flags::REPLY {
            let content = src[0..3].to_vec();
            src.advance(content.len());
            return Ok(Some(Self { content }));
        }

        if src.len() < 8 {
            return Ok(None);
        }

        let length = u32::from_be_bytes(src[4..8].try_into().unwrap()) as usize;
        if length > MAX_MESSAGE_SIZE {
            anyhow::bail!("Network message exceeds max allowed size");
        }

        if src.len() < 8 + length {
            src.reserve(8 + length - src.len());
            return Ok(None);
        }

        let content = src[0..8 + length].to_vec();
        src.advance(content.len());

        Ok(Some(Self { content }))
    }

    /// Encode `data` into network wire format
    pub fn encode<T>(data: T) -> anyhow::Result<Self>
    where
        T: MessageType + Serialize,
    {
        let id = MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self::encode_with_id_and_flags(data, id, 0)
    }

    fn encode_with_id_and_flags<T>(data: T, id: u16, flags: u8) -> anyhow::Result<Self>
    where
        T: MessageType + Serialize,
    {
        let payload = bincode::serialize(&data)?;

        let mut content = Vec::with_capacity(6 + payload.len());

        content.extend_from_slice(&id.to_be_bytes());
        content.push(flags.to_be());
        content.push((T::MESSAGE_TYPE as u8).to_be());

        if payload.len() > MAX_MESSAGE_SIZE {
            anyhow::bail!("message content exceedes max message size");
        }

        content.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        content.extend_from_slice(&payload);

        Ok(Self { content })
    }

    /// Creates an ACK NetworkMessage with `id`
    pub fn ack_message(id: u16) -> Self {
        let id = id.to_be_bytes();
        let content = vec![id[0], id[1], (flags::REPLY | flags::ACK).to_be()];

        Self { content }
    }

    /// Creates a PING NetworkMessage with `id`
    pub fn ping_message(id: u16) -> Self {
        let id = id.to_be_bytes();
        let content = vec![id[0], id[1], (flags::REPLY | flags::PING).to_be()];

        Self { content }
    }

    /// Creates a CANCEL NetworkMessage with `id`
    pub fn cancel_message(id: u16) -> Self {
        let id = id.to_be_bytes();
        let content = vec![id[0], id[1], (flags::REPLY | flags::CANCEL).to_be()];

        Self { content }
    }

    /// Creates a reply message with `id` and encoded `data`
    pub fn reply_message<T>(id: u16, data: T) -> anyhow::Result<Self>
    where
        T: MessageType + Serialize,
    {
        Self::encode_with_id_and_flags(data, id, flags::REPLY)
    }

    /// Try to deserialize the message data into `T`, it fails the current message is of different
    /// type
    pub fn data<'a, T: MessageType + Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        if self.type_id()? != T::MESSAGE_TYPE {
            anyhow::bail!("Received invalid reply");
        }

        bincode::deserialize(&self.content[8..])
            .map_err(|err| anyhow::anyhow!("Received invalid reply {err}"))
    }

    /// Writes the current message bytes into `writer`
    pub async fn write_into(
        &self,
        writer: &mut (impl AsyncWrite + std::marker::Unpin),
    ) -> anyhow::Result<()> {
        writer.write_all(&self.content).await?;
        writer.flush().await.map_err(anyhow::Error::from)
    }

    /// Returns the message id
    pub fn id(&self) -> u16 {
        u16::from_be_bytes(self.content[0..2].try_into().unwrap())
    }

    /// Returns the `TypeId` of this message, if a message doesn't contain a TypeId, returns 0
    /// instead
    pub fn type_id(&self) -> anyhow::Result<MessageTypes> {
        if self.content.len() < 4 {
            anyhow::bail!("No Message Type Id for this message")
        } else {
            MessageTypes::from_u8(u8::from_be(self.content[3])).ok_or_else(|| anyhow::anyhow!(""))
        }
    }

    /// Returns true if message has the REPLY flag
    pub fn is_reply(&self) -> bool {
        self.content[2].to_le() & flags::REPLY == flags::REPLY
    }

    /// Returns true if message has the ACK flag
    pub fn is_ack(&self) -> bool {
        self.content[2].to_le() & flags::ACK == flags::ACK
    }

    /// Returns true if message has the PING flag
    pub fn is_ping(&self) -> bool {
        self.content[2].to_le() & flags::PING == flags::PING
    }

    /// Returns true if message has the CANCEL flag
    pub fn is_cancel(&self) -> bool {
        self.content[2].to_le() & flags::CANCEL == flags::CANCEL
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, MessageType, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct TestingMessage {
        num: u32,
        text: String,
    }

    #[test]
    fn ensure_encoded_message_can_be_decoded() {
        let original_data = TestingMessage {
            num: 10,
            text: "secret".into(),
        };

        let message =
            NetworkMessage::encode(original_data.clone()).expect("Failed to encode message");

        let mut bytes = BytesMut::new();
        bytes.put(&message.content[..]);

        let decoded_message = match NetworkMessage::try_decode(&mut bytes) {
            Ok(Some(decoded)) => decoded,
            Ok(_) => panic!("Failed to decode message"),
            Err(err) => panic!("Failed to decode message {err}"),
        };

        assert_eq!(message.content, decoded_message.content);

        assert_eq!(
            decoded_message
                .data::<TestingMessage>()
                .expect("Failed to extract data"),
            original_data
        );
    }

    #[test]
    fn ensure_ack() {
        let message = NetworkMessage::encode(TestingMessage {
            num: 10,
            text: "secret".into(),
        })
        .expect("Failed to encode message");

        let ack = NetworkMessage::ack_message(message.id());
        assert_eq!(ack.content.len(), 3);
        assert!(ack.is_ack());

        let mut bytes = BytesMut::new();
        bytes.put(&ack.content[..]);

        let decoded_message = NetworkMessage::try_decode(&mut bytes)
            .expect("Failed to decode")
            .unwrap();

        assert_eq!(ack.id(), message.id());
        assert_eq!(ack.content, decoded_message.content);
    }

    #[test]
    fn ensure_ping() {
        let message = NetworkMessage::encode(TestingMessage {
            num: 10,
            text: "secret".into(),
        })
        .expect("Failed to encode message");

        let ping = NetworkMessage::ping_message(message.id());
        assert_eq!(ping.content.len(), 3);
        assert!(ping.is_ping());

        let mut bytes = BytesMut::new();
        bytes.put(&ping.content[..]);

        let decoded_message = NetworkMessage::try_decode(&mut bytes)
            .expect("Failed to decode")
            .unwrap();

        assert_eq!(ping.id(), message.id());
        assert_eq!(ping.content, decoded_message.content);
    }

    #[test]
    fn ensure_reply_can_be_decoded() {
        let message = NetworkMessage::encode(TestingMessage {
            num: 10,
            text: "secret".into(),
        })
        .expect("Failed to encode message");

        let reply = NetworkMessage::reply_message(
            message.id(),
            TestingMessage {
                num: 1,
                text: "this is a reply".into(),
            },
        )
        .expect("Failed to encode reply");

        assert!(reply.is_reply());

        let mut bytes = BytesMut::new();
        bytes.put(&reply.content[..]);

        let decoded_message = NetworkMessage::try_decode(&mut bytes)
            .expect("Failed to decode")
            .unwrap();

        assert_eq!(reply.content, decoded_message.content);
    }
}
