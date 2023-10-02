use std::{sync::atomic::AtomicU16, vec};

use crate::hash_type_id::{HashTypeId, TypeId};
use bytes::{Buf, BytesMut};
use serde::{de::Deserialize, Serialize};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::IronCarrierError;

const MAX: usize = 8 * 1024 * 1024;
static MESSAGE_ID: AtomicU16 = AtomicU16::new(1);

mod flags {
    pub const REPLY: u8 = 0b0000_0001;
    pub const ACK: u8 = 0b0000_0010;
    pub const PING: u8 = 0b000_0100;
}

#[derive(Debug)]
pub struct NetworkMessage {
    content: Vec<u8>,
}

impl NetworkMessage {
    pub fn try_decode(src: &mut BytesMut) -> crate::Result<Option<Self>> {
        if src.len() < 3 {
            return Ok(None);
        }

        if src[2] & flags::ACK == flags::ACK || src[2] & flags::PING == flags::PING {
            let content = src[0..3].to_vec();
            src.advance(content.len());
            return Ok(Some(Self { content }));
        }

        if src.len() < 13 {
            return Ok(None);
        }

        let length = u16::from_le_bytes(src[11..13].try_into().unwrap()) as usize;
        if length > MAX {
            return Err(Box::new(IronCarrierError::ParseNetworkMessage));
        }

        if src.len() < 13 + length {
            src.reserve(13 + length - src.len());
            return Ok(None);
        }

        let content = src[0..13 + length].to_vec();
        src.advance(content.len());

        Ok(Some(Self { content }))
    }

    pub fn encode<T>(data: T) -> crate::Result<Self>
    where
        T: HashTypeId + Serialize,
    {
        let data = bincode::serialize(&data)?;

        let mut content = Vec::with_capacity(13 + data.len());
        let id = MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        content.extend_from_slice(&id.to_le_bytes());
        content.push(0);
        content.extend_from_slice(&T::ID.to_le_bytes());
        content.extend_from_slice(&(data.len() as u16).to_le_bytes());
        content.extend_from_slice(&data);

        Ok(Self { content })
    }

    pub fn ack_message(id: u16) -> Self {
        let id = id.to_le_bytes();
        let content = vec![id[0], id[1], flags::REPLY | flags::ACK];

        Self { content }
    }

    pub fn ping_message(id: u16) -> Self {
        let id = id.to_le_bytes();
        let content = vec![id[0], id[1], flags::REPLY | flags::PING];

        Self { content }
    }

    pub fn reply_message<T>(id: u16, data: T) -> crate::Result<Self>
    where
        T: HashTypeId + Serialize,
    {
        let data = bincode::serialize(&data)?;

        let mut content = Vec::with_capacity(13 + data.len());

        content.extend_from_slice(&id.to_le_bytes());
        content.push(flags::REPLY);
        content.extend_from_slice(&T::ID.to_le_bytes());
        content.extend_from_slice(&(data.len() as u16).to_le_bytes());
        content.extend_from_slice(&data);

        Ok(Self { content })
    }

    pub fn data<'a, T: HashTypeId + Deserialize<'a>>(&'a self) -> crate::Result<T> {
        if self.is_ack() || self.type_id() != T::ID {
            Err(IronCarrierError::InvalidReply)?;
        }

        bincode::deserialize(&self.content[13..]).map_err(|_| IronCarrierError::InvalidReply.into())
    }

    pub async fn write_into(
        &self,
        writer: &mut (impl AsyncWrite + std::marker::Unpin),
    ) -> crate::Result<()> {
        writer.write_all(&self.content).await?;
        writer.flush().await.map_err(Box::from)
    }

    pub fn id(&self) -> u16 {
        u16::from_le_bytes(self.content[0..2].try_into().unwrap())
    }

    pub fn type_id(&self) -> TypeId {
        if self.content.len() < 13 {
            0.into()
        } else {
            u64::from_le_bytes(self.content[3..11].try_into().unwrap()).into()
        }
    }

    pub fn is_reply(&self) -> bool {
        self.content[2] & flags::REPLY == flags::REPLY
    }

    pub fn is_ack(&self) -> bool {
        self.content[2] & flags::ACK == flags::ACK
    }

    pub fn is_ping(&self) -> bool {
        self.content[2] & flags::PING == flags::PING
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use iron_carrier_macros::HashTypeId;
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, HashTypeId, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct Data {
        num: u32,
        text: String,
    }

    #[test]
    fn ensure_encoded_message_can_be_decoded() {
        let original_data = Data {
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
                .data::<Data>()
                .expect("Failed to extract data"),
            original_data
        );
    }

    #[test]
    fn ensure_ack() {
        let message = NetworkMessage::encode(Data {
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

        assert_eq!(ack.content, decoded_message.content);
    }

    #[test]
    fn ensure_reply_can_be_decoded() {
        let message = NetworkMessage::encode(Data {
            num: 10,
            text: "secret".into(),
        })
        .expect("Failed to encode message");

        let reply = NetworkMessage::reply_message(
            message.id(),
            Data {
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
