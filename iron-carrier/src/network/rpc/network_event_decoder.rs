use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use super::network_message::NetworkMessage;

pub struct NetWorkEventDecoder;

impl Decoder for NetWorkEventDecoder {
    type Item = NetworkMessage;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&mut *src);
        let message = NetworkMessage::try_decode(&mut cursor);
        if message.is_some() {
            let position = cursor.position() as usize;
            src.advance(position);
        }

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use crate::states::consensus::{ConsensusReached, StartConsensus};

    use super::*;

    #[test]
    fn empty_buffer_return_none() {
        let mut bytes = BytesMut::new();

        assert!(NetWorkEventDecoder.decode(&mut bytes).unwrap().is_none());
    }

    #[tokio::test]
    async fn partial_message_return_none() {
        let mut message_bytes = Vec::new();
        NetworkMessage::new(ConsensusReached, Some(10))
            .unwrap()
            .write_into(&mut message_bytes)
            .await
            .unwrap();

        let mut bytes = BytesMut::new();
        bytes.extend(&message_bytes[..2]);

        assert!(NetWorkEventDecoder.decode(&mut bytes).unwrap().is_none());
    }

    #[tokio::test]
    async fn can_decode_multiple_messages() {
        let messages = [
            NetworkMessage::new(ConsensusReached, Some(10)).unwrap(),
            NetworkMessage::new(ConsensusReached, Some(10)).unwrap(),
            NetworkMessage::new(StartConsensus, None).unwrap(),
        ];

        let mut message_bytes = Vec::new();
        for message in &messages {
            message.write_into(&mut message_bytes).await.unwrap();
        }

        let mut bytes = BytesMut::new();
        bytes.extend(&message_bytes);

        for message in &messages {
            let decoded = NetWorkEventDecoder.decode(&mut bytes).unwrap();
            assert!(decoded.is_some());
            assert_eq!(message.id(), decoded.unwrap().id());
        }

        assert!(!bytes.has_remaining());
    }
}
