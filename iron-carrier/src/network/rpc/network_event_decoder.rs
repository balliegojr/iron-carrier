use bytes::BytesMut;
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

        NetworkMessage::try_decode(src)
    }
}
