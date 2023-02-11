use std::error::Error;

use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use crate::{network_events::NetworkEvents, IronCarrierError};

pub struct NetWorkEventDecoder;

const MAX: usize = 8 * 1024 * 1024;

impl Decoder for NetWorkEventDecoder {
    type Item = NetworkEvents;
    type Error = Box<dyn Error + Send + Sync>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        if length > MAX {
            return Err(Box::new(IronCarrierError::ParseNetworkMessage));
        }

        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        let data = src[4..4 + length].to_vec();
        src.advance(4 + length);

        match bincode::deserialize(&data) {
            Ok(event) => Ok(Some(event)),
            Err(_) => Err(Box::new(IronCarrierError::ParseNetworkMessage)),
        }
    }
}
