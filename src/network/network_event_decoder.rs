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
        if src.is_empty() {
            return Ok(None);
        }

        if src[0] == 0 {
            decode_event(src)
        } else {
            decode_stream(src)
        }
    }
}

fn decode_stream(
    src: &mut BytesMut,
) -> Result<Option<NetworkEvents>, Box<dyn Error + Send + Sync>> {
    const STREAM_HEADER: usize = 21;

    if src.len() < STREAM_HEADER {
        return Ok(None);
    }

    // connection.write_u8(1).await?;
    // connection.write_u64(transfer_id).await?; // 1..9
    // connection.write_u64(block_index).await?; // 9..17
    // connection.write_u32(block.len() as u32).await?; 17..21
    // connection.write_all(block).await?;

    let mut length_bytes = [0u8; 4];
    length_bytes.copy_from_slice(&src[17..21]);
    let length = u32::from_be_bytes(src[17..21].try_into()?) as usize;

    if length > MAX {
        return Err(Box::new(IronCarrierError::ParseNetworkMessage));
    }

    if src.len() < STREAM_HEADER + length {
        src.reserve(STREAM_HEADER + length - src.len());
        return Ok(None);
    }

    let transfer_id = u64::from_be_bytes(src[1..9].try_into()?);
    let block_index = u64::from_be_bytes(src[9..17].try_into()?);
    let block = std::sync::Arc::new(src[21..21 + length].to_vec());
    src.advance(STREAM_HEADER + length);

    Ok(Some(NetworkEvents::FileTransfer(
        transfer_id,
        crate::file_transfer::FileTransfer::TransferBlock { block_index, block },
    )))
}
fn decode_event(src: &mut BytesMut) -> Result<Option<NetworkEvents>, Box<dyn Error + Send + Sync>> {
    if src.len() < 5 {
        return Ok(None);
    }

    let mut length_bytes = [0u8; 4];
    length_bytes.copy_from_slice(&src[1..5]);
    let length = u32::from_be_bytes(length_bytes) as usize;

    if length > MAX {
        return Err(Box::new(IronCarrierError::ParseNetworkMessage));
    }

    if src.len() < 5 + length {
        src.reserve(5 + length - src.len());
        return Ok(None);
    }

    let data = src[5..5 + length].to_vec();
    src.advance(5 + length);

    match bincode::deserialize(&data) {
        Ok(event) => Ok(Some(event)),
        Err(_) => Err(Box::new(IronCarrierError::ParseNetworkMessage)),
    }
}
