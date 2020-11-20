mod frame;
mod direct_stream;

use std::sync::Arc;

pub use frame::{FrameReader, FrameWriter, FrameMessage, frame_stream };
pub use direct_stream::{DirectStream};
use tokio::{sync::Mutex, io::{AsyncRead, AsyncWrite}};


pub fn get_streamers<T: AsyncRead + AsyncWrite + Unpin>(stream: T) -> (DirectStream<T>, FrameReader<T>, FrameWriter<T>) {
    let stream = Arc::new(Mutex::new(stream));
    ( DirectStream::new(stream.clone()), FrameReader::new(stream.clone()), FrameWriter::new(stream ))
}