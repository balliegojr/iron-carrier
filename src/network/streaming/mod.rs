mod frame;
mod file_streamer;

use std::sync::Arc;

pub(crate) use frame::{FrameReader, FrameWriter, FrameMessage, frame_stream };
pub(crate) use file_streamer::{Receiver as FileReceiver, Sender as FileSender, file_streamers};
use tokio::{sync::Mutex, io::{AsyncRead, AsyncWrite}};


pub fn get_streamers<T: AsyncRead + AsyncWrite + Unpin>(stream: T) -> (FrameReader<T>, FrameWriter<T>) {
    let stream = Arc::new(Mutex::new(stream));
    ( FrameReader::new(stream.clone()), FrameWriter::new(stream ))
}