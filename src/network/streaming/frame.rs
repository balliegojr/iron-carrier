use bytes::{Buf, BytesMut};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::AsyncRead,
    io::AsyncReadExt,
    io::AsyncWrite,
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
};

use crate::IronCarrierError;

const BUFFER_SIZE: usize = 8 * 1024;
const COMMAND_SIZE: usize = 8;

/// A Message to be serialized or deserialized for the rpc call
///
/// It uses [bincode] to serialize arguments
/// #Examples
///
/// ``` ignore
/// let mut message = FrameMessage::new("message_name".to_string());
/// message.append_arg(1);
/// message.append_arg("a");
/// ```
#[derive(Serialize, Deserialize, Debug)]
pub struct FrameMessage {
    ident: String,
    data: Vec<u8>,
}

impl FrameMessage {
    /// Creates a new [FrameMessage] without any args
    pub fn new(ident: &str) -> Self {
        Self {
            ident: ident.to_owned(),
            data: Vec::new(),
        }
    }

    /// Returns the name of this frame
    pub fn frame_ident(&self) -> &str {
        &self.ident
    }

    /// Add an argument of type `T` to this frame
    ///
    /// Returns [Ok] if successful
    pub fn with_arg<A: Serialize>(mut self, arg: &A) -> crate::Result<Self> {
        let ser_value = bincode::serialize(arg)?;
        let ser_size = bincode::serialize(&ser_value.len())?;

        self.data.extend(ser_size);
        self.data.extend(ser_value);

        Ok(self)
    }

    /// Return the next argument in this frame
    ///
    /// This function may fail if the argument can't be deserialized to [`T`]
    /// or if there isn't an argument to be retrieved
    ///
    /// [`OK`]`(`[T]`)` if the argument is correct
    pub fn next_arg<A: DeserializeOwned>(&mut self) -> crate::Result<A> {
        let size = std::mem::size_of::<usize>();

        if self.data.len() < size {
            return Err(IronCarrierError::ParseCommandError.into());
        }

        let bytes: Vec<u8> = self.data.drain(0..size).collect();

        let size: usize = bincode::deserialize(&bytes)?;

        if self.data.len() < size {
            return Err(IronCarrierError::ParseCommandError.into());
        }

        let bytes: Vec<u8> = self.data.drain(0..size).collect();
        let result = bincode::deserialize::<A>(&bytes)?;

        Ok(result)
    }
}

impl From<&str> for FrameMessage {
    /// Creates a new [`FrameMessage`] from a primitive [`str`]
    fn from(name: &str) -> Self {
        FrameMessage::new(name)
    }
}

/// Creates and return a pair of [FrameReader]  and [FrameWriter] using stream
/// It consumes stream in the process
pub fn frame_stream<T: AsyncRead + AsyncWrite + Unpin>(
    stream: T,
) -> (FrameReader<ReadHalf<T>>, FrameWriter<WriteHalf<T>>) {
    let (rx, tx) = tokio::io::split(stream);
    (FrameReader::new(rx), FrameWriter::new(tx))
}

/// Read only [FrameMessage] processor
///
/// it uses an internal buffer to store information read from the stream
pub struct FrameReader<T: AsyncRead + Unpin> {
    socket_stream: T,
    buffer: BytesMut,
}

/// Write only [FrameMessage] processor
pub struct FrameWriter<T: AsyncWrite + Unpin> {
    socket_stream: T,
}

impl<T: AsyncRead + Unpin> FrameReader<T> {
    /// Constructs a new [FrameReader] using the socket_stream
    /// Pre allocates a buffer to store information
    pub fn new(socket_stream: T) -> Self {
        Self {
            socket_stream,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    /// Consumes information from the internal buffer to parse a frame  
    /// this function may fail information stored in the buffer is bad
    ///
    /// Returns [Ok]`(`[Some]`(`[FrameMessage]`)` `)` if success  
    /// Returns [Ok]`(`[None]`)` if there is not enough information to parse a frame
    fn parse_frame(&mut self) -> crate::Result<Option<FrameMessage>> {
        if self.buffer.remaining() < COMMAND_SIZE {
            return Ok(None);
        }

        let bytes = &self.buffer.as_ref()[..COMMAND_SIZE];
        let to_read: usize = bincode::deserialize(bytes)?;

        if self.buffer.remaining() < to_read as usize + COMMAND_SIZE {
            return Ok(None);
        }

        let bytes = &self.buffer.as_ref()[COMMAND_SIZE..COMMAND_SIZE + to_read];
        let result: FrameMessage = bincode::deserialize(bytes)?;

        self.buffer.advance(to_read + COMMAND_SIZE);

        return Ok(Some(result));
    }

    /// Read and parse the next [FrameMessage]  
    /// This function tries to process the information in the internal buffer first  
    /// Then it tries to read more information from the stream
    ///
    /// Returns [Ok]`(`[Some]`(`[FrameMessage]`) `)` if the parsing is successful  
    ///
    /// Returns [Ok]`(`[None]`) if there is no information in the buffer or the stream  
    ///
    /// Returns [Err] if there isn't enought information for a full [FrameMessage] to be parsed  
    pub async fn next_frame(&mut self) -> crate::Result<Option<FrameMessage>> {
        loop {
            if let Some(frame) = self
                .parse_frame()
                .map_err(|_| IronCarrierError::ParseCommandError)?
            {
                return Ok(Some(frame));
            }

            let mut buf = [0u8; BUFFER_SIZE];
            let read = self
                .socket_stream
                .read(&mut buf)
                .await
                .map_err(|_| IronCarrierError::NetworkIOReadingError)?;
            if 0 == read {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(IronCarrierError::NetworkIOReadingError.into());
                }
            } else {
                self.buffer.extend(&buf[..read]);
            }
        }
    }
}

impl<T: AsyncWrite + Unpin> FrameWriter<T> {
    /// Constructs a new [FrameWriter]
    pub fn new(socket_stream: T) -> Self {
        Self { socket_stream }
    }

    /// Writes a [FrameMessage] to stream  
    ///
    /// It may fail if stream can't be written
    pub async fn write_frame(&mut self, frame: FrameMessage) -> crate::Result<()> {
        let ser_value = bincode::serialize(&frame)?;
        let ser_size = bincode::serialize(&ser_value.len())?;

        self.socket_stream
            .write_all(&ser_size[..])
            .await
            .map_err(|_| IronCarrierError::NetworkIOWritingError)?;
        self.socket_stream
            .write_all(&ser_value[..])
            .await
            .map_err(|_| IronCarrierError::NetworkIOWritingError)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn frame_can_be_parsed() -> crate::Result<()> {
        let (server_stream, client_stream) = tokio::io::duplex(BUFFER_SIZE);
        let (mut server_reader, _) = frame_stream(server_stream);

        let handle = tokio::spawn(async move {
            let (_, mut client_writer) = frame_stream(client_stream);

            client_writer
                .write_frame("some message".into())
                .await
                .unwrap();
            let message_with_data = FrameMessage::new("message_data")
                .with_arg(&1)
                .unwrap()
                .with_arg(&"other_info")
                .unwrap();

            client_writer.write_frame(message_with_data).await.unwrap();
            client_writer
                .write_frame("other message".into())
                .await
                .unwrap();
        });

        assert_eq!(
            server_reader.next_frame().await?.unwrap().ident,
            "some message".to_string()
        );
        assert_eq!(
            server_reader.next_frame().await?.unwrap().ident,
            "message_data".to_string()
        );
        assert_eq!(
            server_reader.next_frame().await?.unwrap().ident,
            "other message".to_string()
        );

        handle.await.unwrap();

        assert!(server_reader.next_frame().await.unwrap().is_none());

        return Ok(());
    }
}
