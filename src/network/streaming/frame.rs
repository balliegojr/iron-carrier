use std::sync::Arc;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{io::AsyncRead, io::AsyncWrite, sync::Mutex, io::AsyncReadExt, io::AsyncWriteExt};
use bytes::{ BytesMut, Buf, };

use crate::RSyncError;

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
    name: String,
    data: Vec<u8>
}

impl FrameMessage {
    /// Creates a new [FrameMessage] without any args
    pub fn new(name: String) -> Self {
        Self {
            name,
            data: Vec::new()
        }
    }

    /// Returns the name of this frame
    pub fn frame_name(&self) -> &str {
        &self.name
    }

    /// Add an argument of type `T` to this frame
    /// 
    /// Returns [Ok] if successful
    pub fn append_arg<T : Serialize>(&mut self, arg: &T) -> Result<(), RSyncError> {
        let ser_value = bincode::serialize(arg)?;
        let ser_size = bincode::serialize(&ser_value.len())?;

        self.data.extend(ser_size);
        self.data.extend(ser_value);

        Ok(())
    }

    /// Return the next argument in this frame
    /// 
    /// This function may fail if the argument can't be deserialized to [`T`]
    /// or if there isn't an argument to be retrieved
    /// 
    /// [`OK`]`(`[T]`)` if the argument is correct
    pub fn next_arg<T : DeserializeOwned>(&mut self) -> Result<T, RSyncError> {
        let size = std::mem::size_of::<usize>();

        if self.data.len() < size { return Err(RSyncError::ErrorParsingCommands); }

        let bytes: Vec<u8> = self.data.drain(0..size).collect();
        
        let size: usize = bincode::deserialize(&bytes)?;
        
        if self.data.len() < size { return Err(RSyncError::ErrorParsingCommands); }

        let bytes: Vec<u8> = self.data.drain(0..size).collect();
        let result = bincode::deserialize::<T>(&bytes)?;

        Ok(result)
    }
}

impl From<&str> for FrameMessage {
    /// Creates a new [`FrameMessage`] from a primitive [`str`]
    fn from(name: &str) -> Self {
        FrameMessage::new(name.to_owned())
    }
}

/// Creates and return a pair of [FrameReader]  and [FrameWriter] using stream
/// It consumes stream in the process
pub fn frame_stream<T: AsyncRead + AsyncWrite + Unpin>(stream: T) -> (FrameReader<T>, FrameWriter<T>) {
    let stream = Arc::new(Mutex::new(stream));
    ( FrameReader::new(stream.clone()), FrameWriter::new(stream ))
}

/// Read only [FrameMessage] processor
///
/// it uses an internal buffer to store information read from the stream
pub struct  FrameReader<T: AsyncRead + Unpin> {
    socket_stream: Arc<Mutex<T>>,
    buffer: BytesMut
}

/// Write only [FrameMessage] processor
pub struct FrameWriter<T: AsyncWrite + Unpin> {
    socket_stream: Arc<Mutex<T>>,
}

impl <T: AsyncRead + Unpin> FrameReader<T> {
    /// Constructs a new [FrameReader] using the socket_stream
    /// Pre allocates a buffer to store information
    pub fn new(socket_stream: Arc<Mutex<T>>) -> Self {
        Self {
            socket_stream,
            buffer: BytesMut::with_capacity(BUFFER_SIZE)
        }
    }

    /// Consumes information from the internal buffer to parse a frame  
    /// this function may fail information stored in the buffer is bad
    /// 
    /// Returns [Ok]`(`[Some]`(`[FrameMessage]`)` `)` if success  
    /// Returns [Ok]`(`[None]`)` if there is not enough information to parse a frame
    fn parse_frame(& mut self) -> Result<Option<FrameMessage>, Box<dyn std::error::Error>> {
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
    pub async fn next_frame(&mut self) -> Result<Option<FrameMessage>, RSyncError> {
        
        loop {
            if let Some(frame) = self.parse_frame().map_err(|_| RSyncError::ErrorParsingCommands)? {
                return Ok(Some(frame));
            }

            let mut buf = [0u8; BUFFER_SIZE];
            let mut stream = self.socket_stream.lock().await;
            let read = stream.read(&mut buf).await.map_err(|_| RSyncError::ErrorParsingCommands)?;
            if 0 == read {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(RSyncError::ErrorParsingCommands);
                }
            } else {
                self.buffer.extend(&buf[..read]);
            }
        }
    }
}

impl <T: AsyncWrite + Unpin> FrameWriter<T> {
    /// Constructs a new [FrameWriter]
    pub fn new(socket_stream: Arc<Mutex<T>>) -> Self {
        Self {
            socket_stream,
        }
    }

    /// Writes a [FrameMessage] to stream  
    ///
    /// It may fail if stream can't be written
    pub async fn write_frame(&mut self, frame: FrameMessage) -> Result<(), RSyncError> {
        let ser_value = bincode::serialize(&frame)?;
        let ser_size = bincode::serialize(&ser_value.len())?;
        
        let mut stream = self.socket_stream.lock().await;
        
        stream.write_all(&ser_size[..]).await.map_err(|_| RSyncError::ErrorParsingCommands)?;
        stream.write_all(&ser_value[..]).await.map_err(|_| RSyncError::ErrorParsingCommands)?;

        Ok(())
    }   
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn frame_can_be_parsed() -> Result<(), Box<dyn std::error::Error>> {
        let (server_stream, client_stream) = tokio::io::duplex(BUFFER_SIZE);
        let (mut server_reader, _) = frame_stream(server_stream);
        
        let handle = tokio::spawn(async move {
            let (_, mut client_writer) = frame_stream(client_stream);
            
            client_writer.write_frame("some message".into()).await.unwrap();
            let mut message_with_data = FrameMessage::new("message_data".to_string());
            message_with_data.append_arg(&1).unwrap();
            message_with_data.append_arg(&"other_info").unwrap();
            
            client_writer.write_frame(message_with_data).await.unwrap();
            client_writer.write_frame("other message".into()).await.unwrap();
        });

        assert_eq!(server_reader.next_frame().await?.unwrap().name, "some message".to_string());
        assert_eq!(server_reader.next_frame().await?.unwrap().name, "message_data".to_string());
        assert_eq!(server_reader.next_frame().await?.unwrap().name, "other message".to_string());

        handle.await.unwrap();

        assert!(server_reader.next_frame().await.unwrap().is_none());

        return Ok(());
    }
}