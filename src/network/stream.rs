use serde::{Serialize, Deserialize};
use tokio::{io::AsyncRead, io::AsyncWrite, io::BufReader};
use bytes::{ BytesMut, Buf, };
use tokio::io::{AsyncReadExt, AsyncWriteExt };

use crate::{RSyncError, fs::FileInfo};

const BUFFER_SIZE: usize = 8 * 1024;
const COMMAND_SIZE: usize = 8;


/// Possible commands to exchange between server and peer
#[derive(Serialize,Deserialize, Debug, PartialEq)]
pub(crate) enum Command {
    Ping,
    Pong,
    FetchUnsyncedFileList(Vec<(String, u64)>),
    FileList(Vec<(String, Vec<FileInfo>)>),
    InitFileTransfer(String, FileInfo),
    FileTransferSuccess,
    FileTransferFailed
}

/// Receive or send parsed Command using a TcpStream
pub(crate) struct CommandStream<T: AsyncRead + AsyncWrite + Unpin> {
    socket_stream: BufReader<T>,
    buffer: BytesMut
}

impl <T: AsyncRead + AsyncWrite + Unpin> CommandStream<T> {
    /// Create a CommandStream and allocate an internal buffer
    pub fn new(socket_stream: T) -> Self {
        CommandStream {
            socket_stream: BufReader::new(socket_stream),
            buffer: BytesMut::with_capacity(BUFFER_SIZE)
        }
    }

    /// Return Some(Command) parsed from internal buffer
    /// Return None if there is not enough data in buffer
    fn parse_command(& mut self) -> Result<Option<Command>, Box<dyn std::error::Error>> {
        if self.buffer.remaining() < COMMAND_SIZE {
            return Ok(None);
        }

        let bytes = &self.buffer.as_ref()[..COMMAND_SIZE];
        let to_read: usize = bincode::deserialize(bytes)?;
       
        if self.buffer.remaining() < to_read as usize + COMMAND_SIZE {
            return Ok(None);
        }

        let bytes = &self.buffer.as_ref()[COMMAND_SIZE..COMMAND_SIZE + to_read];
        let result: Command = bincode::deserialize(bytes)?;
        
        self.buffer.advance(to_read + COMMAND_SIZE);

        return Ok(Some(result));

    }

    /// Retrive next Command from internal stream
    pub async fn next_command(&mut self) -> Result<Option<Command>, RSyncError> {
        
        loop {
            if let Some(cmd) = self.parse_command().map_err(|_| RSyncError::ErrorParsingCommands)? {
                return Ok(Some(cmd));
            }

            let mut buf = [0u8; BUFFER_SIZE];
            let read = self.socket_stream.read(&mut buf).await.map_err(|_| RSyncError::ErrorParsingCommands)?;
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

    /// Read `buf_size` bytes from internal buffer and write into `buf_write`
    pub async fn write_to_buffer<R: AsyncWrite + Unpin>(&mut self, buf_size: usize, buf_write: &mut R) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf_size = buf_size;
        
        let buffer_rem = self.buffer.remaining();
        if buffer_rem > buf_size {
            let bytes = &self.buffer.as_ref()[..buf_size];
            buf_write.write(bytes).await?;
            return Ok(());
        }

        if buffer_rem > 0 {
            let bytes = &self.buffer.as_ref()[..buffer_rem];
            buf_write.write(bytes).await?;
            buf_size -= buffer_rem;
            self.buffer.advance(buffer_rem);
        }


        let mut buf = [0u8; BUFFER_SIZE];
        while buf_size > 0 {

            let read = self.socket_stream.read(&mut buf).await?;
            if read <= buf_size {
                buf_write.write(&buf[..read]).await?;
                buf_size -= read;
            } else  {
                buf_write.write(&buf[..buf_size]).await?;
                self.buffer.extend(&buf[buf_size..]);
                buf_size = 0;
            }
        }

        Ok(())
    }

    /// read `buf_size` from `buf_read` and write into internal stream
    pub async fn send_data_from_buffer<R: AsyncRead + Unpin>(&mut self, buf_size: u64, buf_read: &mut R) -> Result<(), Box<dyn std::error::Error>> {
        if buf_size != tokio::io::copy( buf_read, &mut self.socket_stream).await? {
            Err(Box::new(RSyncError::ErrorSendingFile))
        } else {
            Ok(())
        }
    }
    
    /// parse `cmd` and send it to internal stream
    pub async fn send_command(&mut self, cmd: &Command) -> Result<(), Box<dyn std::error::Error>> {
        let mut ser = bincode::serialize(&bincode::serialized_size(cmd)?)?;
        ser.extend(bincode::serialize(cmd)?);

       
        if 0 == self.socket_stream.write(&ser[..]).await? {
            return Err(Box::new(std::io::Error::from(std::io::ErrorKind::WriteZero)));
        }

        // self.socket_stream.flush().await?;

        Ok(())
    }   


}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn can_parse_commands() -> Result<(), Box<dyn std::error::Error>> {
        let (server_stream, client_stream) = tokio::io::duplex(BUFFER_SIZE);
        let mut server_cmd_stream = CommandStream::new(server_stream);
        let mut client_cmd_stream = CommandStream::new(client_stream);

        client_cmd_stream.send_command(&Command::Ping).await?;
        client_cmd_stream.send_command(&Command::Ping).await?;
        client_cmd_stream.send_command(&Command::Pong).await?;
        client_cmd_stream.send_command(&Command::Ping).await?;
            
        assert_eq!(server_cmd_stream.next_command().await.unwrap(), Some(Command::Ping));
        assert_eq!(server_cmd_stream.next_command().await.unwrap(), Some(Command::Ping));
        assert_eq!(server_cmd_stream.next_command().await.unwrap(), Some(Command::Pong));
        assert_eq!(server_cmd_stream.next_command().await.unwrap(), Some(Command::Ping));

        client_cmd_stream.socket_stream.shutdown().await?;

        assert_eq!(server_cmd_stream.next_command().await.unwrap(), None);
        

        return Ok(());
    }

    #[tokio::test]
    async fn command_stream_can_write_into_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let (server_stream, client_stream) = tokio::io::duplex(BUFFER_SIZE);
        let mut server_cmd_stream = CommandStream::new(server_stream);
        let mut client_cmd_stream = CommandStream::new(client_stream);

        let mut buffer: &[u8] = &[0u8; BUFFER_SIZE * 8];
        tokio::spawn(async move {
            assert!(client_cmd_stream.send_data_from_buffer(BUFFER_SIZE as u64 * 8, &mut buffer).await.is_ok());
        });
        
        let mut sink = tokio::io::sink();
        assert!( server_cmd_stream.write_to_buffer(BUFFER_SIZE * 8, &mut sink).await.is_ok() );
        
        return Ok(());
    }
}