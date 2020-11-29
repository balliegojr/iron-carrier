use std::sync::Arc;

use tokio::{prelude::*, io::AsyncRead, io::AsyncReadExt, io::AsyncWrite, sync::Mutex};
use crate::IronCarrierError;

const BUFFER_SIZE: usize = 8 * 1024;

#[derive(Debug)]
pub struct DirectStream<T : AsyncRead + AsyncWrite + Unpin> {
    socket_stream: Arc<Mutex<T>>,
}

impl <T : AsyncRead + AsyncWrite + Unpin> DirectStream<T> {
    pub fn new(socket_stream: Arc<Mutex<T>>) -> Self {
        DirectStream {
            socket_stream,
        }
    }

    /// Read `buf_size` bytes from internal buffer and write into `buf_write`
    pub async fn read_stream<R: AsyncWrite + Unpin>(&mut self, buf_size: usize, buf_write: &mut R) -> crate::Result<()> {
        let mut buf_size = buf_size;

        
        let mut buf = [0u8; BUFFER_SIZE];
        let mut stream = self.socket_stream.lock().await;
        while buf_size > 0 {
            let size = std::cmp::min(BUFFER_SIZE, buf_size);
            stream.read_exact(&mut buf[..size]).await.map_err(|_| IronCarrierError::NetworkIOReadingError)?;
            buf_write.write(&buf[..size]).await.map_err(|_| IronCarrierError::NetworkIOReadingError)?;
            buf_size -= size;
        }

        buf_write.flush().await.map_err(|_| IronCarrierError::NetworkIOReadingError)?;

        Ok(())
    }

    /// read `buf_size` from `buf_read` and write into internal stream
    pub async fn write_to_stream<R: AsyncRead + Unpin>(&mut self, buf_size: u64, buf_read: &mut R) -> crate::Result<()> {
        // TODO: fix implementation to actually use buf_size
        let mut stream = self.socket_stream.lock().await;
        if buf_size != tokio::io::copy( buf_read, &mut ( *stream)).await.map_err(|_| IronCarrierError::NetworkIOWritingError)? {
            Err(IronCarrierError::NetworkIOWritingError)
        } else {
            Ok(())
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_stream_can_write_into_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let (server_stream, client_stream) = tokio::io::duplex(BUFFER_SIZE);
        let mut server_cmd_stream = DirectStream::new(Arc::new(Mutex::new(server_stream)));
        let mut client_cmd_stream = DirectStream::new(Arc::new(Mutex::new(client_stream)));

        let mut buffer: &[u8] = &[0u8; 50000];
        tokio::spawn(async move {
            assert!(client_cmd_stream.write_to_stream(50000, &mut buffer).await.is_ok());
        });
        
        let mut sink = tokio::io::sink();
        assert!( server_cmd_stream.read_stream(50000, &mut sink).await.is_ok() );
        
        return Ok(());
    }
}