use std::collections::HashMap;

use crate::{
    config::Config,
    fs::{self, FileInfo},
    sync::file_events_buffer::FileEventsBuffer,
};
use tokio::{
    io::AsyncRead,
    io::AsyncReadExt,
    io::AsyncWrite,
    io::AsyncWriteExt,
    io::{ReadHalf, WriteHalf},
};

const BUFFER_SIZE: usize = 8 * 1024;

pub struct Sender<T: AsyncWrite + Unpin> {
    stream: T,
}

impl<T: AsyncWrite + Unpin> Sender<T> {
    pub fn new(stream: T) -> Self {
        Self { stream }
    }
    /// read `buf_size` from `buf_read` and write into internal stream
    pub async fn send_file<R: AsyncRead + Unpin>(
        &mut self,
        ident: u64,
        buf_read: &mut R,
    ) -> crate::Result<()> {
        let buff = bincode::serialize(&ident)?;

        self.stream.write_all(&buff).await?;
        tokio::io::copy(buf_read, &mut self.stream).await?;

        Ok(())
    }
}

pub(crate) struct Receiver<'a, T: AsyncRead + Unpin> {
    stream: T,
    ident: u64,
    files: HashMap<u64, FileInfo>,
    config: &'a Config,
    peer_address: String,
}

impl<'a, T: AsyncRead + Unpin> Receiver<'a, T> {
    pub fn new(stream: T, config: &'a Config, peer_address: String) -> Self {
        Receiver {
            stream,
            ident: 0,
            files: HashMap::new(),
            config,
            peer_address,
        }
    }

    async fn read_file<'b>(
        &mut self,
        file_info: FileInfo,
        events_buffer: &'b FileEventsBuffer,
    ) -> crate::Result<()> {
        let mut buf = [0u8; BUFFER_SIZE];
        let mut buf_size = file_info.size.unwrap() as usize;

        let mut buf_write = fs::get_temp_file(&file_info, &self.config).await?;
        while buf_size > 0 {
            let size = std::cmp::min(BUFFER_SIZE, buf_size);
            self.stream.read_exact(&mut buf[..size]).await?;
            buf_write.write(&buf[..size]).await?;
            buf_size -= size;
        }

        buf_write.flush().await?;

        events_buffer.add_event(&file_info, &self.peer_address);
        fs::flush_temp_file(&file_info, &self.config).await?;

        Ok(())
    }

    pub async fn wait_files<'b>(
        &mut self,
        events_buffer: &'b FileEventsBuffer,
    ) -> crate::Result<()> {
        while self.files.len() > 0 {
            let mut handle_buf = [0u8; 8];
            self.stream.read_exact(&mut handle_buf[..]).await?;

            let file_handle: u64 = bincode::deserialize(&handle_buf)?;
            // TODO: handle error
            match self.files.remove(&file_handle) {
                Some(file_info) => {
                    self.read_file(file_info, events_buffer).await?;
                }
                None => {
                    log::error!("file handle {} don't exist", &file_handle)
                }
            }
        }

        Ok(())
    }

    pub fn prepare_file_transfer(&mut self, file: FileInfo) -> u64 {
        self.ident += 1;
        self.files.insert(self.ident, file);

        self.ident
    }
}

pub(crate) fn file_streamers<'a, T>(
    stream: T,
    config: &'a Config,
    peer_address: String,
) -> (Receiver<'a, ReadHalf<T>>, Sender<WriteHalf<T>>)
where
    T: AsyncRead + AsyncWrite,
{
    let (rx, tx) = tokio::io::split(stream);
    (Receiver::new(rx, config, peer_address), Sender::new(tx))
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };

    use super::*;

    fn sample_config(test_folder: &str) -> Config {
        Config::parse_content(format!(
            "port = 8090
       
        [paths]
        a = \"./tmp/{}\"",
            test_folder
        ))
        .unwrap()
    }

    fn create_tmp_file(path: PathBuf, contents: &str) {
        if !path.parent().unwrap().exists() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        }

        std::fs::write(path, contents).unwrap();
    }

    #[tokio::test]
    async fn file_streamer() -> Result<(), Box<dyn std::error::Error>> {
        let (rx_stream, tx_stream) = tokio::io::duplex(BUFFER_SIZE);

        let config = Arc::new(sample_config("file_streamer"));

        let mut tx = Sender { stream: tx_stream };
        let mut rx = Receiver {
            ident: 0,
            files: HashMap::new(),
            stream: rx_stream,
            config: &config,
            peer_address: "".into(),
        };

        create_tmp_file("./tmp/file_streamer/file_1".into(), "some content");
        let mut file = FileInfo::new(
            "a".into(),
            "file_1".into(),
            Path::new("./tmp/file_streamer/file_1").metadata().unwrap(),
        );
        let mut buffer: &[u8] = b"some file content";

        file.size = Some(buffer.len() as u64);

        let file_handle = rx.prepare_file_transfer(file);
        tokio::spawn(async move {
            tx.send_file(file_handle, &mut buffer).await.unwrap();
        });

        let events_buffer = FileEventsBuffer::new(config.clone());
        rx.wait_files(&events_buffer).await.unwrap();

        assert_eq!(
            tokio::fs::read_to_string("./tmp/file_streamer/file_1")
                .await
                .unwrap(),
            "some file content"
        );

        return Ok(());
    }
}
