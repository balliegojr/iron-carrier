use bytes::{Buf, BufMut, BytesMut};
use chacha20poly1305::{
    aead::{
        stream::{DecryptorLE31, EncryptorLE31},
        KeyInit,
    },
    XChaCha20Poly1305,
};
use pbkdf2::pbkdf2_hmac_array;
use pin_project_lite::pin_project;
use sha2::Sha256;
use std::{
    pin::Pin,
    task::{ready, Poll},
};

use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncWrite},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

pub fn crypto_stream(
    inner: TcpStream,
    plain_key: &str,
    group_name: &str,
) -> (
    CryptoStreamReadHalf<OwnedReadHalf>,
    CryptoStreamWriteHalf<OwnedWriteHalf>,
) {
    let (read, write) = inner.into_split();

    let key = get_key(plain_key, group_name);
    let start_nonce = [0u8; 20];

    let encryptor: EncryptorLE31<XChaCha20Poly1305> =
        chacha20poly1305::aead::stream::EncryptorLE31::from_aead(
            XChaCha20Poly1305::new(key.as_ref().into()),
            start_nonce.as_ref().into(),
        );

    let decryptor: DecryptorLE31<XChaCha20Poly1305> =
        chacha20poly1305::aead::stream::DecryptorLE31::from_aead(
            XChaCha20Poly1305::new(key.as_ref().into()),
            start_nonce.as_ref().into(),
        );

    (
        CryptoStreamReadHalf::new(read, decryptor),
        CryptoStreamWriteHalf::new(write, encryptor),
    )
}

pin_project_lite::pin_project! {
    pub struct CryptoStreamWriteHalf<T> {
        #[pin]
        inner: T,
        encryptor: EncryptorLE31<XChaCha20Poly1305>,
        buffer: bytes::BytesMut
    }
}

impl<T> CryptoStreamWriteHalf<T>
where
    T: AsyncWrite,
{
    pub fn new(inner: T, encryptor: EncryptorLE31<XChaCha20Poly1305>) -> Self {
        Self {
            inner,
            encryptor,
            buffer: BytesMut::with_capacity(4096),
        }
    }
    fn get_encrypted(&mut self, buf: &[u8]) -> Option<Vec<u8>> {
        match self.encryptor.encrypt_next(buf) {
            Ok(mut encrypted) => {
                let len = (encrypted.len() as u32).to_le_bytes();
                let mut buf = Vec::with_capacity(encrypted.len() + std::mem::size_of::<u32>());
                buf.extend_from_slice(&len);
                buf.append(&mut encrypted);

                Some(buf)
            }
            Err(_) => None,
        }
    }

    fn flush_buf(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut me = self.project();
        while me.buffer.has_remaining() {
            match ready!(me.inner.as_mut().poll_write(cx, &me.buffer[..])) {
                Ok(0) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    )));
                }
                Ok(n) => me.buffer.advance(n),
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<T> AsyncWrite for CryptoStreamWriteHalf<T>
where
    T: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        if !self.buffer.is_empty() {
            ready!(self.as_mut().flush_buf(cx))?
        }

        for chunk in buf.chunks(1024) {
            let Some(encrypted) = self.get_encrypted(chunk) else {
                log::error!("Failed to encrypt content");
                return std::task::Poll::Ready(Err(std::io::ErrorKind::InvalidInput.into()));
            };

            let me = self.as_mut().project();
            match me.inner.poll_write(cx, &encrypted[..]) {
                Poll::Ready(written) => {
                    let written = written?;
                    if written < encrypted.len() {
                        self.buffer.put(&encrypted[written..]);
                    }
                }
                Poll::Pending => {
                    self.buffer.put(&encrypted[..]);
                }
            }
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

pin_project! {
    pub struct CryptoStreamReadHalf<T> {
        #[pin]
        inner: T,
        decryptor: DecryptorLE31<XChaCha20Poly1305>,
        buffer: Vec<u8>,
        pos: usize,
        cap: usize
    }
}

impl<T> CryptoStreamReadHalf<T> {
    pub fn new(inner: T, decryptor: DecryptorLE31<XChaCha20Poly1305>) -> Self {
        Self {
            inner,
            decryptor,
            buffer: vec![0u8; 4096],
            pos: 0,
            cap: 0,
        }
    }
    /// Produce a value if there is enough data in the internal buffer
    fn produce(mut self: Pin<&mut Self>) -> Option<Vec<u8>> {
        if self.cap <= self.pos {
            return None;
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&self.buffer[self.pos..self.pos + 4]);
        let length = u32::from_le_bytes(length_bytes) as usize;

        let me = self.as_mut().project();
        if *me.cap >= *me.pos + length + 4 {
            let decrypted = me
                .decryptor
                .decrypt_next(&me.buffer[*me.pos + 4..*me.pos + 4 + length])
                .ok();

            if decrypted.is_some() {
                *me.pos += 4 + length;
            }

            if *me.pos == *me.cap {
                *me.pos = 0;
                *me.cap = 0;
            }

            decrypted
        } else {
            self.adjust_buffer(length + 4);
            None
        }
    }

    fn adjust_buffer(self: Pin<&mut Self>, desired_additional: usize) {
        let me = self.project();
        if *me.cap + desired_additional >= me.buffer.len() && *me.pos > 0 {
            me.buffer.copy_within(*me.pos..*me.cap, 0);
            *me.cap -= *me.pos;
            *me.pos = 0;
        }

        if *me.pos + desired_additional > me.buffer.len() {
            me.buffer.resize(me.buffer.len() * 2, 0);
        }
    }
}

impl<T> AsyncRead for CryptoStreamReadHalf<T>
where
    T: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // First we try to produce any value available from a previous read
        if let Some(decrypted) = self.as_mut().produce() {
            buf.put_slice(&decrypted);
            return std::task::Poll::Ready(Ok(()));
        }

        // Keep pooling the inner reader and trying to produce a value as long as we read something
        // from inner
        loop {
            let empty_read = ready!(self.as_mut().poll_fill_buf(cx))?.is_empty();
            if empty_read {
                return std::task::Poll::Ready(Ok(()));
            }

            if let Some(decrypted) = self.as_mut().produce() {
                buf.put_slice(&decrypted);
                return std::task::Poll::Ready(Ok(()));
            }
        }
    }
}

impl<R: AsyncRead> tokio::io::AsyncBufRead for CryptoStreamReadHalf<R> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        let me = self.project();

        let mut buf = tokio::io::ReadBuf::new(&mut me.buffer[*me.cap..]);
        ready!(me.inner.poll_read(cx, &mut buf))?;
        if !buf.filled().is_empty() {
            *me.cap += buf.filled().len();
        }

        std::task::Poll::Ready(Ok(&me.buffer[*me.pos..*me.cap]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let me = self.project();
        *me.pos += amt;
        if *me.pos >= *me.cap {
            *me.pos = 0;
            *me.cap = 0;
        }
    }
}

fn get_key(plain_key: &str, salt: &str) -> [u8; 32] {
    const ITERATIONS: u32 = 4096;
    pbkdf2_hmac_array::<Sha256, 32>(plain_key.as_bytes(), salt.as_bytes(), ITERATIONS)
}

#[cfg(test)]
mod tests {
    use std::{assert_eq, time::Duration};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    pub async fn test_crypto_stream_write_half() {
        let key = get_key("key", "group");
        let start_nonce = [0u8; 20];

        let mut encryptor: EncryptorLE31<XChaCha20Poly1305> =
            chacha20poly1305::aead::stream::EncryptorLE31::from_aead(
                XChaCha20Poly1305::new(key.as_ref().into()),
                start_nonce.as_ref().into(),
            );

        let expected = {
            let mut encrypted = encryptor.encrypt_next("some content".as_bytes()).unwrap();
            let mut expected = Vec::new();
            expected.extend((encrypted.len() as u32).to_le_bytes());
            expected.append(&mut encrypted);

            expected
        };

        let mut writer = CryptoStreamWriteHalf::new(
            tokio::io::BufWriter::new(Vec::new()),
            chacha20poly1305::aead::stream::EncryptorLE31::from_aead(
                XChaCha20Poly1305::new(key.as_ref().into()),
                start_nonce.as_ref().into(),
            ),
        );

        assert_eq!(
            writer.write(b"some content").await.unwrap(),
            "some content".bytes().len()
        );

        assert_eq!(expected, writer.inner.buffer())
    }

    #[tokio::test]
    pub async fn test_crypto_stream_read_half() {
        let key = get_key("key", "group");
        let start_nonce = [0u8; 20];

        let (rx, mut tx) = tokio::io::duplex(100);

        tokio::spawn(async move {
            let encrypted_content = {
                let mut encryptor: EncryptorLE31<XChaCha20Poly1305> =
                    chacha20poly1305::aead::stream::EncryptorLE31::from_aead(
                        XChaCha20Poly1305::new(key.as_ref().into()),
                        start_nonce.as_ref().into(),
                    );

                let mut expected = Vec::new();

                for data in ["some content", "some other content", "even more content"] {
                    let mut encrypted = encryptor.encrypt_next(data.as_bytes()).unwrap();
                    expected.extend((encrypted.len() as u32).to_le_bytes());
                    expected.append(&mut encrypted);
                }

                expected
            };

            for chunk in encrypted_content.chunks(10) {
                let _ = tx.write(chunk).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let decryptor = chacha20poly1305::aead::stream::DecryptorLE31::from_aead(
            XChaCha20Poly1305::new(key.as_ref().into()),
            start_nonce.as_ref().into(),
        );
        let mut reader = CryptoStreamReadHalf::new(rx, decryptor);

        let mut plain_content = String::new();
        let _ = reader.read_to_string(&mut plain_content).await;

        assert_eq!(
            plain_content,
            "some contentsome other contenteven more content"
        );
    }

    #[tokio::test]
    pub async fn test_big_transfer() {
        let key = get_key("key", "group");
        let start_nonce = [0u8; 20];

        let (rx, tx) = tokio::io::duplex(4096);

        let decryptor = chacha20poly1305::aead::stream::DecryptorLE31::from_aead(
            XChaCha20Poly1305::new(key.as_ref().into()),
            start_nonce.as_ref().into(),
        );
        let encryptor: EncryptorLE31<XChaCha20Poly1305> =
            chacha20poly1305::aead::stream::EncryptorLE31::from_aead(
                XChaCha20Poly1305::new(key.as_ref().into()),
                start_nonce.as_ref().into(),
            );

        let mut writer = CryptoStreamWriteHalf::new(tx, encryptor);
        let mut reader = CryptoStreamReadHalf::new(rx, decryptor);

        let size = 1024 * 4;
        tokio::spawn(async move {
            let content = vec![100u8; size];
            let _ = writer.write(&content).await;
            let _ = writer.flush().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut bytes_expected = size;
        let mut read_buf = vec![0u8; 1024];
        while let Ok(bytes_read) = reader.read(&mut read_buf[..]).await {
            if bytes_read == 0 {
                break;
            }

            assert!(read_buf[..bytes_read].iter().all(|b| *b == 100));
            bytes_expected -= bytes_read;
        }

        assert_eq!(0, bytes_expected);
    }
}
