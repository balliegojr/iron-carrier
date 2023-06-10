use bytes::{Buf, BufMut, BytesMut};
use chacha20poly1305::{aead::stream::EncryptorLE31, XChaCha20Poly1305};

use std::{
    pin::Pin,
    task::{ready, Poll},
};

use tokio::io::AsyncWrite;

pin_project_lite::pin_project! {
    /// Async Encryption Write Half.
    ///
    /// This struct has an internal buffer to hold encrypted bytes that were not written to the
    /// inner writter. Under "normal" circunstances, the internal buffer will be seldom used.
    pub struct WriteHalf<T> {
        #[pin]
        inner: T,
        encryptor: EncryptorLE31<XChaCha20Poly1305>,
        buffer: bytes::BytesMut
    }
}

impl<T> WriteHalf<T>
where
    T: AsyncWrite,
{
    pub fn new(inner: T, encryptor: EncryptorLE31<XChaCha20Poly1305>) -> Self {
        Self {
            inner,
            encryptor,
            buffer: BytesMut::with_capacity(super::BUFFER_SIZE),
        }
    }

    /// Encrypts `buf` contents and return a [`Vec<u8>`] with 4 bytes in LE representing the encrypted content
    /// length and the encrypted contents.
    ///
    /// [0, 0, 0, 0, ...]
    ///
    /// If the encryption fails, it returns [Option::None]
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

    /// Flush the internal buffer into the inner writer. This functions does nothing if the
    /// internal buffer is empty.   
    ///
    /// If the inner writter writes 0 bytes, this function will return an
    /// [std::io::ErrorKind::WriteZero] error.
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

impl<T> AsyncWrite for WriteHalf<T>
where
    T: AsyncWrite + Unpin,
{
    /// poll_write is called repeatedly with the same buf contents if Poll::Pending is returned.
    ///
    /// We need to avoid encrypting the same content multiple times, because the encryption and
    /// decryption operations must be paired, so we write the encrypted content to the internal
    /// buffer and return how many bytes from the original buf were encrypted.
    ///
    /// The first thing this function do is to flush the internal buffer, this is the only
    /// moment this function can return Poll::Pending.
    ///
    /// Then it will split the buf content into 1024 bytes chunks, we do this to try to avoid a
    /// huge internal buffer in case the content of the message is too big.
    ///
    /// Each chunk is then encrypted and written to the inner writter.
    /// If the writting operation results in Poll::Pending or Poll::Ready and the written bytes
    /// is less than the encrypted content, we write the rest of the encrypted bytes to the
    /// internal buffer and return Poll::Ready with the number of bytes that were encrypted.
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        if !self.buffer.is_empty() {
            ready!(self.as_mut().flush_buf(cx))?
        }

        let mut total_written = 0;
        for chunk in buf.chunks(super::CHUNK_SIZE) {
            let Some(encrypted) = self.get_encrypted(chunk) else {
                log::error!("Failed to encrypt content");
                return std::task::Poll::Ready(Err(std::io::ErrorKind::InvalidInput.into()));
            };

            total_written += chunk.len();

            let me = self.as_mut().project();
            match me.inner.poll_write(cx, &encrypted[..]) {
                Poll::Ready(written) => {
                    let written = written?;
                    if written < encrypted.len() {
                        self.buffer.put(&encrypted[written..]);
                        return Poll::Ready(Ok(total_written));
                    }
                }
                Poll::Pending => {
                    self.buffer.put(&encrypted[..]);
                    return Poll::Ready(Ok(total_written));
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

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use chacha20poly1305::KeyInit;
    use tokio::io::AsyncWriteExt;

    use crate::network::crypto_stream::get_key;

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

        let mut writer = WriteHalf::new(
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
}
