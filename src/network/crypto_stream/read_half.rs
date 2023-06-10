use chacha20poly1305::{aead::stream::DecryptorLE31, XChaCha20Poly1305};
use pin_project_lite::pin_project;
use std::{pin::Pin, task::ready};

use tokio::io::{AsyncBufRead, AsyncRead};

use super::BUFFER_SIZE;

pin_project! {
    /// Async Encryption Read Half
    pub struct ReadHalf<T> {
        #[pin]
        inner: T,
        decryptor: DecryptorLE31<XChaCha20Poly1305>,
        buffer: Vec<u8>,
        pos: usize,
        cap: usize
    }
}

impl<T> ReadHalf<T> {
    pub fn new(inner: T, decryptor: DecryptorLE31<XChaCha20Poly1305>) -> Self {
        Self {
            inner,
            decryptor,
            buffer: vec![0u8; BUFFER_SIZE],
            pos: 0,
            cap: 0,
        }
    }
    /// Produce a value if there is enough data in the internal buffer
    ///
    /// When a value is produced, it will advance the buffer to the position for the next value.
    fn produce(mut self: Pin<&mut Self>) -> Option<Vec<u8>> {
        if self.cap <= self.pos {
            return None;
        }

        // Producing a value is a relatively simple operation.
        // Read 4 bytes from the buffer and cast to a u32 as the length of the message.
        // If there is enough bytes in the buffer, read the bytes and decrypt the message.
        //
        // Then advance the buffer to the next position (4 + length)
        //
        // If there isn't enough bytes to produce a message, just return None

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

    /// Adjusts the buffer to fit the next full message.
    ///
    /// When the buffer reach a position where the length of the message is greater than the buffer
    /// available capacity, it is necessary to reset the buffer position to 0 and move the bytes
    /// available to the beginning of the buffer, freeing buffer capacity to be filled.
    ///
    /// It is also possible that the message length is bigger than the buffer full size, in this
    /// case the buffer will be resized to double it's full capacity. This operation should not
    /// be necessary because the writter is limited to write 1024 bytes long messages
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

impl<T> AsyncRead for ReadHalf<T>
where
    T: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // The poll read simply tries to produce a value from the internal buffer.
        // If no value is produced, it then tries to poll more bytes from the inner reader
        loop {
            if let Some(decrypted) = self.as_mut().produce() {
                buf.put_slice(&decrypted);
                return std::task::Poll::Ready(Ok(()));
            }

            if ready!(self.as_mut().poll_fill_buf(cx))?.is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }
        }
    }
}

impl<R: AsyncRead> tokio::io::AsyncBufRead for ReadHalf<R> {
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

#[cfg(test)]
mod tests {
    use std::{assert_eq, time::Duration};

    use chacha20poly1305::{aead::stream::EncryptorLE31, KeyInit};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::network::crypto_stream::get_key;

    use super::*;

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
        let mut reader = ReadHalf::new(rx, decryptor);

        let mut plain_content = String::new();
        let _ = reader.read_to_string(&mut plain_content).await;

        assert_eq!(
            plain_content,
            "some contentsome other contenteven more content"
        );
    }
}
