use chacha20poly1305::{
    aead::{
        stream::{DecryptorLE31, EncryptorLE31},
        KeyInit,
    },
    XChaCha20Poly1305,
};
use pbkdf2::pbkdf2_hmac_array;
use sha2::Sha256;

use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

mod read_half;
mod write_half;

const BUFFER_SIZE: usize = 4096;
const CHUNK_SIZE: usize = 1024;

pub fn crypto_stream(
    inner: TcpStream,
    plain_key: &str,
    group_name: &str,
) -> (
    read_half::ReadHalf<OwnedReadHalf>,
    write_half::WriteHalf<OwnedWriteHalf>,
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
        read_half::ReadHalf::new(read, decryptor),
        write_half::WriteHalf::new(write, encryptor),
    )
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

        let mut writer = write_half::WriteHalf::new(tx, encryptor);
        let mut reader = read_half::ReadHalf::new(rx, decryptor);

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
