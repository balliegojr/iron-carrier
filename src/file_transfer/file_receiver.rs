use std::io::SeekFrom;

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::OwnedSemaphorePermit,
};

use crate::{
    config::Config,
    file_transfer::get_file_block_index,
    hash_helper,
    storage::{self, FileInfo},
    transaction_log::{EntryStatus, EntryType, LogEntry, TransactionLog},
};

use super::{BlockHash, BlockIndex, TransferType};

pub struct FileReceiver {
    remote_file: FileInfo,
    file_handle: File,
    block_size: u64,
    expected_blocks: u64,
    received_blocks: u64,
    _transfer_permit: OwnedSemaphorePermit,
}

impl FileReceiver {
    pub async fn new(
        config: &'static Config,
        transaction_log: &'static TransactionLog,
        remote_file: FileInfo,
        block_size: u64,
        transfer_permit: OwnedSemaphorePermit,
    ) -> crate::Result<Self> {
        let file_handle = get_file_handle(&remote_file, config).await?;
        let file_size = remote_file.file_size()?;
        if file_size != file_handle.metadata().await?.len() {
            file_handle.set_len(file_size).await?;
            log::trace!("set {:?} len to {file_size}", remote_file.path);
        }

        transaction_log
            .append_entry(
                &remote_file.storage,
                &remote_file.path,
                None,
                LogEntry::new(EntryType::Write, EntryStatus::Done),
            )
            .await?;

        let expected_blocks = (file_size / block_size) + 1;
        Ok(Self {
            remote_file,
            block_size,
            expected_blocks,
            received_blocks: 0,
            file_handle,
            _transfer_permit: transfer_permit,
        })
    }

    pub async fn get_required_block_index(
        &mut self,
        remote_block_index: Vec<BlockHash>,
    ) -> crate::Result<Vec<BlockHash>> {
        let file_size = self.remote_file.file_size()?;
        let local_block_index =
            get_file_block_index(&mut self.file_handle, self.block_size, file_size).await?;

        let required: Vec<u64> = remote_block_index
            .into_iter()
            .zip(local_block_index.into_iter())
            .enumerate()
            .filter_map(|(i, (remote, local))| {
                if remote != local {
                    Some(i as u64)
                } else {
                    None
                }
            })
            .collect();

        self.expected_blocks = required.len() as u64;
        Ok(required)
    }

    pub async fn write_block(
        &mut self,
        block_index: BlockIndex,
        block: &[u8],
    ) -> crate::Result<bool> {
        let position = block_index * self.block_size;

        if self.file_handle.seek(SeekFrom::Start(position)).await? == position {
            self.file_handle.write_all(block).await?;
        }

        self.received_blocks += 1;
        Ok(self.received_blocks == self.expected_blocks)
    }

    pub async fn finish(
        mut self,
        config: &'static Config,
        transaction_log: &'static TransactionLog,
    ) -> crate::Result<()> {
        self.file_handle.flush().await?;
        log::trace!("finishing {:?} transfer", self.remote_file.path);

        storage::fix_times_and_permissions(&self.remote_file, config)?;

        let successful_transfer = self.received_blocks == self.expected_blocks;
        transaction_log
            .append_entry(
                &self.remote_file.storage,
                &self.remote_file.path,
                None,
                LogEntry::new(
                    EntryType::Write,
                    if successful_transfer {
                        EntryStatus::Done
                    } else {
                        EntryStatus::Fail
                    },
                ),
            )
            .await?;

        Ok(())
    }
}

pub async fn get_transfer_type(
    remote_file: &FileInfo,
    config: &'static Config,
) -> crate::Result<Option<TransferType>> {
    let file_path = remote_file.get_absolute_path(config)?;
    if !file_path.exists() {
        return Ok(Some(TransferType::Everything));
    }

    let local_file = remote_file.get_local_file_info(config)?;
    if hash_helper::calculate_file_hash(remote_file)
        != hash_helper::calculate_file_hash(&local_file)
    {
        Ok(Some(TransferType::PartialTransfer))
    } else {
        Ok(None)
    }
}

pub async fn get_file_handle(file_info: &FileInfo, config: &'static Config) -> crate::Result<File> {
    let file_path = file_info.get_absolute_path(config)?;

    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            log::debug!("creating folders {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(file_path)
        .await
        .map_err(Box::from)
}
