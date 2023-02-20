use std::io::SeekFrom;

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    config::Config,
    hash_helper,
    ignored_files::IgnoredFilesCache,
    storage::{self, FileInfo},
    transaction_log::{EntryStatus, EntryType, LogEntry, TransactionLog},
};

use super::{block_index, BlockHash, BlockIndex, TransferType};

pub struct FileReceiver {
    receiving_from: u64,
    remote_file: FileInfo,
    file_handle: File,
    block_size: u64,
    expected_blocks: u64,
    received_blocks: u64,
}

impl FileReceiver {
    pub async fn new(
        config: &'static Config,
        transaction_log: &'static TransactionLog,
        remote_file: FileInfo,
        block_size: u64,
        receiving_from: u64,
    ) -> crate::Result<Self> {
        let file_handle =
            crate::storage::file_operations::open_file_for_writing(config, &remote_file).await?;
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
                LogEntry::new(
                    EntryType::Write,
                    EntryStatus::Pending,
                    remote_file.get_date(),
                ),
            )
            .await?;

        let expected_blocks = (file_size / block_size) + 1;
        Ok(Self {
            receiving_from,
            remote_file,
            block_size,
            expected_blocks,
            received_blocks: 0,
            file_handle,
        })
    }

    pub async fn get_required_block_index(
        &mut self,
        remote_block_index: Vec<BlockHash>,
    ) -> crate::Result<Vec<BlockHash>> {
        let file_size = self.remote_file.file_size()?;
        let local_block_index =
            block_index::get_file_block_index(&mut self.file_handle, self.block_size, file_size)
                .await?;

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
    ) -> crate::Result<()> {
        let position = block_index * self.block_size;

        if self.file_handle.seek(SeekFrom::Start(position)).await? == position {
            self.file_handle.write_all(block).await?;
        }

        self.received_blocks += 1;
        Ok(())
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
                    self.remote_file.get_date(),
                ),
            )
            .await?;

        Ok(())
    }

    pub fn receiving_from(&self) -> u64 {
        self.receiving_from
    }
}

pub async fn get_transfer_type(
    remote_file: &FileInfo,
    config: &'static Config,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<TransferType> {
    if let Some(storage_config) = config.storages.get(&remote_file.storage) {
        if ignored_files_cache
            .get(storage_config)
            .await
            .is_ignored(&remote_file.path)
        {
            return Ok(TransferType::NoTransfer);
        }
    }

    let file_path = remote_file.get_absolute_path(config)?;
    if !file_path.exists() {
        return Ok(TransferType::FullFile);
    }

    let local_file = remote_file.get_local_file_info(config)?;
    if hash_helper::calculate_file_hash(remote_file)
        != hash_helper::calculate_file_hash(&local_file)
    {
        Ok(TransferType::Partial)
    } else {
        Ok(TransferType::NoTransfer)
    }
}
