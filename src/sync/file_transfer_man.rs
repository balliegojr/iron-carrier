use std::{
    cmp,
    collections::{HashMap, LinkedList},
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::prelude::{FileExt, MetadataExt},
    sync::Arc,
    usize,
};

use message_io::network::Endpoint;
use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    conn::{CommandDispatcher, CommandType},
    fs::{self, FileInfo},
    hash_helper,
    transaction_log::{self, EventStatus, EventType, TransactionLogWriter},
};

use super::{
    file_watcher::{EventSupression, FileWatcher},
    SyncEvent,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum FileHandlerEvent {
    DeleteFile(FileInfo),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FileSyncEvent {
    PrepareSync(FileInfo, u64, Vec<(usize, u64)>, bool),
    SyncBlocks(u64, Vec<usize>),
    WriteChunk(u64, usize, Vec<u8>),
    EndSync(u64),
}

impl std::fmt::Display for FileSyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileSyncEvent::PrepareSync(file, file_hash, _, _) => {
                write!(f, "Prepare Sync for file ({}) {:?}", file_hash, file.path)
            }
            FileSyncEvent::SyncBlocks(file_hash, blocks) => write!(
                f,
                "SyncBlocks - {} - blocks out of sync {}",
                file_hash,
                blocks.len()
            ),
            FileSyncEvent::WriteChunk(file_hash, block, _) => {
                write!(f, "WriteBlock - {} - Block Index {}", file_hash, block)
            }
            FileSyncEvent::EndSync(file_hash) => write!(f, "EndSync - {}", file_hash),
        }
    }
}

pub struct FileTransferMan {
    config: Arc<Config>,
    commands: CommandDispatcher,
    sync_out: HashMap<u64, FileSync>,
    sync_in: HashMap<u64, FileSync>,
    queue_out: LinkedList<(u64, Endpoint)>,
}

impl FileTransferMan {
    pub fn new(commands: CommandDispatcher, config: Arc<Config>) -> Self {
        Self {
            commands,
            config,
            sync_out: HashMap::new(),
            sync_in: HashMap::new(),
            queue_out: LinkedList::new(),
        }
    }

    pub fn handle_stream(&mut self, data: &[u8], peer_id: &str) -> crate::Result<()> {
        let file_hash = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let block_index = u64::from_be_bytes(data[8..16].try_into().unwrap()) as usize;
        let buf = &data[16..];

        self.handle_write_block(file_hash, block_index, buf)
    }

    pub fn handle_event(&mut self, event: FileHandlerEvent) -> crate::Result<()> {
        // TODO: include watcher suppression
        match event {
            FileHandlerEvent::DeleteFile(ref file_info) => self.delete_file(file_info),
        }
    }

    fn delete_file(&mut self, file: &FileInfo) -> crate::Result<()> {
        let event_status = match fs::delete_file(file, &self.config) {
            Ok(_) => EventStatus::Finished,
            Err(err) => {
                log::error!("Failed to delete file {}", err);
                EventStatus::Failed
            }
        };
        self.get_log_writer()?.append(
            file.storage.to_string(),
            EventType::Delete(file.path.clone()),
            event_status,
        )?;

        Ok(())
    }
    pub fn has_pending_transfers(&self) -> bool {
        !self.sync_out.is_empty() || !self.sync_in.is_empty() || !self.queue_out.is_empty()
    }

    pub fn send_file_to_peer(
        &mut self,
        file_info: FileInfo,
        peer_id: String,
        is_new_file: bool,
        is_request: bool,
    ) -> crate::Result<()> {
        log::info!("Sending file {:?} to peer", file_info.path);

        let file_hash = hash_helper::calculate_hash(&file_info);
        let event = match self.sync_out.get_mut(&file_hash) {
            Some(file_sync) => {
                file_sync.peers_count += 1;

                FileSyncEvent::PrepareSync(
                    file_info,
                    file_hash,
                    file_sync.block_index.clone(),
                    is_request,
                )
            }
            None => {
                let file_path = file_info.get_absolute_path(&self.config)?;
                let mut file_handler = std::fs::File::open(file_path)?;
                let file_size = file_info.size.unwrap();
                let block_size = get_block_size(file_size);
                let block_index = self.get_file_block_index(
                    &mut file_handler,
                    block_size,
                    file_size,
                    !is_new_file,
                );

                let file_sync = FileSync {
                    file_info: file_info.clone(),
                    file_handler,
                    block_index: block_index.clone(),
                    block_size,
                    peers_count: 1,
                    consume_queue: !is_request,
                };
                self.sync_out.insert(file_hash, file_sync);
                FileSyncEvent::PrepareSync(file_info, file_hash, block_index, is_request)
            }
        };

        self.commands.to(SyncEvent::FileSyncEvent(event), &peer_id);

        Ok(())
    }

    fn get_file_block_index(
        &self,
        file: &mut File,
        block_size: u64,
        file_size: u64,
        calculate_hash: bool,
    ) -> Vec<(usize, u64)> {
        //TODO: implement proper hashing/checksum

        if file_size == 0 {
            return Vec::new();
        }

        let total_blocks = (file_size / block_size) + 1;
        let mut block_index = Vec::with_capacity(total_blocks as usize);

        let mut buf = vec![0u8; block_size as usize];
        let mut position = 0usize;

        while position < file_size as usize {
            let current_read = std::cmp::min(file_size as usize - position, block_size as usize);
            match file.read_exact(&mut buf[..current_read]) {
                Ok(_) => {
                    if calculate_hash {
                        block_index.push((position, hash_helper::calculate_hash(&buf)));
                    } else {
                        block_index.push((position, 0))
                    }
                    position += current_read;
                }
                Err(_) => todo!(),
            }
        }

        block_index
    }

    fn file_sync_event(
        &mut self,
        event: FileSyncEvent,
        peer_id: String,
        file_watcher: &mut Option<FileWatcher>,
        log_writer: &mut TransactionLogWriter<File>,
    ) -> crate::Result<()> {
        match event {
            FileSyncEvent::PrepareSync(file_info, file_hash, block_index, consume_queue) => self
                .handle_prepare_sync(
                    file_info,
                    file_hash,
                    block_index,
                    peer_id,
                    log_writer,
                    consume_queue,
                ),
            FileSyncEvent::SyncBlocks(file_hash, out_of_sync) => {
                self.handle_sync_blocks(file_hash, out_of_sync, peer_id)
            }
            FileSyncEvent::EndSync(file_hash) => {
                self.handle_end_sync(file_hash, file_watcher, log_writer)
            }
            FileSyncEvent::WriteChunk(file_hash, block_index, buf) => {
                self.handle_write_block(file_hash, block_index, &buf)
            }
        }
    }

    fn handle_prepare_sync(
        &mut self,
        file_info: FileInfo,
        file_hash: u64,
        block_index: Vec<(usize, u64)>,
        peer_id: String,
        log_writer: &mut TransactionLogWriter<File>,
        consume_queue: bool,
    ) -> crate::Result<()> {
        let file_path = file_info.get_absolute_path(&self.config)?;

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                log::debug!("creating folders {:?}", parent);
                std::fs::create_dir_all(parent)?;
            }
        }

        log_writer.append(
            file_info.storage.clone(),
            EventType::Write(file_info.path.clone()),
            EventStatus::Started,
        )?;

        let mut file_handler = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_path)?;

        let local_file_size = file_handler.metadata()?.size();

        let block_size = get_block_size(file_info.size.unwrap());
        let local_block_index =
            self.get_file_block_index(&mut file_handler, block_size, local_file_size, true);

        let out_of_sync = block_index
            .iter()
            .enumerate()
            .filter_map(|(index, (pos, hash))| match local_block_index.get(index) {
                Some((local_pos, local_hash)) => {
                    if local_pos == pos && hash == local_hash {
                        None
                    } else {
                        Some(index)
                    }
                }
                None => Some(index),
            })
            .collect();

        let file_sync = FileSync {
            file_info,
            file_handler,
            block_index,
            block_size,
            peers_count: 1,
            consume_queue,
        };

        self.sync_in.insert(file_hash, file_sync);

        let event = FileSyncEvent::SyncBlocks(file_hash, out_of_sync);
        self.commands.to(SyncEvent::FileSyncEvent(event), &peer_id);

        Ok(())
    }

    fn handle_sync_blocks(
        &mut self,
        file_hash: u64,
        out_of_sync: Vec<usize>,
        peer_id: String,
    ) -> crate::Result<()> {
        // TODO: split execution into multiple sends
        let file_sync = self.sync_out.get_mut(&file_hash).unwrap();
        let file_size = file_sync.file_info.size.unwrap() as usize;
        for index in out_of_sync {
            let (position, _) = file_sync.block_index[index];
            let bytes_to_read = cmp::min(file_sync.block_size as usize, file_size - position);
            let mut buf = vec![0u8; bytes_to_read];

            file_sync
                .file_handler
                .read_exact_at(&mut buf[..bytes_to_read], position as u64)?;

            let mut data = Vec::with_capacity(bytes_to_read + 16);
            data.extend(file_hash.to_be_bytes());
            data.extend((index as u64).to_be_bytes());
            data.extend(&buf[..bytes_to_read]);

            self.commands.to(CommandType::Stream(data), &peer_id);
        }
        self.commands.to(
            SyncEvent::FileSyncEvent(FileSyncEvent::EndSync(file_hash)),
            &peer_id,
        );
        if file_sync.consume_queue {
            self.commands.now(SyncEvent::ConsumeSyncQueue);
        }
        file_sync.peers_count -= 1;
        if file_sync.peers_count == 0 {
            self.sync_out.remove(&file_hash);
        }

        Ok(())
    }

    pub fn handle_write_block(
        &mut self,
        file_hash: u64,
        block_index: usize,
        buf: &[u8],
    ) -> crate::Result<()> {
        let file_sync = self.sync_in.get_mut(&file_hash).unwrap();
        let (position, _) = file_sync.block_index[block_index];
        file_sync.file_handler.write_all_at(buf, position as u64)?;

        Ok(())
    }

    fn handle_end_sync(
        &mut self,
        file_hash: u64,
        file_watcher: &mut Option<FileWatcher>,
        log_writer: &mut TransactionLogWriter<File>,
    ) -> crate::Result<()> {
        let mut file_sync = self.sync_in.remove(&file_hash).unwrap();
        file_sync.file_handler.flush()?;

        fs::fix_times_and_permissions(&file_sync.file_info, &self.config)?;

        let file_info = { file_sync.file_info };
        log_writer.append(
            file_info.storage.clone(),
            EventType::Write(file_info.path.clone()),
            EventStatus::Finished,
        )?;

        if let Some(ref mut file_watcher) = file_watcher {
            file_watcher.supress_next_event(file_info, EventSupression::Write);
        }

        if file_sync.consume_queue {
            self.commands.now(SyncEvent::ConsumeSyncQueue);
        }
        Ok(())
    }
    fn get_log_writer(&mut self) -> crate::Result<&mut TransactionLogWriter<File>> {
        todo!()
        // match self.log_writer {
        //     Some(ref mut log_writer) => Ok(log_writer),
        //     None => {
        //         let log_writer = transaction_log::get_log_writer(&self.config.log_path)?;
        //         self.log_writer = Some(log_writer);
        //         Ok(self.log_writer.as_mut().unwrap())
        //     }
        // }
    }
}

const MIN_BLOCK_SIZE: u64 = 1024 * 128;
const MAX_BLOCK_SIZE: u64 = 1024 * 1024 * 16;

fn get_block_size(file_size: u64) -> u64 {
    let mut block_size = MIN_BLOCK_SIZE;
    while block_size < MAX_BLOCK_SIZE && file_size / block_size > 2000 {
        block_size *= 2;
    }

    block_size
}

#[derive(Debug)]
struct FileSync {
    file_info: FileInfo,
    file_handler: File,
    block_size: u64,
    block_index: Vec<(usize, u64)>,
    peers_count: u32,
    consume_queue: bool,
}
