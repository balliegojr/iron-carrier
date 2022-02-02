use std::{
    cmp,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::prelude::{FileExt, MetadataExt},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    conn::{CommandDispatcher, Commands},
    fs::{self, FileInfo},
    hash_helper,
    transaction_log::{EventStatus, EventType, TransactionLogWriter},
};

use super::{file_watcher::SupressionType, WatcherEvent};

#[derive(Debug, Serialize, Deserialize)]
pub enum FileHandlerEvent {
    DeleteFile(FileInfo),
    MoveFile(FileInfo, FileInfo),
    SendFile(FileInfo, String, bool),
    BroadcastFile(FileInfo, bool),
    RequestFile(FileInfo, String, bool),
    PrepareSync(FileInfo, u64, Vec<(u64, u64)>, bool),
    SyncBlocks(u64, Vec<u64>),
    WriteChunk(u64, usize, Vec<u8>),
    EndSync(u64),
}

impl std::fmt::Display for FileHandlerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileHandlerEvent::PrepareSync(file, file_hash, _, _) => {
                write!(f, "Prepare Sync for file ({}) {:?}", file_hash, file.path)
            }
            FileHandlerEvent::SyncBlocks(file_hash, blocks) => write!(
                f,
                "SyncBlocks - {} - blocks out of sync {}",
                file_hash,
                blocks.len()
            ),
            FileHandlerEvent::WriteChunk(file_hash, block, _) => {
                write!(f, "WriteBlock - {} - Block Index {}", file_hash, block)
            }
            FileHandlerEvent::EndSync(file_hash) => write!(f, "EndSync - {}", file_hash),
            FileHandlerEvent::DeleteFile(file) => write!(f, "Delete file {:?}", file.path),
            FileHandlerEvent::SendFile(file, peer_id, _) => {
                write!(f, "Send file {:?} to {}", file.path, peer_id)
            }
            FileHandlerEvent::BroadcastFile(_, _) => write!(f, "Broadcast file to all peers"),
            FileHandlerEvent::RequestFile(file, _, _) => write!(f, "Request File {:?}", file.path),
            FileHandlerEvent::MoveFile(src, dest) => {
                write!(f, "Move file from {:?} to {:?}", src.path, dest.path)
            }
        }
    }
}

pub struct FileTransferMan {
    config: Arc<Config>,
    commands: CommandDispatcher,
    sync_out: HashMap<u64, FileSync>,
    sync_in: HashMap<u64, FileSync>,
    log_writer: TransactionLogWriter<File>,
}

impl FileTransferMan {
    pub fn new(
        commands: CommandDispatcher,
        config: Arc<Config>,
        log_writer: TransactionLogWriter<File>,
    ) -> Self {
        Self {
            commands,
            config,
            sync_out: HashMap::new(),
            sync_in: HashMap::new(),
            log_writer,
        }
    }

    pub fn handle_stream(&mut self, data: &[u8], _peer_id: &str) -> crate::Result<bool> {
        let file_hash = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let block_index = u64::from_be_bytes(data[8..16].try_into().unwrap()) as usize;
        let buf = &data[16..];

        self.handle_write_block(file_hash, block_index, buf)
    }

    pub fn handle_event(
        &mut self,
        event: FileHandlerEvent,
        peer_id: Option<&str>,
    ) -> crate::Result<bool> {
        match event {
            FileHandlerEvent::DeleteFile(file_info) => {
                self.delete_file(file_info)?;
                Ok(peer_id.is_none())
            }
            FileHandlerEvent::MoveFile(src, dest) => {
                self.move_file(src, dest)?;
                Ok(peer_id.is_none())
            }
            FileHandlerEvent::SendFile(file_info, peer_id, is_new_file) => {
                self.send_prepare_sync(file_info, &peer_id, is_new_file, false)
            }
            FileHandlerEvent::BroadcastFile(file_info, is_new_file) => {
                self.broadcast_prepare_sync(file_info, is_new_file)
            }
            FileHandlerEvent::RequestFile(file_info, to_peer_id, is_new_file) => {
                // FIXME: improve this flow
                if peer_id.is_none() {
                    self.commands.to(
                        FileHandlerEvent::RequestFile(
                            file_info,
                            self.config.node_id.clone(),
                            is_new_file,
                        ),
                        &to_peer_id,
                    );
                    Ok(false)
                } else {
                    self.send_prepare_sync(file_info, &to_peer_id, is_new_file, true)
                }
            }
            FileHandlerEvent::PrepareSync(file_info, file_hash, block_index, consume_queue) => self
                .handle_prepare_sync(
                    file_info,
                    file_hash,
                    block_index,
                    peer_id.unwrap(),
                    consume_queue,
                ),
            FileHandlerEvent::SyncBlocks(file_hash, out_of_sync) => {
                self.handle_sync_blocks(file_hash, out_of_sync, peer_id.unwrap())
            }
            FileHandlerEvent::EndSync(file_hash) => self.handle_end_sync(file_hash),
            FileHandlerEvent::WriteChunk(file_hash, block_index, buf) => {
                self.handle_write_block(file_hash, block_index, &buf)
            }
        }
    }

    fn delete_file(&mut self, file: FileInfo) -> crate::Result<()> {
        match fs::delete_file(&file, &self.config) {
            Ok(_) => {
                self.log_writer.append(
                    file.storage.to_string(),
                    EventType::Delete(file.path.clone()),
                    EventStatus::Finished,
                )?;
                self.commands
                    .now(WatcherEvent::Supress(file, SupressionType::Delete));
            }
            Err(err) => {
                log::error!("Failed to delete file {}", err);
            }
        };

        Ok(())
    }

    fn move_file(&mut self, src: FileInfo, dest: FileInfo) -> crate::Result<()> {
        match fs::move_file(&src, &dest, &self.config) {
            Err(err) => {
                log::error!("Failed to move file: {}", err);
            }
            Ok(_) => {
                self.log_writer.append(
                    src.storage.clone(),
                    EventType::Move(src.path.clone(), dest.path),
                    EventStatus::Finished,
                )?;

                self.commands
                    .now(WatcherEvent::Supress(src, SupressionType::Delete));
            }
        };

        Ok(())
    }

    pub fn send_prepare_sync(
        &mut self,
        file_info: FileInfo,
        peer_id: &str,
        is_new_file: bool,
        is_request: bool,
    ) -> crate::Result<bool> {
        log::info!("Sending file {:?} to peer", file_info.path);

        let file_hash = hash_helper::calculate_hash(&file_info);
        let event = match self.sync_out.get_mut(&file_hash) {
            Some(file_sync) => {
                if let Some(peers_count) = file_sync.peers_count.as_mut() {
                    *peers_count += 1;
                }

                FileHandlerEvent::PrepareSync(
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
                    peers_count: Some(1),
                    consume_queue: !is_request,
                };
                self.sync_out.insert(file_hash, file_sync);
                FileHandlerEvent::PrepareSync(file_info, file_hash, block_index, is_request)
            }
        };

        self.commands.to(event, peer_id);

        Ok(false)
    }
    pub fn broadcast_prepare_sync(
        &mut self,
        file_info: FileInfo,
        is_new_file: bool,
    ) -> crate::Result<bool> {
        log::info!("Broadcast prepare sync file {:?}", file_info.path);

        let file_hash = hash_helper::calculate_hash(&file_info);
        let file_path = file_info.get_absolute_path(&self.config)?;
        let mut file_handler = std::fs::File::open(file_path)?;
        let file_size = file_info.size.unwrap();
        let block_size = get_block_size(file_size);
        let block_index =
            self.get_file_block_index(&mut file_handler, block_size, file_size, !is_new_file);

        let file_sync = FileSync {
            file_info: file_info.clone(),
            file_handler,
            block_index: block_index.clone(),
            block_size,
            peers_count: None,
            consume_queue: true,
        };
        self.sync_out.insert(file_hash, file_sync);
        let event = FileHandlerEvent::PrepareSync(file_info, file_hash, block_index, false);

        self.commands.broadcast(event);

        Ok(false)
    }

    fn get_file_block_index(
        &self,
        file: &mut File,
        block_size: u64,
        file_size: u64,
        calculate_hash: bool,
    ) -> Vec<(u64, u64)> {
        //TODO: implement proper hashing/checksum

        if file_size == 0 {
            return Vec::new();
        }

        let total_blocks = (file_size / block_size) + 1;
        let mut block_index = Vec::with_capacity(total_blocks as usize);

        let mut buf = vec![0u8; block_size as usize];
        let mut position = 0u64;

        while position < file_size {
            let current_read = std::cmp::min(file_size - position, block_size) as usize;
            match file.read_exact(&mut buf[..current_read]) {
                Ok(_) => {
                    if calculate_hash {
                        block_index.push((position, hash_helper::calculate_hash(&buf)));
                    } else {
                        block_index.push((position, 0))
                    }
                    position += current_read as u64;
                }
                Err(_) => todo!(),
            }
        }

        block_index
    }

    fn handle_prepare_sync(
        &mut self,
        file_info: FileInfo,
        file_hash: u64,
        block_index: Vec<(u64, u64)>,
        peer_id: &str,
        consume_queue: bool,
    ) -> crate::Result<bool> {
        let file_path = file_info.get_absolute_path(&self.config)?;

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                log::debug!("creating folders {:?}", parent);
                std::fs::create_dir_all(parent)?;
            }
        }

        self.log_writer.append(
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

        let out_of_sync: Vec<u64> = block_index
            .iter()
            .enumerate()
            .filter_map(|(index, (pos, hash))| match local_block_index.get(index) {
                Some((local_pos, local_hash)) => {
                    if local_pos == pos && hash == local_hash {
                        None
                    } else {
                        Some(index as u64)
                    }
                }
                None => Some(index as u64),
            })
            .collect();

        if !out_of_sync.is_empty() {
            let file_sync = FileSync {
                file_info,
                file_handler,
                block_index,
                block_size,
                peers_count: None,
                consume_queue,
            };

            self.sync_in.insert(file_hash, file_sync);
        }

        self.commands.to(
            FileHandlerEvent::SyncBlocks(file_hash, out_of_sync),
            peer_id,
        );

        Ok(false)
    }

    fn handle_sync_blocks(
        &mut self,
        file_hash: u64,
        out_of_sync: Vec<u64>,
        peer_id: &str,
    ) -> crate::Result<bool> {
        // TODO: split execution into multiple sends
        let file_sync = self.sync_out.get_mut(&file_hash).unwrap();
        let file_size = file_sync.file_info.size.unwrap();
        for index in out_of_sync {
            let (position, _) = file_sync.block_index[index as usize];
            let bytes_to_read = cmp::min(file_sync.block_size, file_size - position) as usize;
            let mut buf = vec![0u8; bytes_to_read];

            file_sync
                .file_handler
                .read_exact_at(&mut buf[..bytes_to_read], position as u64)?;

            let mut data = Vec::with_capacity(bytes_to_read + 16);
            data.extend(file_hash.to_be_bytes());
            data.extend((index as u64).to_be_bytes());
            data.extend(&buf[..bytes_to_read]);

            self.commands.to(Commands::Stream(data), peer_id);
        }
        let consume_queue = file_sync.consume_queue;

        self.commands
            .to(FileHandlerEvent::EndSync(file_hash), peer_id);

        let should_remove = match file_sync.peers_count.as_mut() {
            Some(peers_count) => {
                *peers_count -= 1;
                *peers_count == 0
            }
            None => false,
        };

        if should_remove {
            self.sync_out.remove(&file_hash);
        }

        Ok(consume_queue)
    }

    pub fn handle_write_block(
        &mut self,
        file_hash: u64,
        block_index: usize,
        buf: &[u8],
    ) -> crate::Result<bool> {
        let file_sync = self.sync_in.get_mut(&file_hash).unwrap();
        let (position, _) = file_sync.block_index[block_index];
        file_sync.file_handler.write_all_at(buf, position as u64)?;

        Ok(false)
    }

    fn handle_end_sync(&mut self, file_hash: u64) -> crate::Result<bool> {
        match self.sync_in.remove(&file_hash) {
            Some(mut file_sync) => {
                file_sync.file_handler.flush()?;

                fs::fix_times_and_permissions(&file_sync.file_info, &self.config)?;

                let file_info = { file_sync.file_info };
                self.log_writer.append(
                    file_info.storage.clone(),
                    EventType::Write(file_info.path.clone()),
                    EventStatus::Finished,
                )?;

                self.commands
                    .now(WatcherEvent::Supress(file_info, SupressionType::Write));

                Ok(file_sync.consume_queue)
            }
            None => Ok(false),
        }
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
    block_index: Vec<(u64, u64)>,
    peers_count: Option<u32>,
    consume_queue: bool,
}
