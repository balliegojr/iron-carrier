use std::{
    cmp,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::prelude::{FileExt, MetadataExt},
    sync::Arc,
    usize,
};

use message_io::{network::Endpoint, node::NodeHandler};

use crate::{
    config::Config,
    fs::{self, FileInfo},
    hash_helper,
};

use super::{send_message, CarrierEvent, FileSyncEvent};

pub(crate) struct FileTransferMan {
    config: Arc<Config>,
    handler: NodeHandler<CarrierEvent>,
    sync_out: HashMap<u64, FileSync>,
    sync_in: HashMap<u64, FileSync>,
    received_files: HashMap<FileInfo, u64>,
}

impl FileTransferMan {
    pub fn new(handler: NodeHandler<CarrierEvent>, config: Arc<Config>) -> Self {
        Self {
            handler,
            config,
            sync_out: HashMap::new(),
            sync_in: HashMap::new(),
            received_files: HashMap::new(),
        }
    }

    pub fn has_pending_transfers(&self) -> bool {
        self.sync_out.len() > 0 || self.sync_in.len() > 0
    }

    pub fn send_file_to_peer(
        &mut self,
        file_info: FileInfo,
        peer: Endpoint,
        peer_id: u64,
    ) -> crate::Result<()> {
        if self.is_file_from_peer(&file_info, peer_id) {
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            return Ok(());
        }

        let file_hash = hash_helper::calculate_hash(&file_info);
        let file_path = file_info.get_absolute_path(&self.config)?;
        let mut file_handler = std::fs::File::open(file_path)?;

        let file_size = file_info.size.unwrap();
        let block_size = get_block_size(file_size);
        let block_index = self.get_file_block_index(&mut file_handler, block_size, file_size);

        let event = FileSyncEvent::PrepareSync(file_info.clone(), file_hash, block_index.clone());

        let file_sync = FileSync {
            file_info,
            file_handler,
            block_index,
            block_size,
        };

        self.sync_out.insert(file_hash, file_sync);

        send_message(&self.handler, &CarrierEvent::FileSyncEvent(event), peer);

        Ok(())
    }

    fn is_file_from_peer(&self, file_info: &FileInfo, peer_id: u64) -> bool {
        self.received_files
            .get(file_info)
            .map_or(false, |v| *v == peer_id)
    }

    fn get_file_block_index(
        &self,
        file: &mut File,
        block_size: u64,
        file_size: u64,
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
                    block_index.push((position, hash_helper::calculate_hash(&buf)));
                    position += current_read;
                }
                Err(_) => todo!(),
            }
        }

        block_index
    }

    pub fn file_sync_event(
        &mut self,
        event: FileSyncEvent,
        endpoint: Endpoint,
        peer_id: u64,
    ) -> crate::Result<()> {
        match event {
            FileSyncEvent::PrepareSync(file_info, file_hash, block_index) => {
                self.handle_prepare_sync(file_info, file_hash, block_index, endpoint)
            }
            FileSyncEvent::SyncBlocks(file_hash, out_of_sync) => {
                self.handle_sync_blocks(file_hash, out_of_sync, endpoint)
            }
            FileSyncEvent::EndSync(file_hash) => self.handle_end_sync(file_hash, peer_id),
            FileSyncEvent::WriteChunk(file_hash, block_index, buf) => {
                self.handle_write_block(file_hash, block_index, buf)
            }
        }
    }

    fn handle_prepare_sync(
        &mut self,
        file_info: FileInfo,
        file_hash: u64,
        block_index: Vec<(usize, u64)>,
        endpoint: Endpoint,
    ) -> crate::Result<()> {
        let file_path = file_info.get_absolute_path(&self.config)?;

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                log::debug!("creating folders {:?}", parent);
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut file_handler = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(file_path)?;

        let local_file_size = file_handler.metadata()?.size();

        let block_size = get_block_size(file_info.size.unwrap());
        let local_block_index =
            self.get_file_block_index(&mut file_handler, block_size, local_file_size);

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
        };

        self.sync_in.insert(file_hash, file_sync);

        let event = FileSyncEvent::SyncBlocks(file_hash, out_of_sync);
        send_message(&self.handler, &CarrierEvent::FileSyncEvent(event), endpoint);

        Ok(())
    }

    fn handle_sync_blocks(
        &mut self,
        file_hash: u64,
        out_of_sync: Vec<usize>,
        endpoint: Endpoint,
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

            let event = FileSyncEvent::WriteChunk(file_hash, index, buf);
            send_message(&self.handler, &CarrierEvent::FileSyncEvent(event), endpoint);
        }
        send_message(
            &self.handler,
            &CarrierEvent::FileSyncEvent(FileSyncEvent::EndSync(file_hash)),
            endpoint,
        );
        self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        self.sync_out.remove(&file_hash);
        Ok(())
    }

    fn handle_write_block(
        &mut self,
        file_hash: u64,
        block_index: usize,
        buf: Vec<u8>,
    ) -> crate::Result<()> {
        let file_sync = self.sync_in.get_mut(&file_hash).unwrap();
        let (position, _) = file_sync.block_index[block_index];
        file_sync
            .file_handler
            .write_all_at(&buf[..], position as u64)?;

        Ok(())
    }

    fn handle_end_sync(&mut self, file_hash: u64, peer_id: u64) -> crate::Result<()> {
        let mut file_sync = self.sync_in.remove(&file_hash).unwrap();
        file_sync.file_handler.flush()?;

        fs::fix_times_and_permissions(&file_sync.file_info, &self.config)?;

        let file_info = { file_sync.file_info };
        self.received_files.insert(file_info, peer_id);

        self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        Ok(())
    }
}

const CHUNK_SIZE: u64 = 65536;
const MIN_BLOCK_SIZE: u64 = CHUNK_SIZE * 2;

fn get_block_size(_file_size: u64) -> u64 {
    //TODO: implement block size
    MIN_BLOCK_SIZE
}

#[derive(Debug)]
struct FileSync {
    file_info: FileInfo,
    file_handler: File,
    block_size: u64,
    block_index: Vec<(usize, u64)>,
}