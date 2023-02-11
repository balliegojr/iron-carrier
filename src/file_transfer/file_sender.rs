use std::{
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    io::SeekFrom,
};

use tokio::io::AsyncSeekExt;
use tokio::{fs::File, io::AsyncReadExt};

use crate::{
    config::Config, hash_helper, network::ConnectionHandler, network_events::NetworkEvents,
    storage::FileInfo, IronCarrierError,
};

use super::{
    get_block_size, get_file_block_index, BlockHash, BlockIndex, FileTransfer, TransferType,
};

pub struct FileSender {
    file: FileInfo,
    transfer_id: u64,
    file_handle: File,
    block_size: u64,
    block_hashes: Vec<BlockHash>,
    participant_nodes: HashSet<u64>,

    nodes_required_blocks: HashMap<u64, Vec<BlockIndex>>,
}

impl FileSender {
    pub async fn new(
        file: FileInfo,
        nodes: HashSet<u64>,
        config: &'static Config,
    ) -> crate::Result<Self> {
        let file_size = file.file_size()?;
        let block_size = get_block_size(file_size);
        let transfer_id = hash_helper::calculate_file_hash(&file);

        // TODO: file block index should be built only if necessary
        let mut file_handle = {
            let file_path = file.get_absolute_path(config)?;
            tokio::fs::File::open(file_path).await?
        };
        let block_hashes = get_file_block_index(&mut file_handle, block_size, file_size).await?;

        Ok(Self {
            file,
            transfer_id,
            participant_nodes: nodes,
            block_size,
            nodes_required_blocks: HashMap::default(),
            block_hashes,
            file_handle,
        })
    }

    pub fn transfer_id(&self) -> u64 {
        self.transfer_id
    }

    pub fn has_participant_nodes(&self) -> bool {
        !self.participant_nodes.is_empty()
    }

    pub fn pending_information(&self) -> bool {
        !self
            .participant_nodes
            .iter()
            .all(|node| self.nodes_required_blocks.contains_key(node))
    }

    pub async fn query_transfer_type(
        &self,
        connection_handler: &'static ConnectionHandler<NetworkEvents>,
    ) -> crate::Result<()> {
        connection_handler
            .broadcast_to(
                NetworkEvents::FileTransfer(
                    self.transfer_id(),
                    FileTransfer::QueryTransferType {
                        file: self.file.clone(),
                        block_size: self.block_size,
                    },
                ),
                self.participant_nodes.iter(),
            )
            .await
    }

    pub async fn set_transfer_type(
        &mut self,
        connection_handler: &'static ConnectionHandler<NetworkEvents>,
        node_id: u64,
        transfer_type: TransferType,
    ) -> crate::Result<()> {
        match transfer_type {
            TransferType::FullFile => {
                let required_blocks = (0..self.block_hashes.len() as u64).collect();
                self.nodes_required_blocks.insert(node_id, required_blocks);
            }
            TransferType::Partial => {
                connection_handler
                    .send_to(
                        NetworkEvents::FileTransfer(
                            self.transfer_id,
                            FileTransfer::QueryRequiredBlocks {
                                sender_block_index: self.block_hashes.clone(),
                            },
                        ),
                        node_id,
                    )
                    .await?;
            }
            TransferType::NoTransfer => {
                self.nodes_required_blocks.remove(&node_id);
                self.participant_nodes.retain(|node| *node != node_id);
            }
        };

        Ok(())
    }

    pub fn set_required_blocks(&mut self, node_id: u64, required_blocks: Vec<BlockIndex>) {
        self.nodes_required_blocks.insert(node_id, required_blocks);
    }

    pub async fn transfer_blocks(
        mut self,
        connection_handler: &'static ConnectionHandler<NetworkEvents>,
    ) -> crate::Result<()> {
        let mut block_nodes: BTreeMap<u64, Vec<u64>> = std::collections::BTreeMap::new();

        for (node, node_blocks) in self.nodes_required_blocks.into_iter() {
            for block in node_blocks {
                block_nodes.entry(block).or_default().push(node);
            }
        }

        let file_size = self.file.file_size()?;
        // FIXME: use a proper streaming for this, without bincode
        for (block_index, nodes) in block_nodes.into_iter() {
            let position = block_index * self.block_size;
            let bytes_to_read = cmp::min(self.block_size, file_size - position);

            let mut block = vec![0u8; bytes_to_read as usize];

            if self.file_handle.seek(SeekFrom::Start(position)).await? != position {
                return Err(IronCarrierError::IOReadingError.into());
            }

            self.file_handle
                .read_exact(&mut block[..bytes_to_read as usize])
                .await?;

            connection_handler
                .broadcast_to(
                    NetworkEvents::FileTransfer(
                        self.transfer_id,
                        FileTransfer::TransferBlock {
                            block_index,
                            block: block.into(),
                        },
                    ),
                    nodes.iter(),
                )
                .await?;
        }

        connection_handler
            .broadcast_to(
                NetworkEvents::FileTransfer(self.transfer_id, FileTransfer::TransferComplete),
                self.participant_nodes.iter(),
            )
            .await?;

        Ok(())
    }
}
