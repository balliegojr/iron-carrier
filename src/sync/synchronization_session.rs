use std::collections::{HashMap, HashSet};

use crate::{conn::CommandDispatcher, fs::FileInfo};

use super::{FileHandlerEvent, Origin, SyncEvent};

pub(crate) struct SynchronizationState {
    storage_state: HashMap<String, HashSet<String>>,
    current_storage_index: HashMap<Origin, HashSet<FileInfo>>,
    current_storage_peers: HashSet<String>,
    expected_state_replies: usize,
    commands: CommandDispatcher,
}

impl SynchronizationState {
    pub fn new(commands: CommandDispatcher) -> Self {
        Self {
            storage_state: HashMap::new(),
            current_storage_index: HashMap::new(),
            expected_state_replies: 0,
            current_storage_peers: HashSet::new(),
            commands,
        }
    }

    pub fn add_storages_to_sync(&mut self, storages: Vec<String>, peer_id: &str) -> bool {
        for storage in storages {
            let state = self
                .storage_state
                .entry(storage)
                .or_insert_with(HashSet::new);

            state.insert(peer_id.to_string());
        }

        self.expected_state_replies -= 1;
        self.expected_state_replies == 0
    }

    pub fn start_sync_after_replies(&mut self, number_of_replies: usize) {
        self.expected_state_replies = number_of_replies;
    }

    pub fn get_next_storage(&mut self) -> Option<(String, HashSet<String>)> {
        if self.storage_state.is_empty() {
            return None;
        }

        let next_storage = self.storage_state.iter().next();

        let storage = match next_storage {
            Some((storage, _)) => storage.clone(),
            None => return None,
        };

        let peers = self.storage_state.remove(&storage).unwrap();
        self.current_storage_peers = peers.clone();
        Some((storage, peers))
    }

    pub fn set_storage_index(&mut self, origin: Origin, index: HashSet<FileInfo>) -> bool {
        self.current_storage_index.insert(origin, index);
        self.current_storage_peers.len() == self.current_storage_index.len() - 1
    }

    pub fn build_event_queue(&mut self) {
        let consolidated_index = self.get_consolidated_index();
        consolidated_index
            .into_iter()
            .for_each(|(_, v)| self.convert_index_to_events(v));

        self.current_storage_index.clear();
        self.current_storage_peers.clear();

        if !self.storage_state.is_empty() {
            self.commands.enqueue(SyncEvent::SyncNextStorage);
        }
    }

    fn get_consolidated_index(&self) -> HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> {
        let mut consolidated_index: HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> =
            HashMap::new();
        for (origin, index) in &self.current_storage_index {
            for file in index {
                consolidated_index
                    .entry(file)
                    .or_default()
                    .insert(origin, file);
            }
        }

        consolidated_index.retain(|file, files| {
            files.len() < self.current_storage_index.len()
                || files.values().any(|w| file.is_out_of_sync(w))
        });

        consolidated_index
    }
    /// convert each line of the consolidated index to a vec of actions
    fn convert_index_to_events(&self, files: HashMap<&Origin, &FileInfo>) {
        let (source, source_file) = files
            .iter()
            .max_by(|(_, a), (_, b)| {
                a.deleted_at
                    .unwrap_or_else(|| a.modified_at.unwrap())
                    .cmp(&b.deleted_at.unwrap_or_else(|| b.modified_at.unwrap()))
            })
            .unwrap();

        match *source {
            Origin::Initiator => {
                for action in self.current_storage_peers.iter().filter_map(|peer_id| {
                    self.get_action_for_peer_file(
                        source_file,
                        files.get(&Origin::Peer(peer_id.clone())),
                        peer_id.clone(),
                    )
                }) {
                    self.commands.enqueue(action);
                }
            }
            Origin::Peer(origin_peer) => {
                if source_file.is_deleted() {
                    self.commands
                        .enqueue(FileHandlerEvent::DeleteFile((*source_file).clone()))
                } else {
                    self.commands.enqueue(FileHandlerEvent::RequestFile(
                        (*source_file).clone(),
                        origin_peer.clone(),
                        !files.contains_key(&Origin::Initiator),
                    ))
                }

                for action in self
                    .current_storage_peers
                    .iter()
                    .filter(|peer| **peer != *origin_peer)
                    .filter_map(|peer_id| {
                        self.get_action_for_peer_file(
                            source_file,
                            files.get(&Origin::Peer(peer_id.clone())),
                            peer_id.clone(),
                        )
                    })
                {
                    self.commands.enqueue(action);
                }
            }
        }
    }

    fn get_action_for_peer_file(
        &self,
        source_file: &FileInfo,
        peer_file: Option<&&FileInfo>,
        peer_id: String,
    ) -> Option<FileHandlerEvent> {
        match peer_file {
            Some(&peer_file) => {
                if source_file.is_out_of_sync(peer_file) {
                    Some(FileHandlerEvent::SendFile(
                        source_file.clone(),
                        peer_id,
                        false,
                    ))
                } else {
                    None
                }
            }
            None => {
                if source_file.is_deleted() {
                    None
                } else {
                    Some(FileHandlerEvent::SendFile(
                        source_file.clone(),
                        peer_id,
                        true,
                    ))
                }
            }
        }
    }
}
