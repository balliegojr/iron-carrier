use std::collections::{HashMap, HashSet, LinkedList};

use crate::fs::FileInfo;

use super::{CarrierEvent, Origin, QueueEventType};

#[derive(Debug)]
pub(crate) struct SynchronizationState {
    storage_state: HashMap<String, HashSet<u64>>,
    current_storage_index: HashMap<Origin, HashSet<FileInfo>>,
    current_storage_peers: HashSet<u64>,
    expected_state_replies: usize,
}

impl SynchronizationState {
    pub fn new() -> Self {
        Self {
            storage_state: HashMap::new(),
            current_storage_index: HashMap::new(),
            expected_state_replies: 0,
            current_storage_peers: HashSet::new(),
        }
    }

    pub fn add_storages_to_sync(&mut self, storages: Vec<String>, peer_id: u64) -> bool {
        for storage in storages {
            let state = self
                .storage_state
                .entry(storage)
                .or_insert_with(HashSet::new);

            state.insert(peer_id);
        }

        self.expected_state_replies -= 1;
        self.expected_state_replies == 0
    }

    pub fn start_sync_after_replies(&mut self, number_of_replies: usize) {
        self.expected_state_replies = number_of_replies;
    }

    pub fn get_next_storage(&mut self) -> Option<(String, HashSet<u64>)> {
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

    pub fn get_event_queue(&mut self) -> LinkedList<(CarrierEvent, QueueEventType)> {
        let consolidated_index = self.get_consolidated_index();
        let mut actions: LinkedList<(CarrierEvent, QueueEventType)> = consolidated_index
            .iter()
            .flat_map(|(_, v)| self.convert_index_to_events(v))
            .collect();

        self.current_storage_index.clear();
        self.current_storage_peers.clear();

        if !self.storage_state.is_empty() {
            actions.push_back((CarrierEvent::SyncNextStorage, QueueEventType::Signal));
        }

        actions
    }

    fn get_consolidated_index(&self) -> HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> {
        let mut consolidated_index: HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> =
            HashMap::new();
        for (origin, index) in &self.current_storage_index {
            for file in index {
                consolidated_index
                    .entry(&file)
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
    fn convert_index_to_events(
        &self,
        files: &HashMap<&Origin, &FileInfo>,
    ) -> Vec<(CarrierEvent, QueueEventType)> {
        let mut actions = Vec::new();
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
                actions.extend(self.current_storage_peers.iter().filter_map(|peer_id| {
                    self.get_action_for_peer_file(
                        source_file,
                        files.get(&Origin::Peer(*peer_id)),
                        *peer_id,
                    )
                }));
            }
            Origin::Peer(origin_peer) => {
                actions.push(if source_file.is_deleted() {
                    (
                        CarrierEvent::DeleteFile((*source_file).clone()),
                        QueueEventType::Signal,
                    )
                } else {
                    (
                        CarrierEvent::RequestFile(
                            (*source_file).clone(),
                            !files.contains_key(&Origin::Initiator),
                        ),
                        QueueEventType::Peer(*origin_peer),
                    )
                });
                actions.extend(
                    self.current_storage_peers
                        .iter()
                        .filter(|peer| **peer != *origin_peer)
                        .filter_map(|peer_id| {
                            self.get_action_for_peer_file(
                                source_file,
                                files.get(&Origin::Peer(*peer_id)),
                                *peer_id,
                            )
                        }),
                );
            }
        }

        actions
    }

    fn get_action_for_peer_file(
        &self,
        source_file: &FileInfo,
        peer_file: Option<&&FileInfo>,
        peer_id: u64,
    ) -> Option<(CarrierEvent, QueueEventType)> {
        match peer_file {
            Some(peer_file) => {
                if source_file.is_out_of_sync(&peer_file) {
                    Some((
                        CarrierEvent::SendFile(source_file.clone(), peer_id, false),
                        QueueEventType::Signal,
                    ))
                } else {
                    None
                }
            }
            None => {
                if source_file.is_deleted() {
                    None
                } else {
                    Some((
                        CarrierEvent::SendFile(source_file.clone(), peer_id, true),
                        QueueEventType::Signal,
                    ))
                }
            }
        }
    }
}
