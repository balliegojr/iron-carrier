use std::collections::{HashMap, HashSet, LinkedList};

use crate::fs::FileInfo;

use super::{CarrierEvent, Origin, QueueEventType};

#[derive(Debug)]
pub(crate) struct SynchronizationSession {
    storages: Vec<String>,
    current_storage_index: HashMap<Origin, Option<Vec<FileInfo>>>,
}

impl SynchronizationSession {
    pub fn new(storages: Vec<String>) -> Self {
        Self {
            storages,
            current_storage_index: HashMap::new(),
        }
    }

    pub fn get_next_storage(&mut self) -> Option<String> {
        self.storages.pop()
    }

    pub fn set_storage_index(&mut self, origin: Origin, index: Option<Vec<FileInfo>>) {
        self.current_storage_index.insert(origin, index);
    }

    pub fn have_index_for_peers(&self, peers: &[u64]) -> bool {
        peers
            .iter()
            .all(|p| self.current_storage_index.contains_key(&Origin::Peer(*p)))
    }

    pub fn get_event_queue(&mut self, peers: &[u64]) -> LinkedList<(CarrierEvent, QueueEventType)> {
        let consolidated_index = self.get_consolidated_index(peers);
        let actions = consolidated_index
            .iter()
            .flat_map(|(_, v)| self.convert_index_to_events(v, peers))
            .collect();

        actions
    }

    fn get_consolidated_index(
        &self,
        peers: &[u64],
    ) -> HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> {
        let mut consolidated_index: HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> =
            HashMap::new();
        for (origin, index) in &self.current_storage_index {
            if let Some(index) = index {
                for file in index {
                    consolidated_index
                        .entry(&file)
                        .or_default()
                        .insert(origin, file);
                }
            }
        }
        let mut all_origins: HashSet<Origin> =
            peers.iter().map(|peer| Origin::Peer(*peer)).collect();
        all_origins.insert(Origin::Initiator);

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
        peers: &[u64],
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
                actions.extend(peers.iter().filter_map(|peer_id| {
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
                    peers
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
