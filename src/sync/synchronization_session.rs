use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use crate::{conn::CommandDispatcher, storage::FileInfo};

use super::{FileHandlerEvent, SyncEvent};

pub(crate) struct SynchronizationState {
    node_id: String,
    storage_state: HashMap<String, HashSet<String>>,
    current_storage_index: HashMap<String, Vec<FileInfo>>,
    current_storage_peers: HashSet<String>,
    expected_state_replies: usize,
    commands: CommandDispatcher,
}

impl SynchronizationState {
    pub fn new(node_id: String, commands: CommandDispatcher) -> Self {
        Self {
            node_id,
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

    pub fn get_next_storage(&mut self) -> Option<String> {
        if self.storage_state.is_empty() {
            return None;
        }

        let next_storage = self.storage_state.iter().next();

        let storage = match next_storage {
            Some((storage, _)) => storage.clone(),
            None => return None,
        };

        let peers = self.storage_state.remove(&storage).unwrap();
        self.current_storage_peers = peers;
        Some(storage)
    }

    pub fn set_storage_index(&mut self, origin: String, index: Vec<FileInfo>) -> bool {
        self.current_storage_index.insert(origin, index);
        self.current_storage_peers.len() == self.current_storage_index.len() - 1
    }

    pub fn build_event_queue(&mut self) {
        let consolidated_index = self.get_consolidated_index();
        consolidated_index
            .into_iter()
            .for_each(|(_, v)| self.execute_actions_for_file(v));

        self.current_storage_peers.clear();

        if !self.storage_state.is_empty() {
            self.commands.enqueue(SyncEvent::SyncNextStorage);
        } else {
            self.commands.enqueue(SyncEvent::EndSync);
        }
    }

    fn get_consolidated_index(&mut self) -> HashMap<PathBuf, HashMap<String, FileInfo>> {
        let mut consolidated_index: HashMap<PathBuf, HashMap<String, FileInfo>> = HashMap::new();
        for (origin, index) in std::mem::take(&mut self.current_storage_index) {
            for file in index {
                consolidated_index
                    .entry(file.path.clone())
                    .or_default()
                    .insert(origin.clone(), file);
            }
        }

        consolidated_index
    }
    /// Execute the necessary actions for this file
    /// Most of the actions here are propagated to every other peer, the only exception is when we need to request the file for this peer
    /// For this scenario, we trigger a secondary sync to propagate the file to other peers
    fn execute_actions_for_file(&self, mut files: HashMap<String, FileInfo>) {
        let local_file = files.remove(&self.node_id);

        let most_recent = files
            .iter()
            .max_by(|(_, a), (_, b)| a.date_cmp(b))
            .map(|(peer, _)| peer.to_owned());

        match (local_file, most_recent) {
            (None, Some(peer)) => {
                let peer_file = files.remove(&peer).unwrap();
                if peer_file.is_deleted() {
                    // There is no local file, se propagate delete to where other peers
                    self.propagate_delete(files);
                } else {
                    // There is no local file, we request ir from peer, and then propagate to other peers
                    self.commands.enqueue(FileHandlerEvent::RequestFile(
                        peer_file.clone(),
                        peer.clone(),
                        true,
                    ));
                    self.propagate_file_to_other_peers(peer_file, peer, files);
                }
            }
            (Some(local_file), None) => {
                if !local_file.is_deleted() {
                    // There is a local file, and no other peer has it, we need to propagate
                    for peer in &self.current_storage_peers {
                        self.commands.enqueue(FileHandlerEvent::SendFile(
                            local_file.clone(),
                            peer.to_string(),
                            true,
                        ));
                    }
                }
            }
            (Some(local_file), Some(peer)) => {
                let ord = {
                    let peer_file = files.get(&peer).unwrap();
                    local_file.date_cmp(peer_file)
                };

                // Both files exist, we check for the most recent one
                match ord {
                    std::cmp::Ordering::Less => {
                        // Peer file is more recent than local file
                        let peer_file = files.remove(&peer).unwrap();
                        match (local_file.is_deleted(), peer_file.is_deleted()) {
                            (is_local_deleted, true) => {
                                // Peer file is deleted, we delete local file if needed and propagate deletion to other peers
                                if !is_local_deleted {
                                    self.commands.now(FileHandlerEvent::DeleteFile(local_file));
                                }
                                self.propagate_delete(files)
                            }
                            // We request the file from peer, and then propagate to other peers
                            (is_new_file, false) => {
                                self.commands.enqueue(FileHandlerEvent::RequestFile(
                                    peer_file.clone(),
                                    peer.clone(),
                                    is_new_file,
                                ));
                                self.propagate_file_to_other_peers(peer_file, peer, files);
                            }
                        }
                    }
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                        // local file is the most recent, we propagate its state to every peer
                        if local_file.is_deleted() {
                            // delete on every peer that has the file
                            self.propagate_delete(files);
                        } else {
                            // Create or update the file on every other peer
                            for (peer, is_new_file) in
                                self.current_storage_peers.iter().filter_map(|peer| {
                                    match files.remove(peer) {
                                        Some(peer_file) => {
                                            if local_file.is_out_of_sync(&peer_file) {
                                                Some((peer, false))
                                            } else {
                                                None
                                            }
                                        }
                                        None => Some((peer, true)),
                                    }
                                })
                            {
                                self.commands.enqueue(FileHandlerEvent::SendFile(
                                    local_file.clone(),
                                    peer.clone(),
                                    is_new_file,
                                ))
                            }
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn propagate_delete(&self, files: HashMap<String, FileInfo>) {
        for (peer, peer_file) in files
            .into_iter()
            .filter(|(_, peer_file)| !peer_file.is_deleted())
        {
            self.commands
                .to(FileHandlerEvent::DeleteFile(peer_file), &peer);
        }
    }

    fn propagate_file_to_other_peers(
        &self,
        file: FileInfo,
        except: String,
        files: HashMap<String, FileInfo>,
    ) {
        let peers = self.current_storage_peers.iter().filter(|p| {
            except.ne(p.as_str())
                && files
                    .get(p.as_str())
                    .map(|f| f.is_out_of_sync(&file))
                    .unwrap_or(true)
        });
        for peer in peers {
            let is_new = files.contains_key(peer);
            self.commands.enqueue(FileHandlerEvent::SendFile(
                file.clone(),
                peer.to_string(),
                is_new,
            ));
        }
    }
}
