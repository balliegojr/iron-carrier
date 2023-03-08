use std::collections::{HashMap, HashSet};

use crate::{
    ignored_files::IgnoredFilesCache,
    network_events::Synchronization,
    state_machine::State,
    storage::{FileInfo, FileInfoType},
    IronCarrierError, SharedState,
};

use super::matching_files::MatchedFilesIter;

pub fn get_file_sync_action(
    local_node: u64,
    peers: &[u64],
    mut file: HashMap<u64, FileInfo>,
) -> Option<SyncAction> {
    if peers.is_empty() {
        return None;
    }

    let node_with_most_recent_file = file
        .iter()
        .max_by(|(_, a), (_, b)| a.date_cmp(b))
        .map(|(peer, _)| *peer)
        .unwrap();

    let is_local_the_most_recent = node_with_most_recent_file == local_node;

    let most_recent_file = file.remove(&node_with_most_recent_file).unwrap();
    let node_out_of_sync: HashSet<u64> = peers
        .iter()
        .chain(&[local_node])
        .filter(|peer_id| {
            **peer_id != node_with_most_recent_file
                && file
                    .get(peer_id)
                    .map(|f| most_recent_file.is_out_of_sync(f))
                    .unwrap_or_else(|| most_recent_file.is_existent())
        })
        .copied()
        .collect();

    if node_out_of_sync.is_empty() {
        return None;
    }

    match most_recent_file.info_type {
        FileInfoType::Existent { .. } if is_local_the_most_recent => Some(SyncAction::Send {
            file: most_recent_file,
            nodes: node_out_of_sync,
        }),
        FileInfoType::Existent { .. } => Some(SyncAction::DelegateSend {
            delegate_to: node_with_most_recent_file,
            file: most_recent_file,
            nodes: node_out_of_sync,
        }),
        FileInfoType::Deleted { .. } => Some(SyncAction::Delete {
            file: most_recent_file,
            nodes: node_out_of_sync,
        }),
        FileInfoType::Moved { .. } => Some(SyncAction::Move {
            file: most_recent_file,
            nodes: node_out_of_sync,
        }),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SyncAction {
    Delete {
        file: FileInfo,
        nodes: HashSet<u64>,
    },
    Send {
        file: FileInfo,
        nodes: HashSet<u64>,
    },
    DelegateSend {
        delegate_to: u64,
        file: FileInfo,
        nodes: HashSet<u64>,
    },
    Move {
        file: FileInfo,
        nodes: HashSet<u64>,
    },
}

#[derive(Debug)]
pub struct DispatchActions {
    peers: Vec<u64>,
    matched_files_iter: MatchedFilesIter,
}

impl DispatchActions {
    pub fn new(peers: Vec<u64>, matched_files_iter: MatchedFilesIter) -> Self {
        Self {
            peers,
            matched_files_iter,
        }
    }
}

impl State for DispatchActions {
    type Output = (Vec<(FileInfo, HashSet<u64>)>, HashSet<u64>);

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let actions = self.matched_files_iter.filter_map(|file| {
            get_file_sync_action(shared_state.config.node_id_hashed, &self.peers, file)
        });

        let mut files_to_send = Vec::default();
        let mut file_transfer_required = false;
        let mut ignored_files_cache = IgnoredFilesCache::default();

        for action in actions {
            if let Err(err) = execute_action(
                action,
                shared_state,
                &mut files_to_send,
                &mut file_transfer_required,
                &mut ignored_files_cache,
            )
            .await
            {
                log::error!("failed to execute action {err}");
            }
        }

        if !file_transfer_required {
            return Err(IronCarrierError::AbortExecution.into());
        }

        shared_state
            .connection_handler
            .broadcast_to(
                Synchronization::StartTransferingFiles.into(),
                self.peers.iter(),
            )
            .await?;

        Ok((
            files_to_send,
            HashSet::from_iter(self.peers.iter().copied()),
        ))
    }
}

pub async fn execute_action(
    action: SyncAction,
    shared_state: &SharedState,
    files_to_send: &mut Vec<(FileInfo, HashSet<u64>)>,
    file_transfer_required: &mut bool,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<()> {
    match action {
        SyncAction::Delete { file, nodes } => {
            if nodes.contains(&shared_state.config.node_id_hashed) {
                crate::storage::file_operations::delete_file(
                    shared_state.config,
                    shared_state.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            log::trace!("Requesting {nodes:?} to delete {:?} ", file.path);
            shared_state
                .connection_handler
                .broadcast_to(
                    Synchronization::DeleteFile { file: file.clone() }.into(),
                    nodes.iter(),
                )
                .await?;
        }
        SyncAction::Move { file, nodes } => {
            if nodes.contains(&shared_state.config.node_id_hashed) {
                crate::storage::file_operations::move_file(
                    shared_state.config,
                    shared_state.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            shared_state
                .connection_handler
                .broadcast_to(
                    Synchronization::MoveFile { file: file.clone() }.into(),
                    nodes.iter(),
                )
                .await?;
        }
        SyncAction::Send { file, nodes } => {
            *file_transfer_required = true;
            files_to_send.push((file, nodes));
        }
        SyncAction::DelegateSend {
            delegate_to,
            file,
            nodes,
        } => {
            *file_transfer_required = true;
            log::trace!(
                "Requesting {delegate_to} to send {:?} to {nodes:?}",
                file.path
            );

            shared_state
                .connection_handler
                .send_to(
                    Synchronization::SendFileTo { file, nodes }.into(),
                    delegate_to,
                )
                .await?
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn with_file_type(info_type: FileInfoType) -> FileInfo {
        FileInfo {
            storage: "".to_string(),
            path: "path".into(),
            info_type,
            permissions: 0,
        }
    }
    #[test]
    fn test_generate_delete_actions() {
        let mut files = HashMap::default();
        files.insert(0, with_file_type(FileInfoType::Deleted { deleted_at: 10 }));
        files.insert(
            1,
            with_file_type(FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            }),
        );

        // File deletion should generate actions only for nodes that have the file, all the cases
        // bellow will generate the deletion action for node 1

        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }

        match get_file_sync_action(1, &[0], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0, &[], files).is_none());

        // All nodes have the file registered as deleted, no action should be performed
        let mut files = HashMap::default();
        files.insert(0, with_file_type(FileInfoType::Deleted { deleted_at: 10 }));
        files.insert(1, with_file_type(FileInfoType::Deleted { deleted_at: 10 }));

        assert!(get_file_sync_action(0, &[1,], files.clone()).is_none());
        assert!(get_file_sync_action(0, &[1, 2, 3], files).is_none());

        // One node has the file moved, but most recent has the file deleted
        let mut files = HashMap::default();
        files.insert(0, with_file_type(FileInfoType::Deleted { deleted_at: 10 }));
        files.insert(
            1,
            with_file_type(FileInfoType::Moved {
                old_path: "olde_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }
    }

    #[test]
    fn test_generate_send_actions() {
        let mut files = HashMap::default();
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(1, with_file_type(FileInfoType::Deleted { deleted_at: 0 }));

        // Most recent file exists for local node, every other have the file as deleted or no
        // records of the file, all other nodes should receive the file

        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([1, 2, 3])),
            _ => panic!(),
        }

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0, &[], files).is_none());

        // Two nodes have the file, the others should receive the file
        let mut files = HashMap::default();
        files.insert(
            1,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0, &[1,], files.clone()).is_none());
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([2, 3])),
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, HashSet::from([2, 3]));
            }
            _ => panic!(),
        }

        // One node has the file as moved, but another node has more recent Write
        let mut files = HashMap::default();
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            1,
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }
    }

    #[test]
    fn test_generate_delegate_send_actions() {
        let mut files = HashMap::default();
        files.insert(
            1,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(0, with_file_type(FileInfoType::Deleted { deleted_at: 0 }));

        // Most recent file exists for some other node, local node have the file deleted, other
        // nodes have no records of the file, node 1 should send the files for all other nodes,
        // including local node
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, HashSet::from([0, 2, 3]));
            }
            _ => panic!(),
        }

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, HashSet::from([0]));
            }
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0, &[], files).is_none());

        // Two nodes have the file, the others should receive the file
        let mut files = HashMap::default();
        files.insert(
            1,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0, &[1,], files.clone()).is_none());
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([2, 3])),
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, HashSet::from([2, 3]));
            }
            _ => panic!(),
        }
    }

    #[test]
    pub fn test_generate_move_action() {
        let mut files = HashMap::default();
        files.insert(
            0,
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 10,
            }),
        );
        // FIXME: this node should not appear in the action
        // It is necessary to add a special case to handle this scenario
        files.insert(
            1,
            with_file_type(FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(2, with_file_type(FileInfoType::Deleted { deleted_at: 0 }));

        // File moving should generate actions only for nodes that do not have the file
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, HashSet::from([1, 2, 3])),
            _ => panic!(),
        }
        match get_file_sync_action(2, &[0, 1, 3], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, HashSet::from([1, 2, 3])),
            _ => panic!(),
        }

        files.insert(
            1,
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 0,
            }),
        );

        assert!(get_file_sync_action(0, &[1], files.clone()).is_none());

        files.insert(
            1,
            with_file_type(FileInfoType::Moved {
                old_path: "other_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, HashSet::from([1])),
            _ => panic!(),
        }
    }
}
