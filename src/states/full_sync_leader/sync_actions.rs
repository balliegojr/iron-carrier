use std::collections::HashMap;

use crate::storage::{FileInfo, FileInfoType};

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
    let mut node_out_of_sync: Vec<u64> = peers
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

    node_out_of_sync.sort();
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
        nodes: Vec<u64>,
    },
    Send {
        file: FileInfo,
        nodes: Vec<u64>,
    },
    DelegateSend {
        delegate_to: u64,
        file: FileInfo,
        nodes: Vec<u64>,
    },
    Move {
        file: FileInfo,
        nodes: Vec<u64>,
    },
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
                size: 0,
            }),
        );

        // File deletion should generate actions only for nodes that have the file, all the cases
        // bellow will generate the deletion action for node 1

        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, vec![1]),
            _ => panic!(),
        }

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, vec![1]),
            _ => panic!(),
        }

        match get_file_sync_action(1, &[0], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, vec![1]),
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
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, vec![1]),
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
                size: 0,
            }),
        );
        files.insert(1, with_file_type(FileInfoType::Deleted { deleted_at: 0 }));

        // Most recent file exists for local node, every other have the file as deleted or no
        // records of the file, all other nodes should receive the file

        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, vec![1, 2, 3]),
            _ => panic!(),
        }

        match get_file_sync_action(0, &[1], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, vec![1]),
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
                size: 0,
            }),
        );
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0, &[1,], files.clone()).is_none());
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, vec![2, 3]),
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, vec![2, 3]);
            }
            _ => panic!(),
        }

        // One node has the file as moved, but another node has more recent Write
        let mut files = HashMap::default();
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
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
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, vec![1]),
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
                assert_eq!(nodes, vec![0, 2, 3]);
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
                assert_eq!(nodes, vec![0]);
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
                size: 0,
            }),
        );
        files.insert(
            0,
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0, &[1,], files.clone()).is_none());
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, vec![2, 3]),
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1);
                assert_eq!(nodes, vec![2, 3]);
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
                size: 0,
            }),
        );
        files.insert(2, with_file_type(FileInfoType::Deleted { deleted_at: 0 }));

        // File moving should generate actions only for nodes that do not have the file
        match get_file_sync_action(0, &[1, 2, 3], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, vec![1, 2, 3]),
            _ => panic!(),
        }
        match get_file_sync_action(2, &[0, 1, 3], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, vec![1, 2, 3]),
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
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, vec![1]),
            _ => panic!(),
        }
    }
}
