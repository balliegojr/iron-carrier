use std::collections::{HashMap, HashSet};

use crate::{
    ignored_files::IgnoredFilesCache,
    node_id::NodeId,
    state_machine::State,
    states::sync::events::{DeleteFile, MoveFile, SendFileTo},
    storage::{FileInfo, FileInfoType},
    SharedState,
};

use super::files_matcher::MatchedFilesIter;

/// Get the [SyncAction] for the given `file`
pub fn get_file_sync_action(
    local_node: NodeId,
    nodes: &[NodeId],
    mut file: HashMap<NodeId, FileInfo>,
) -> Option<SyncAction> {
    if nodes.is_empty() {
        return None;
    }

    let node_with_most_recent_file = file
        .iter()
        .max_by(|(_, a), (_, b)| a.date_cmp(b))
        .map(|(node, _)| *node)
        .unwrap();

    let is_local_the_most_recent = node_with_most_recent_file == local_node;

    let most_recent_file = file.remove(&node_with_most_recent_file).unwrap();
    let node_out_of_sync: HashSet<NodeId> = nodes
        .iter()
        .chain(&[local_node])
        .filter(|node_id| {
            **node_id != node_with_most_recent_file
                && file
                    .get(node_id)
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

/// Possible actions necessary to synchronize a file
#[derive(Debug, PartialEq, Eq)]
pub enum SyncAction {
    Delete {
        file: FileInfo,
        nodes: HashSet<NodeId>,
    },
    Send {
        file: FileInfo,
        nodes: HashSet<NodeId>,
    },
    DelegateSend {
        delegate_to: NodeId,
        file: FileInfo,
        nodes: HashSet<NodeId>,
    },
    Move {
        file: FileInfo,
        nodes: HashSet<NodeId>,
    },
}

#[derive(Debug)]
pub struct Dispatcher {
    nodes: Vec<NodeId>,
    matched_files_iter: MatchedFilesIter,
}

impl Dispatcher {
    pub fn new(nodes: Vec<NodeId>, matched_files_iter: MatchedFilesIter) -> Self {
        Self {
            nodes,
            matched_files_iter,
        }
    }
}

impl State for Dispatcher {
    type Output = Vec<(FileInfo, HashSet<NodeId>)>;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let actions = self.matched_files_iter.filter_map(|file| {
            get_file_sync_action(shared_state.config.node_id_hashed, &self.nodes, file)
        });

        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut files_to_send = Vec::new();
        let mut receive_files_from = HashMap::new();

        for action in actions {
            if let Err(err) = execute_action(
                action,
                shared_state,
                &mut files_to_send,
                &mut receive_files_from,
                &mut ignored_files_cache,
            )
            .await
            {
                log::error!("failed to execute action {err}");
            }
        }

        Ok(files_to_send)
    }
}
/// Execute the given `action`
///
/// `Delete` and `Move` actions are executed immediately (or sent for all required nodes)
/// `Send` is actions just add the file `files_to_send`, these actions are handled by the file
/// transfer module
/// `DeletegateSend` are sent to other nodes, so they can transfer their files to nodes that need
/// it
pub async fn execute_action(
    action: SyncAction,
    shared_state: &SharedState,
    files_to_send: &mut Vec<(FileInfo, HashSet<NodeId>)>,
    receive_files_from: &mut HashMap<NodeId, HashSet<NodeId>>,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<()> {
    match action {
        SyncAction::Delete { file, nodes } => {
            if nodes.contains(&shared_state.config.node_id_hashed) {
                crate::storage::file_operations::delete_file(
                    shared_state.config,
                    &shared_state.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            log::info!("Delete {:?} on {nodes:?}", file.path);
            shared_state
                .rpc
                .multi_call(DeleteFile { file }, nodes)
                .ack()
                .await?;
        }
        SyncAction::Move { file, nodes } => {
            if nodes.contains(&shared_state.config.node_id_hashed) {
                crate::storage::file_operations::move_file(
                    shared_state.config,
                    &shared_state.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            log::info!("Move {:?} on {nodes:?}", file.path);
            shared_state
                .rpc
                .multi_call(MoveFile { file }, nodes)
                .ack()
                .await?;
        }
        SyncAction::Send { file, nodes } => {
            for node in nodes.iter() {
                receive_files_from
                    .entry(*node)
                    .or_default()
                    .insert(shared_state.config.node_id_hashed);
            }
            files_to_send.push((file, nodes));
        }
        SyncAction::DelegateSend {
            delegate_to,
            file,
            nodes,
        } => {
            log::trace!(
                "Requesting {delegate_to} to send {:?} to {nodes:?}",
                file.path
            );

            for node in nodes.iter() {
                receive_files_from
                    .entry(*node)
                    .or_default()
                    .insert(delegate_to);
            }

            shared_state
                .rpc
                .call(SendFileTo { file, nodes }, delegate_to)
                .ack()
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
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
        );
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            }),
        );

        // File deletion should generate actions only for nodes that have the file, all the cases
        // bellow will generate the deletion action for node 1

        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }

        match get_file_sync_action(1.into(), &[0.into()], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0.into(), &[], files).is_none());

        // All nodes have the file registered as deleted, no action should be performed
        let mut files = HashMap::default();
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
        );
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
        );

        assert!(get_file_sync_action(0.into(), &[1.into(),], files.clone()).is_none());
        assert!(get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files).is_none());

        // One node has the file moved, but most recent has the file deleted
        let mut files = HashMap::default();
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
        );
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Moved {
                old_path: "olde_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::Delete { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }
    }

    #[test]
    fn test_generate_send_actions() {
        let mut files = HashMap::default();
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
        );

        // Most recent file exists for local node, every other have the file as deleted or no
        // records of the file, all other nodes should receive the file

        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Send { file: _, nodes } => {
                assert_eq!(nodes, HashSet::from([1.into(), 2.into(), 3.into()]))
            }
            _ => panic!(),
        }

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0.into(), &[], files).is_none());

        // Two nodes have the file, the others should receive the file
        let mut files = HashMap::default();
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0.into(), &[1.into(),], files.clone()).is_none());
        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Send { file: _, nodes } => {
                assert_eq!(nodes, HashSet::from([2.into(), 3.into()]))
            }
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1.into());
                assert_eq!(nodes, HashSet::from([2.into(), 3.into()]));
            }
            _ => panic!(),
        }

        // One node has the file as moved, but another node has more recent Write
        let mut files = HashMap::default();
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::Send { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }
    }

    #[test]
    fn test_generate_delegate_send_actions() {
        let mut files = HashMap::default();
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
        );

        // Most recent file exists for some other node, local node have the file deleted, other
        // nodes have no records of the file, node 1 should send the files for all other nodes,
        // including local node
        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1.into());
                assert_eq!(nodes, HashSet::from([0.into(), 2.into(), 3.into()]));
            }
            _ => panic!(),
        }

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1.into());
                assert_eq!(nodes, HashSet::from([0.into()]));
            }
            _ => panic!(),
        }

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0.into(), &[], files).is_none());

        // Two nodes have the file, the others should receive the file
        let mut files = HashMap::default();
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 10,
                created_at: 0,
                size: 0,
            }),
        );

        assert!(get_file_sync_action(0.into(), &[1.into(),], files.clone()).is_none());
        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Send { file: _, nodes } => {
                assert_eq!(nodes, HashSet::from([2.into(), 3.into()]))
            }
            SyncAction::DelegateSend {
                file: _,
                nodes,
                delegate_to,
            } => {
                assert_eq!(delegate_to, 1.into());
                assert_eq!(nodes, HashSet::from([2.into(), 3.into()]));
            }
            _ => panic!(),
        }
    }

    #[test]
    pub fn test_generate_move_action() {
        let mut files = HashMap::default();
        files.insert(
            0.into(),
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 10,
            }),
        );
        // FIXME: this node should not appear in the action
        // It is necessary to add a special case to handle this scenario
        files.insert(
            1.into(),
            with_file_type(FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            }),
        );
        files.insert(
            2.into(),
            with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
        );

        // File moving should generate actions only for nodes that do not have the file
        match get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Move { file: _, nodes } => {
                assert_eq!(nodes, HashSet::from([1.into(), 2.into(), 3.into()]))
            }
            _ => panic!(),
        }
        match get_file_sync_action(2.into(), &[0.into(), 1.into(), 3.into()], files.clone())
            .unwrap()
        {
            SyncAction::Move { file: _, nodes } => {
                assert_eq!(nodes, HashSet::from([1.into(), 2.into(), 3.into()]))
            }
            _ => panic!(),
        }

        files.insert(
            1.into(),
            with_file_type(FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 0,
            }),
        );

        assert!(get_file_sync_action(0.into(), &[1.into()], files.clone()).is_none());

        files.insert(
            1.into(),
            with_file_type(FileInfoType::Moved {
                old_path: "other_path".into(),
                moved_at: 0,
            }),
        );

        match get_file_sync_action(0.into(), &[1.into()], files.clone()).unwrap() {
            SyncAction::Move { file: _, nodes } => assert_eq!(nodes, HashSet::from([1.into()])),
            _ => panic!(),
        }
    }
}
