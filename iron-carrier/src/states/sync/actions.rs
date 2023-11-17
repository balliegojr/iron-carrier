use std::collections::{HashMap, HashSet};

use crate::{
    ignored_files::IgnoredFilesCache,
    node_id::NodeId,
    state_machine::{Result, State},
    states::sync::events::{DeleteFile, MoveFile, SendFileTo},
    storage::{FileInfo, FileInfoType},
    Context,
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

    let most_recent_file = file.remove(&node_with_most_recent_file).unwrap();
    let is_local_node_in_sync = node_with_most_recent_file == local_node
        || file
            .get(&local_node)
            .map(|f| !most_recent_file.is_out_of_sync(f))
            .unwrap_or_default();

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
        FileInfoType::Existent { .. } if is_local_node_in_sync => Some(SyncAction::Send {
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
        delegate_to: NodeId, //FIXME: include all nodes that has the file in sync
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

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        let actions = self.matched_files_iter.filter_map(|file| {
            get_file_sync_action(context.config.node_id_hashed, &self.nodes, file)
        });

        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut files_to_send = Vec::new();

        for action in actions {
            if let Err(err) = execute_action(
                action,
                context,
                &mut files_to_send,
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
    context: &Context,
    files_to_send: &mut Vec<(FileInfo, HashSet<NodeId>)>,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<()> {
    match action {
        SyncAction::Delete { file, mut nodes } => {
            if nodes.remove(&context.config.node_id_hashed) {
                crate::storage::file_operations::delete_file(
                    context.config,
                    &context.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            if !nodes.is_empty() {
                log::info!("Delete {:?} on {nodes:?}", file.path);
                context
                    .rpc
                    .multi_call(DeleteFile { file }, nodes)
                    .ack()
                    .await?;
            }
        }
        SyncAction::Move { file, mut nodes } => {
            if nodes.remove(&context.config.node_id_hashed) {
                crate::storage::file_operations::move_file(
                    context.config,
                    &context.transaction_log,
                    &file,
                    ignored_files_cache,
                )
                .await?;
            }

            if !nodes.is_empty() {
                log::info!("Move {:?} on {nodes:?}", file.path);
                context
                    .rpc
                    .multi_call(MoveFile { file }, nodes)
                    .ack()
                    .await?;
            }
        }
        SyncAction::Send { file, nodes } => {
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

            context
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

    #[test]
    fn ensure_delete_only_for_nodes_that_have_the_file() {
        // File deletion should generate actions only for nodes that have the file, all the cases
        // bellow will generate the deletion action for node 1

        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                0.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
            ),
            (
                1.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 0,
                    created_at: 0,
                    size: 0,
                }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone()),
            Some(SyncAction::Delete {
                file: with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
                nodes: HashSet::from([1.into()])
            }),
        );

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into()], files.clone()),
            Some(SyncAction::Delete {
                file: with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
                nodes: HashSet::from([1.into()])
            }),
        );

        assert_eq!(
            get_file_sync_action(1.into(), &[0.into()], files.clone()),
            Some(SyncAction::Delete {
                file: with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
                nodes: HashSet::from([1.into()])
            }),
        );

        assert!(get_file_sync_action(0.into(), &[], files).is_none());
    }

    #[test]
    fn ensure_delete_not_generated_when_nodes_already_deleted() {
        // All nodes have the file registered as deleted, no action should be performed
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                0.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
            ),
            (
                1.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
            ),
        ]);

        assert!(get_file_sync_action(0.into(), &[1.into(),], files.clone()).is_none());
        assert!(get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files).is_none());
    }

    #[test]
    fn ensure_delete_is_generated_over_moved_files() {
        // One node has the file moved, but most recent has the file deleted
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                0.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
            ),
            (
                1.into(),
                with_file_type(FileInfoType::Moved {
                    old_path: "olde_path".into(),
                    moved_at: 0,
                }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into()], files.clone()),
            Some(SyncAction::Delete {
                nodes: HashSet::from([1.into()]),
                file: with_file_type(FileInfoType::Deleted { deleted_at: 10 }),
            })
        );
    }

    #[test]
    fn ensure_send_when_file_is_delete_or_missing() {
        // Most recent file exists for local node, every other have the file as deleted or no
        // records of the file, all other nodes should receive the file
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                0.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                1.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone()),
            Some(SyncAction::Send {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([1.into(), 2.into(), 3.into()])
            })
        );

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into()], files.clone()),
            Some(SyncAction::Send {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([1.into()])
            })
        );

        assert!(get_file_sync_action(0.into(), &[], files).is_none());
    }

    #[test]
    fn ensure_no_action_when_all_nodes_have_file() {
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                1.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                0.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
        ]);

        assert!(get_file_sync_action(0.into(), &[1.into(),], files.clone()).is_none());
    }

    #[test]
    fn ensure_send_has_precedence_over_delegate_send() {
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                1.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                0.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone()),
            Some(SyncAction::Send {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([2.into(), 3.into()])
            })
        );

        assert_eq!(
            get_file_sync_action(1.into(), &[1.into(), 2.into(), 3.into()], files.clone()),
            Some(SyncAction::Send {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([2.into(), 3.into()])
            })
        );
    }

    #[test]
    fn ensure_send_is_generated_for_moved_files() {
        // One node has the file as moved, but another node has more recent Write
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                0.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                1.into(),
                with_file_type(FileInfoType::Moved {
                    old_path: "old_path".into(),
                    moved_at: 0,
                }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into()], files.clone()),
            Some(SyncAction::Send {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([1.into()])
            })
        );
    }

    #[test]
    fn ensure_delegate_send_when_local_node_needs_file() {
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                1.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                0.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into()], files.clone()),
            Some(SyncAction::DelegateSend {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([0.into()]),
                delegate_to: 1.into(),
            })
        );

        assert_eq!(
            get_file_sync_action(3.into(), &[1.into()], files.clone()),
            Some(SyncAction::DelegateSend {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([3.into()]),
                delegate_to: 1.into(),
            })
        );
    }

    #[test]
    fn ensure_delegate_send_includes_other_nodes() {
        let files: HashMap<NodeId, FileInfo> = HashMap::from([
            (
                1.into(),
                with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
            ),
            (
                0.into(),
                with_file_type(FileInfoType::Deleted { deleted_at: 0 }),
            ),
        ]);

        assert_eq!(
            get_file_sync_action(0.into(), &[1.into(), 2.into(), 3.into()], files.clone()),
            Some(SyncAction::DelegateSend {
                file: with_file_type(FileInfoType::Existent {
                    modified_at: 10,
                    created_at: 0,
                    size: 0,
                }),
                nodes: HashSet::from([0.into(), 2.into(), 3.into()]),
                delegate_to: 1.into(),
            })
        );

        // This should not happen, but lets guarantee no action is generated where there are no
        // peers to delete the file
        assert!(get_file_sync_action(0.into(), &[], files).is_none());
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

    fn with_file_type(info_type: FileInfoType) -> FileInfo {
        FileInfo {
            storage: "".to_string(),
            path: "path".into(),
            info_type,
            permissions: 0,
        }
    }
}
