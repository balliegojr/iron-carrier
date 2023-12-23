use std::collections::{HashMap, HashSet};

use crate::{
    ignored_files::IgnoredFilesCache,
    node_id::NodeId,
    state_machine::{Result, State},
    states::sync::events::{DeleteFile, MoveFile, SendFileTo},
    storage::{FileInfo, Storage},
    Context,
};

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
    storages: HashMap<NodeId, Storage>,
}

impl Dispatcher {
    pub fn new(storages: HashMap<NodeId, Storage>) -> Self {
        Self { storages }
    }
}

impl State for Dispatcher {
    type Output = Vec<(FileInfo, HashSet<NodeId>)>;

    async fn execute(mut self, context: &Context) -> Result<Self::Output> {
        let mut ignored_files_cache = IgnoredFilesCache::default();
        let mut files_to_send = Vec::new();

        let mut actions = get_move_actions(&mut self.storages);
        actions.extend(get_delete_actions(&mut self.storages));
        actions.extend(get_send_actions(
            context.config.node_id_hashed,
            &mut self.storages,
        ));

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

fn get_move_actions(storages: &mut HashMap<NodeId, Storage>) -> Vec<SyncAction> {
    let nodes: HashSet<NodeId> = storages.keys().copied().collect();
    let mut actions = Vec::new();

    for current_node in nodes.iter() {
        let mut node_storage = storages.remove(current_node).unwrap();
        for moved_file in node_storage.moved.iter() {
            let deleted_file = moved_file.as_deleted_file().unwrap();
            let nodes_to_move: HashSet<NodeId> = storages
                .iter()
                .filter_map(|(node, storage)| {
                    if !storage.moved.contains(moved_file)
                        && storage.files.contains(&deleted_file)
                        && is_file_missing_or_older(&storage.files, moved_file)
                    {
                        Some(*node)
                    } else {
                        None
                    }
                })
                .collect();

            for node in nodes_to_move.iter() {
                let other_node_storage = storages.get_mut(node).unwrap();
                let mut removing = other_node_storage.files.get(&deleted_file).unwrap().clone();
                removing.path = moved_file.path.clone();
                other_node_storage.files.replace(removing);
                other_node_storage.files.remove(&deleted_file);
            }

            if !nodes_to_move.is_empty() {
                node_storage.deleted.remove(&deleted_file);
                actions.push(SyncAction::Move {
                    file: moved_file.clone(),
                    nodes: nodes_to_move,
                });
            }
        }
        node_storage.moved.clear();
        storages.insert(*current_node, node_storage);
    }

    actions
}

fn is_file_missing_or_older(files: &HashSet<FileInfo>, file: &FileInfo) -> bool {
    match files.get(file) {
        Some(existing) => existing.date_cmp(file).is_lt(),
        None => true,
    }
}

fn get_delete_actions(storages: &mut HashMap<NodeId, Storage>) -> Vec<SyncAction> {
    let mut actions = Vec::new();
    let all_deleted_files: HashSet<FileInfo> = storages
        .values_mut()
        .flat_map(|storage| storage.deleted.drain())
        .collect();

    for deleted_file in all_deleted_files {
        let has_existing_newer = storages.values().any(|storage| {
            storage
                .files
                .get(&deleted_file)
                .map(|f| deleted_file.date_cmp(f).is_lt())
                .unwrap_or_default()
        });

        if has_existing_newer {
            continue;
        }

        let nodes_to_delete: HashSet<NodeId> = storages
            .iter()
            .filter_map(|(node, storage)| {
                if storage.files.contains(&deleted_file) {
                    Some(*node)
                } else {
                    None
                }
            })
            .collect();

        for node in nodes_to_delete.iter() {
            storages.get_mut(node).unwrap().files.remove(&deleted_file);
        }

        if !nodes_to_delete.is_empty() {
            actions.push(SyncAction::Delete {
                file: deleted_file,
                nodes: nodes_to_delete,
            })
        }
    }

    actions
}

fn get_send_actions(
    local_node: NodeId,
    storages: &mut HashMap<NodeId, Storage>,
) -> Vec<SyncAction> {
    let mut actions = Vec::new();
    let all_files: HashSet<FileInfo> = storages
        .values()
        .flat_map(|storage| storage.files.iter().cloned())
        .collect();

    for file in all_files {
        let (most_recent_node, most_recent_file) = storages.iter().fold(
            (NodeId::from(0), file),
            |(most_recent_node, most_recent_file), (node_id, storage)| match storage
                .files
                .get(&most_recent_file)
            {
                Some(f) if f.date_cmp(&most_recent_file).is_ge() => (*node_id, f.clone()),
                _ => (most_recent_node, most_recent_file),
            },
        );

        let most_recent_node = match storages
            .get(&local_node)
            .unwrap()
            .files
            .get(&most_recent_file)
        {
            Some(f) if !f.is_out_of_sync(&most_recent_file) => local_node,
            _ => most_recent_node,
        };

        let nodes_out_of_sync: HashSet<NodeId> = storages
            .iter()
            .filter_map(
                |(node, storage)| match storage.files.get(&most_recent_file) {
                    Some(f) => {
                        if most_recent_file.is_out_of_sync(f) {
                            Some(*node)
                        } else {
                            None
                        }
                    }
                    None => Some(*node),
                },
            )
            .collect();

        if nodes_out_of_sync.is_empty() {
            continue;
        }

        if local_node == most_recent_node {
            actions.push(SyncAction::Send {
                file: most_recent_file,
                nodes: nodes_out_of_sync,
            });
        } else {
            actions.push(SyncAction::DelegateSend {
                delegate_to: most_recent_node,
                file: most_recent_file,
                nodes: nodes_out_of_sync,
            });
        }
    }

    actions
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
    use crate::storage::FileInfoType;

    use super::*;

    #[test]
    fn ensure_deleted_only_for_nodes_that_have_the_file() {
        let deleted = test_file("path", FileInfoType::Deleted { deleted_at: 10 });
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        let actions = get_delete_actions(&mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::Delete {
                file: deleted,
                nodes: [3.into()].into()
            }]
        );

        let expected = HashMap::from([
            (
                1.into(),
                Storage {
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    ..Default::default()
                },
            ),
        ]);

        assert_eq!(storages, expected);
    }

    #[test]
    fn ensure_deleted_not_generated_for_newer_files() {
        let deleted = test_file("path", FileInfoType::Deleted { deleted_at: 10 });
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        let actions = get_delete_actions(&mut storages);
        assert!(actions.is_empty());

        let expected = HashMap::from([
            (
                1.into(),
                Storage {
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        assert_eq!(storages, expected);
    }

    #[test]
    fn ensure_delete_is_generated_over_moved_files() {
        // One node has the file moved, but most recent has the file deleted
        let deleted = test_file("path", FileInfoType::Deleted { deleted_at: 10 });
        let moved = test_file(
            "path",
            FileInfoType::Moved {
                old_path: "old_path".into(),
                moved_at: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    moved: [moved.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        assert!(get_move_actions(&mut storages).is_empty());

        let actions = get_delete_actions(&mut storages);
        assert!(actions.is_empty());

        let expected = HashMap::from([
            (
                1.into(),
                Storage {
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    ..Default::default()
                },
            ),
        ]);

        assert_eq!(storages, expected);
    }

    #[test]
    fn ensure_send_when_file_is_delete_or_missing() {
        let deleted = test_file("path", FileInfoType::Deleted { deleted_at: 10 });
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    deleted: [deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    ..Default::default()
                },
            ),
        ]);

        assert!(get_delete_actions(&mut storages).is_empty());

        let actions = get_send_actions(1.into(), &mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::Send {
                file: existing,
                nodes: [2.into(), 3.into()].into()
            }]
        );
    }

    #[test]
    fn ensure_no_action_when_all_nodes_have_file() {
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        assert!(get_send_actions(1.into(), &mut storages).is_empty());
    }

    #[test]
    fn ensure_send_has_precedence_over_delegate_send() {
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    ..Default::default()
                },
            ),
        ]);

        let actions = get_send_actions(1.into(), &mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::Send {
                file: existing.clone(),
                nodes: [3.into()].into()
            }]
        );

        let actions = get_send_actions(2.into(), &mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::Send {
                file: existing.clone(),
                nodes: [3.into()].into()
            }]
        );
    }

    #[test]
    fn ensure_send_is_generated_after_move() {
        // One node has the file as moved, but another node has more recent Write
        // If node with moved has the origin as well, a move and a send should be generated
        let destination_newer = test_file(
            "destination",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 10,
            },
        );
        let moved_deleted = test_file("origin", FileInfoType::Deleted { deleted_at: 10 });
        let moved = test_file(
            "destination",
            FileInfoType::Moved {
                old_path: "origin".into(),
                moved_at: 10,
            },
        );
        let moved_existent = test_file(
            "origin",
            FileInfoType::Existent {
                modified_at: 10,
                created_at: 10,
                size: 10,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [destination_newer.clone()].into(),
                    deleted: [moved_deleted.clone()].into(),
                    moved: [moved.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: HashSet::from([moved_existent.clone()]),
                    ..Default::default()
                },
            ),
        ]);

        let move_actions = get_move_actions(&mut storages);
        let delete_actions = get_delete_actions(&mut storages);
        let send_actions = get_send_actions(1.into(), &mut storages);

        assert_eq!(
            move_actions,
            vec![SyncAction::Move {
                file: moved.clone(),
                nodes: [2.into()].into()
            }]
        );

        assert!(delete_actions.is_empty());
        assert_eq!(
            send_actions,
            vec![SyncAction::Send {
                file: destination_newer,
                nodes: [2.into()].into()
            }]
        );
    }

    #[test]
    fn ensure_move_if_destination_exists_but_is_older() {
        // One node has the file as moved.
        // Another node has both origin and destination, but destination is older than move

        let destination_older = test_file(
            "destination",
            FileInfoType::Existent {
                modified_at: 5,
                created_at: 5,
                size: 10,
            },
        );
        let origin = test_file(
            "origin",
            FileInfoType::Existent {
                modified_at: 10,
                created_at: 10,
                size: 10,
            },
        );

        let moved_deleted = test_file("origin", FileInfoType::Deleted { deleted_at: 10 });
        let moved = test_file(
            "destination",
            FileInfoType::Moved {
                old_path: "origin".into(),
                moved_at: 10,
            },
        );
        let moved_existent = test_file(
            "destination",
            FileInfoType::Existent {
                modified_at: 10,
                created_at: 10,
                size: 10,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    deleted: [moved_deleted.clone()].into(),
                    moved: [moved.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: [origin.clone(), destination_older.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        let move_actions = get_move_actions(&mut storages);
        let delete_actions = get_delete_actions(&mut storages);
        let send_actions = get_send_actions(1.into(), &mut storages);

        assert_eq!(
            move_actions,
            vec![SyncAction::Move {
                file: moved.clone(),
                nodes: [2.into()].into()
            }]
        );

        assert!(delete_actions.is_empty());
        assert!(send_actions.is_empty());
    }

    #[test]
    fn ensure_delegate_send_when_local_node_needs_file() {
        let existing = test_file(
            "path",
            FileInfoType::Existent {
                modified_at: 20,
                created_at: 20,
                size: 0,
            },
        );

        let mut storages = HashMap::from([
            (1.into(), Default::default()),
            (
                2.into(),
                Storage {
                    files: [existing.clone()].into(),
                    ..Default::default()
                },
            ),
            (3.into(), Default::default()),
        ]);

        let actions = get_send_actions(1.into(), &mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::DelegateSend {
                delegate_to: 2.into(),
                file: existing.clone(),
                nodes: [1.into(), 3.into()].into()
            }]
        );
    }

    #[test]
    fn ensure_move_action_only_for_nodes_that_need() {
        let origin_existent = test_file(
            "origin",
            FileInfoType::Existent {
                modified_at: 10,
                created_at: 10,
                size: 10,
            },
        );
        let moved_deleted = test_file("origin", FileInfoType::Deleted { deleted_at: 10 });
        let moved = test_file(
            "destination",
            FileInfoType::Moved {
                old_path: "origin".into(),
                moved_at: 10,
            },
        );
        let moved_existent = test_file(
            "destination",
            FileInfoType::Existent {
                modified_at: 10,
                created_at: 10,
                size: 10,
            },
        );

        let mut storages = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    moved: [moved.clone()].into(),
                    deleted: [moved_deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    moved: [moved.clone()].into(),
                    deleted: [moved_deleted.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    files: [origin_existent.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        let actions = get_move_actions(&mut storages);
        assert_eq!(
            actions,
            vec![SyncAction::Move {
                file: moved.clone(),
                nodes: HashSet::from([3.into()])
            }]
        );

        assert!(get_delete_actions(&mut storages).is_empty());

        let expected: HashMap<NodeId, Storage> = HashMap::from([
            (
                1.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                2.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    ..Default::default()
                },
            ),
            (
                3.into(),
                Storage {
                    files: [moved_existent.clone()].into(),
                    ..Default::default()
                },
            ),
        ]);

        assert_eq!(storages, expected);
    }

    fn test_file(path: &str, info_type: FileInfoType) -> FileInfo {
        FileInfo {
            storage: "".to_string(),
            path: path.into(),
            info_type,
            permissions: 0,
        }
    }
}
