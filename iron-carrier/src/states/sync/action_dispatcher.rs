use std::collections::{HashMap, HashSet};

use crate::{
    Context,
    file_transfer::SyncFile,
    ignored_files::IgnoredFilesCache,
    node_id::NodeId,
    relative_path::RelativePathBuf,
    state_machine::{Result, State},
    states::sync::events::{DeleteFile, MoveFile, SendFileTo},
    storage::{
        Storage,
        storage_tree::{DeletedFileInfo, FileId, MovedFileInfo, StorageFile, StorageTree},
    },
};

#[derive(Debug)]
pub struct ActionDispatcher {
    storages: HashMap<NodeId, Storage>,
}

impl ActionDispatcher {
    pub fn new(storages: HashMap<NodeId, Storage>) -> Self {
        Self { storages }
    }
}

impl State for ActionDispatcher {
    type Output = Vec<(SyncFile, HashSet<NodeId>)>;

    async fn execute(mut self, context: &Context) -> Result<Self::Output> {
        let mut ignored_files_cache = IgnoredFilesCache::default();

        buid_move_actions(context, &mut ignored_files_cache, &mut self.storages).await?;
        build_delete_actions(context, &mut ignored_files_cache, &mut self.storages).await?;
        build_send_actions(context, &mut self.storages)
            .await
            .map_err(crate::StateMachineError::from)
    }
}

async fn buid_move_actions(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storages: &mut HashMap<NodeId, Storage>,
) -> anyhow::Result<()> {
    let nodes: HashSet<NodeId> = storages.keys().copied().collect();

    for current_node in nodes.iter() {
        let mut node_storage = storages.remove(current_node).unwrap();
        for moved_file in node_storage.moved.files() {
            let deleted = moved_file.as_deleted();

            // Get all the nodes where the file does not exist in the moved list and it does not
            // exist or is older in the current version
            let nodes_to_move: HashSet<NodeId> = storages
                .iter()
                .filter_map(|(node, storage)| {
                    if !storage.moved.contains_id(moved_file.id())
                        && storage.current.contains_id(deleted.id())
                        && is_file_missing_or_older(&storage.current, moved_file)
                    {
                        Some(*node)
                    } else {
                        None
                    }
                })
                .collect();

            if !nodes_to_move.is_empty() {
                // The file need to move to its new location in the node's tree
                let dst_path = node_storage.moved.get_path(moved_file.id()).unwrap();
                for node in nodes_to_move.iter() {
                    let other_node_storage = storages.get_mut(node).unwrap();
                    other_node_storage
                        .current
                        .move_file(deleted.id(), &dst_path);
                }

                // It also need to be removed from the node's deleted tree, to avoid generating
                // delete events for a non existing file
                node_storage.deleted.remove(deleted.id());
                dispatch_move_action(
                    context,
                    ignored_files_cache,
                    &node_storage.name,
                    nodes_to_move,
                    &moved_file.old_path,
                    &dst_path,
                    moved_file.date(),
                )
                .await?;
            }
        }
        node_storage.moved.clear();
        storages.insert(*current_node, node_storage);
    }

    Ok(())
}

async fn dispatch_move_action(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storage: &str,
    mut nodes: HashSet<NodeId>,

    src_path: &RelativePathBuf,
    dst_path: &RelativePathBuf,
    modified_at: u64,
) -> anyhow::Result<()> {
    if nodes.remove(&context.config.node_id_hashed) {
        crate::storage::file_operations::move_file(
            context,
            ignored_files_cache,
            storage,
            src_path,
            dst_path,
            modified_at,
        )
        .await?;
    }

    if !nodes.is_empty() {
        log::info!("Move {src_path:?} on {nodes:?}");
        context
            .rpc
            .multi_call(
                MoveFile {
                    storage: storage.to_string(),
                    src_path: src_path.clone(),
                    dst_path: dst_path.clone(),
                    modified_at,
                },
                nodes,
            )
            .ack()
            .await?;
    }

    Ok(())
}

fn is_file_missing_or_older<T: StorageFile>(files: &StorageTree<T>, file: &MovedFileInfo) -> bool {
    match files.get(file.id()) {
        Some(existing) => existing.date().cmp(&file.date()).is_lt(),
        None => true,
    }
}

async fn build_delete_actions(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storages: &mut HashMap<NodeId, Storage>,
) -> anyhow::Result<()> {
    // Delete iterates through all deleted files from all storages
    // then it checks which node has the file to the send the delete event
    let all_deleted_files: HashSet<DeletedFileInfo> = storages
        .values_mut()
        .flat_map(|storage| storage.deleted.drain())
        .collect();

    for deleted_file in all_deleted_files {
        // It is important to chek if any node has a newer version of the same file, in that case
        // the delete action is just ignored and the send action is generated later
        let has_existing_newer = storages.values().any(|storage| {
            storage
                .current
                .get(deleted_file.id())
                .map(|f| deleted_file.date().cmp(&f.date()).is_lt())
                .unwrap_or_default()
        });

        if has_existing_newer {
            continue;
        }

        let nodes_to_delete: HashSet<NodeId> = storages
            .iter()
            .filter_map(|(node, storage)| {
                if storage.current.contains_id(deleted_file.id()) {
                    Some(*node)
                } else {
                    None
                }
            })
            .collect();

        if !nodes_to_delete.is_empty() {
            let node_storage = storages
                .get(nodes_to_delete.iter().next().unwrap())
                .unwrap();

            let storage_name = node_storage.name.clone();
            let file_path = node_storage.current.get_path(deleted_file.id()).unwrap();

            for node in nodes_to_delete.iter() {
                storages
                    .get_mut(node)
                    .unwrap()
                    .current
                    .remove(deleted_file.id());
            }

            dispatch_delete_action(
                context,
                ignored_files_cache,
                &storage_name,
                nodes_to_delete,
                file_path,
                deleted_file.date(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn dispatch_delete_action(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storage: &str,
    mut nodes: HashSet<NodeId>,
    path: RelativePathBuf,
    timestamp: u64,
) -> anyhow::Result<()> {
    if nodes.remove(&context.config.node_id_hashed) {
        crate::storage::file_operations::delete_file(
            context,
            ignored_files_cache,
            storage,
            &path,
            timestamp,
        )
        .await?;
    }

    if !nodes.is_empty() {
        log::info!("Delete {path:?} on {nodes:?}");
        context
            .rpc
            .multi_call(
                DeleteFile {
                    storage: storage.to_string(),
                    path,
                    timestamp,
                },
                nodes,
            )
            .ack()
            .await?;
    }

    Ok(())
}

async fn build_send_actions(
    context: &Context,
    storages: &mut HashMap<NodeId, Storage>,
) -> anyhow::Result<Vec<(SyncFile, HashSet<NodeId>)>> {
    let mut to_sync = Vec::new();
    let all_ids: HashSet<FileId> = storages
        .values()
        .flat_map(|storage| storage.current.ids())
        .collect();

    for id in all_ids {
        // Get the node_id with the most recent file
        let (node_id, file) = storages
            .iter()
            .filter_map(|(node_id, s)| s.current.get(id).map(|f| (*node_id, f)))
            .max_by(|(_, a), (_, b)| a.date().cmp(&b.date()))
            .expect("at least one storage must have the file");

        // TODO: is this step necessary?
        let node_id = match storages
            .get(&context.config.node_id_hashed)
            .and_then(|s| s.current.get(id))
        {
            Some(local) if !local.needs_sync(file) => context.config.node_id_hashed,
            _ => node_id,
        };

        let nodes_out_of_sync: HashSet<NodeId> = storages
            .iter()
            .filter_map(|(node, storage)| match storage.current.get(id) {
                Some(f) => {
                    if file.needs_sync(f) {
                        Some(*node)
                    } else {
                        None
                    }
                }
                None => Some(*node),
            })
            .collect();

        if nodes_out_of_sync.is_empty() {
            continue;
        }

        let storage = storages.get(&node_id).unwrap();
        let sync_file = SyncFile {
            storage: storage.name.to_string(),
            path: storage.current.build_path(file),
            info: file.clone(),
        };
        if node_id == context.config.node_id_hashed {
            to_sync.push((sync_file, nodes_out_of_sync));
        } else {
            context
                .rpc
                .call(
                    SendFileTo {
                        file: sync_file,
                        nodes: nodes_out_of_sync,
                    },
                    node_id,
                )
                .ack()
                .await?;
        }
    }

    Ok(to_sync)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde::de::DeserializeOwned;
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;

    use crate::{
        context::local_contexts,
        transaction_log::{append_deleted, append_moved},
    };

    use super::*;

    #[tokio::test]
    async fn delete_generated_for_local_files() {
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "sub/file.txt", 9).await;
        append_deleted(&node, "sub/file.txt", 10).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await.unwrap(),
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await.unwrap(),
            ),
        ]);

        assert!(
            build_delete_actions(&leader, &mut Default::default(), &mut storages)
                .await
                .is_ok()
        );

        assert!(
            !leader
                .fs
                .exists(PathBuf::from("sub/file.txt").as_path())
                .await
        )
    }

    #[tokio::test]
    async fn delete_generated_for_node_files() {
        let [leader, node] = local_contexts().await;

        append_deleted(&leader, "sub/file.txt", 10).await;
        generate_file(&node, "sub/file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await.unwrap(),
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await.unwrap(),
            ),
        ]);

        let task = tokio::spawn(assert_event(node, |event: DeleteFile| {
            assert_eq!(event.path, "sub/file.txt".into());
            assert_eq!(event.timestamp, 10);
        }));

        assert!(
            build_delete_actions(&leader, &mut Default::default(), &mut storages)
                .await
                .is_ok()
        );

        assert!(task.await.is_ok());
    }

    #[tokio::test]
    async fn delete_skip_if_newer_file_exists() {
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "file.txt", 10).await;
        append_deleted(&node, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await.unwrap(),
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await.unwrap(),
            ),
        ]);

        assert!(
            build_delete_actions(&leader, &mut Default::default(), &mut storages)
                .await
                .is_ok()
        );

        assert!(leader.fs.exists(PathBuf::from("file.txt").as_path()).await)
    }

    #[tokio::test]
    async fn move_is_not_generated_when_destination_has_been_deleted() -> anyhow::Result<()> {
        // One node has the file moved, but most recent has the file deleted
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "file.txt", 9).await;
        append_moved(&leader, "file.txt", "old_file.txt", 9).await;

        append_deleted(&node, "file.txt", 10).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await.unwrap(),
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await.unwrap(),
            ),
        ]);

        buid_move_actions(&leader, &mut Default::default(), &mut storages)
            .await
            .expect("failed to process move actions");

        Ok(())
    }

    #[tokio::test]
    async fn move_action_moves_local_file() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "old_file.txt", 0).await;
        append_moved(&node, "file.txt", "old_file.txt", 9).await;
        generate_file(&node, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        buid_move_actions(&leader, &mut Default::default(), &mut storages)
            .await
            .expect("failed to process move actions");

        assert!(leader.fs.exists(PathBuf::from("file.txt").as_path()).await);

        Ok(())
    }

    #[tokio::test]
    async fn move_action_moves_local_file_when_dst_exists_but_is_older() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        // Create a file at the destination with an older timestamp
        generate_file(&leader, "file.txt", 5).await;
        // Node has a moved file with newer timestamp
        append_moved(&node, "file.txt", "old_file.txt", 9).await;
        generate_file(&node, "file.txt", 9).await;

        let storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let task = tokio::spawn(assert_event(node, |event: SendFileTo| {
            assert_eq!(event.file.path, "file.txt".into());
            assert_eq!(event.file.info.date(), 9);
            assert_eq!(event.nodes, [leader.config.node_id_hashed].into());
        }));

        ActionDispatcher::new(storages).execute(&leader).await?;

        task.await?;

        Ok(())
    }

    #[tokio::test]
    async fn move_action_send_event() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&node, "old_file.txt", 9).await;
        append_moved(&leader, "file.txt", "old_file.txt", 9).await;
        generate_file(&leader, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let task = tokio::spawn(assert_event(node, |event: MoveFile| {
            assert_eq!(event.dst_path, "file.txt".into());
            assert_eq!(event.src_path, "old_file.txt".into());
        }));

        buid_move_actions(&leader, &mut Default::default(), &mut storages)
            .await
            .expect("failed to process move actions");

        task.await?;

        Ok(())
    }

    #[tokio::test]
    async fn move_action_send_event_only_to_nodes_out_of_sync() -> anyhow::Result<()> {
        let [leader, node1, node2] = local_contexts().await;

        // node1 has outdated file
        generate_file(&node1, "old_file.txt", 9).await;

        // node2 already has the file in the correct location with correct timestamp
        generate_file(&node2, "file.txt", 9).await;

        // leader has moved the file
        append_moved(&leader, "file.txt", "old_file.txt", 9).await;
        generate_file(&leader, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node1.config.node_id_hashed,
                crate::storage::build(&node1, "a").await?,
            ),
            (
                node2.config.node_id_hashed,
                crate::storage::build(&node2, "a").await?,
            ),
        ]);

        // Setup task to verify that node1 receives move event
        let task1 = tokio::spawn(assert_event(node1, |event: MoveFile| {
            assert_eq!(event.dst_path, "file.txt".into());
            assert_eq!(event.src_path, "old_file.txt".into());
        }));

        // Setup task to verify that node2 does not receive any move event
        let task2 = tokio::spawn(assert_no_event::<MoveFile>(node2));

        buid_move_actions(&leader, &mut Default::default(), &mut storages)
            .await
            .expect("failed to process move actions");

        drop(leader);

        task1.await?;
        task2.await?;

        Ok(())
    }

    #[tokio::test]
    async fn send_when_file_is_missing() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let files = build_send_actions(&leader, &mut storages)
            .await
            .expect("failed to process move actions");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0.path, "file.txt".into());

        Ok(())
    }

    #[tokio::test]
    async fn send_when_file_is_deleted() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&leader, "file.txt", 9).await;
        append_deleted(&node, "file.txt", 8).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let files = build_send_actions(&leader, &mut storages)
            .await
            .expect("failed to process move actions");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0.path, "file.txt".into());

        Ok(())
    }

    #[tokio::test]
    async fn delegate_send_when_file_is_missing() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&node, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let task = tokio::spawn(assert_event(node, |event: SendFileTo| {
            assert_eq!(event.file.path, "file.txt".into());
            assert_eq!(event.nodes, [leader.config.node_id_hashed].into());
        }));

        let files = build_send_actions(&leader, &mut storages)
            .await
            .expect("failed to process move actions");

        assert_eq!(files.len(), 0);

        task.await?;

        Ok(())
    }

    #[tokio::test]
    async fn delegate_send_when_file_is_deleted() -> anyhow::Result<()> {
        let [leader, node] = local_contexts().await;

        generate_file(&node, "file.txt", 9).await;
        append_deleted(&leader, "file.txt", 8).await;

        let mut storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                node.config.node_id_hashed,
                crate::storage::build(&node, "a").await?,
            ),
        ]);

        let task = tokio::spawn(assert_event(node, |event: SendFileTo| {
            assert_eq!(event.file.path, "file.txt".into());
            assert_eq!(event.nodes, [leader.config.node_id_hashed].into());
        }));

        let files = build_send_actions(&leader, &mut storages)
            .await
            .expect("failed to process move actions");

        assert_eq!(files.len(), 0);

        task.await?;

        Ok(())
    }

    #[tokio::test]
    async fn send_has_precedence_over_delegate() -> anyhow::Result<()> {
        let [leader, one, two] = local_contexts().await;

        generate_file(&leader, "file.txt", 9).await;
        generate_file(&one, "file.txt", 9).await;

        let mut storages = HashMap::from([
            (
                one.config.node_id_hashed,
                crate::storage::build(&one, "a").await?,
            ),
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                two.config.node_id_hashed,
                crate::storage::build(&two, "a").await?,
            ),
        ]);

        let task = tokio::spawn(assert_no_event::<SendFileTo>(two.clone()));

        let files = build_send_actions(&leader, &mut storages)
            .await
            .expect("failed to process move actions");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].1, [two.config.node_id_hashed].into());

        drop(leader);
        drop(one);

        task.await?;

        Ok(())
    }

    #[tokio::test]
    async fn complex_scenario() -> anyhow::Result<()> {
        // leader has a file moved from a to b
        // one has an older file b
        // two has a newer file b
        let [leader, one, two] = local_contexts().await;

        // Set up initial state:
        // Leader has moved file.txt from old_file.txt
        generate_file(&leader, "old_file.txt", 5).await;
        append_moved(&leader, "file.txt", "old_file.txt", 5).await;
        generate_file(&leader, "file.txt", 5).await;

        // Node one has an older version of file.txt
        generate_file(&one, "file.txt", 3).await;

        // Node two has a newer version of file.txt
        generate_file(&two, "file.txt", 8).await;

        let storages = HashMap::from([
            (
                leader.config.node_id_hashed,
                crate::storage::build(&leader, "a").await?,
            ),
            (
                one.config.node_id_hashed,
                crate::storage::build(&one, "a").await?,
            ),
            (
                two.config.node_id_hashed,
                crate::storage::build(&two, "a").await?,
            ),
        ]);

        // Expect no move actions because node two has a newer version
        let task1 = tokio::spawn(assert_no_event::<MoveFile>(one));

        // Since node two has the newest version, expect a SendFileTo event to both leader and node one
        let task = tokio::spawn(assert_event(two, |event: SendFileTo| {
            assert_eq!(event.file.path, "file.txt".into());
            assert_eq!(event.file.info.date(), 8);
            assert_eq!(event.nodes.len(), 2);
        }));

        ActionDispatcher::new(storages).execute(&leader).await?;

        drop(leader);

        task1.await?;
        task.await?;

        Ok(())
    }

    async fn assert_event<T: crate::message_types::MessageType + DeserializeOwned, F: FnOnce(T)>(
        context: Context,
        f: F,
    ) {
        let mut events = context
            .rpc
            .subscribe(&[T::MESSAGE_TYPE])
            .await
            .expect("failed to subscribe");

        let event = events.next().await.expect("Event did not arrive");
        let data: T = event.data().expect("invalid event");

        f(data);

        event.ack().await.unwrap();
    }

    async fn assert_no_event<T: crate::message_types::MessageType + DeserializeOwned>(
        context: Context,
    ) {
        let mut events = context
            .rpc
            .subscribe(&[T::MESSAGE_TYPE])
            .await
            .expect("failed to subscribe");

        if events.next().await.is_some() {
            panic!("Receive event");
        }
    }

    async fn generate_file(context: &Context, path: &str, timestamp: u64) {
        let path = PathBuf::from(path);
        context
            .fs
            .open_w(path.as_path(), 1)
            .await
            .expect("failed to open {path}")
            .write_u8(0)
            .await
            .expect("failed to open {path}");

        context
            .fs
            .set_metadata(&path, 777, timestamp)
            .await
            .expect("failed to set metadata on {path}");
    }
}
