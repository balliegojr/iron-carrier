use std::collections::HashSet;

use crate::{
    Context,
    node_id::NodeId,
    relative_path::RelativePathBuf,
    state_machine::{Result, State},
    storage::storage_tree::ExistingFileInfo,
};

mod block_index;
pub use block_index::BlockIndexPosition;

mod events;
mod receiver;
mod sender;
mod transfer;

pub use events::TransferFilesStart;
use serde::{Deserialize, Serialize};
pub use transfer::Transfer;

use self::events::TransferFilesCompleted;

#[derive(Debug)]
pub struct TransferFiles {
    files_to_send: Vec<(SyncFile, HashSet<NodeId>)>,
    sync_leader_id: Option<NodeId>,
}

impl State for TransferFiles {
    type Output = ();

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        let leader_id = self.sync_leader_id;
        let wait_complete_from = match leader_id {
            Some(leader_id) => HashSet::from([leader_id]),
            None => context.rpc.broadcast(TransferFilesStart).ack().await?,
        };

        let receive_task =
            tokio::spawn(receiver::receive_files(context.clone(), wait_complete_from));

        if let Err(err) = sender::send_files(context, self.files_to_send).await {
            log::error!("{err}")
        }

        let node_id = context.config.node_id_hashed;
        log::trace!("{node_id} done sending files");

        match leader_id {
            Some(leader_id) => {
                let _ = context
                    .rpc
                    .call(TransferFilesCompleted, leader_id)
                    .ack()
                    .await;

                if let Err(err) = receive_task.await {
                    log::error!("{err}")
                }
            }
            None => {
                if let Err(err) = receive_task.await {
                    log::error!("{err}")
                }
                let _ = context.rpc.broadcast(TransferFilesCompleted).ack().await;
            }
        }

        log::trace!("{node_id} done receiving files");

        Ok(())
    }
}

impl TransferFiles {
    pub fn new(
        sync_leader_id: Option<NodeId>,
        files_to_send: Vec<(SyncFile, HashSet<NodeId>)>,
    ) -> Self {
        Self {
            files_to_send,
            sync_leader_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncFile {
    pub storage: String,
    pub path: RelativePathBuf,
    pub info: ExistingFileInfo,
}

#[cfg(test)]
mod tests {
    use crate::{fs::MetadataB, message_types::MessageTypes, relative_path::RelativePath};

    use super::*;
    use rand::Rng;
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn leader_send_full_file_to_single_node() {
        let [leader, node] = crate::context::local_contexts().await;

        let file = generate_file(&leader, "test.txt", 10240)
            .await
            .expect("failed to generate file");
        let node_task = spawn_f(node.clone(), leader.config.node_id_hashed, vec![]);

        let leader_task = spawn_l(
            leader.clone(),
            vec![(file.clone(), [node.config.node_id_hashed].into())],
        );

        let (leader_result, node_result) = tokio::join!(leader_task, node_task);
        let leader_result = leader_result.unwrap();
        let node_result = node_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_result.is_ok());

        compare_files(&leader, &node, file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_partial_file_to_single_node() {
        let [leader, node] = crate::context::local_contexts().await;

        let leader_file = generate_file(&leader, "test.txt", 10240)
            .await
            .expect("failed to generate file");
        generate_file(&node, "test.txt", 5120)
            .await
            .expect("failed to generate file");

        let node_task = spawn_f(node.clone(), leader.config.node_id_hashed, vec![]);
        let leader_task = spawn_l(
            leader.clone(),
            vec![(leader_file.clone(), [node.config.node_id_hashed].into())],
        );

        let (leader_result, node_result) = tokio::join!(leader_task, node_task);
        let leader_result = leader_result.unwrap();
        let node_result = node_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_result.is_ok());

        compare_files(&leader, &node, leader_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_full_file_to_multiple_nodes() {
        let [leader, node_one, node_two] = crate::context::local_contexts().await;

        let leader_file = generate_file(&leader, "test.txt", 10240)
            .await
            .expect("failed to generate file");

        let node_one_task = spawn_f(node_one.clone(), leader.config.node_id_hashed, vec![]);
        let node_two_task = spawn_f(node_two.clone(), leader.config.node_id_hashed, vec![]);

        let leader_task = spawn_l(
            leader.clone(),
            vec![(
                leader_file.clone(),
                [
                    node_one.config.node_id_hashed,
                    node_two.config.node_id_hashed,
                ]
                .into(),
            )],
        );

        let (leader_result, node_one_result, node_two_result) =
            tokio::join!(leader_task, node_one_task, node_two_task);
        let leader_result = leader_result.unwrap();
        let node_one_result = node_one_result.unwrap();
        let node_two_result = node_two_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_one_result.is_ok());
        assert!(node_two_result.is_ok());

        compare_files(&leader, &node_one, leader_file.path.as_path()).await;
        compare_files(&leader, &node_two, leader_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_partial_file_to_multiple_nodes() {
        let [leader, node_one, node_two] = crate::context::local_contexts().await;

        let leader_file = generate_file(&leader, "test.txt", 10240)
            .await
            .expect("failed to generate file");

        // Generate partial files on both nodes
        generate_file(&node_one, "test.txt", 5120)
            .await
            .expect("failed to generate file");
        generate_file(&node_two, "test.txt", 7168)
            .await
            .expect("failed to generate file");

        let node_one_task = spawn_f(node_one.clone(), leader.config.node_id_hashed, vec![]);
        let node_two_task = spawn_f(node_two.clone(), leader.config.node_id_hashed, vec![]);

        let leader_task = spawn_l(
            leader.clone(),
            vec![(
                leader_file.clone(),
                [
                    node_one.config.node_id_hashed,
                    node_two.config.node_id_hashed,
                ]
                .into(),
            )],
        );

        let (leader_result, node_one_result, node_two_result) =
            tokio::join!(leader_task, node_one_task, node_two_task);
        let leader_result = leader_result.unwrap();
        let node_one_result = node_one_result.unwrap();
        let node_two_result = node_two_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_one_result.is_ok());
        assert!(node_two_result.is_ok());

        compare_files(&leader, &node_one, leader_file.path.as_path()).await;
        compare_files(&leader, &node_two, leader_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_and_receive_file_to_single_node() {
        let [leader, node] = crate::context::local_contexts().await;

        let leader_send_file = generate_file(&leader, "send.txt", 10240)
            .await
            .expect("failed to generate leader send file");
        let node_send_file = generate_file(&node, "receive.txt", 8192)
            .await
            .expect("failed to generate node send file");

        let node_task = spawn_f(
            node.clone(),
            leader.config.node_id_hashed,
            vec![(
                node_send_file.clone(),
                [leader.config.node_id_hashed].into(),
            )],
        );

        let leader_task = spawn_l(
            leader.clone(),
            vec![(
                leader_send_file.clone(),
                [node.config.node_id_hashed].into(),
            )],
        );

        let (leader_result, node_result) = tokio::join!(leader_task, node_task);
        let leader_result = leader_result.unwrap();
        let node_result = node_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_result.is_ok());

        // Verify files were transferred in both directions
        compare_files(&leader, &node, leader_send_file.path.as_path()).await;
        compare_files(&node, &leader, node_send_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_wait_for_file_from_single_node() {
        let [leader, node] = crate::context::local_contexts().await;

        let node_file = generate_file(&node, "test.txt", 10240)
            .await
            .expect("failed to generate file");

        let node_task = spawn_f(
            node.clone(),
            leader.config.node_id_hashed,
            vec![(node_file.clone(), [leader.config.node_id_hashed].into())],
        );

        let leader_task = spawn_l(leader.clone(), vec![]);

        let (leader_result, node_result) = tokio::join!(leader_task, node_task);
        let leader_result = leader_result.unwrap();
        let node_result = node_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_result.is_ok());

        compare_files(&node, &leader, node_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_wait_for_file_from_multiple_nodes() {
        let [leader, node_one, node_two] = crate::context::local_contexts().await;

        let node_one_file = generate_file(&node_one, "test1.txt", 10240)
            .await
            .expect("failed to generate node one file");
        let node_two_file = generate_file(&node_two, "test2.txt", 8192)
            .await
            .expect("failed to generate node two file");

        let node_one_task = spawn_f(
            node_one.clone(),
            leader.config.node_id_hashed,
            vec![(node_one_file.clone(), [leader.config.node_id_hashed].into())],
        );
        let node_two_task = spawn_f(
            node_two.clone(),
            leader.config.node_id_hashed,
            vec![(node_two_file.clone(), [leader.config.node_id_hashed].into())],
        );

        let leader_task = spawn_l(leader.clone(), vec![]);

        let (leader_result, node_one_result, node_two_result) =
            tokio::join!(leader_task, node_one_task, node_two_task);
        let leader_result = leader_result.unwrap();
        let node_one_result = node_one_result.unwrap();
        let node_two_result = node_two_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_one_result.is_ok());
        assert!(node_two_result.is_ok());

        compare_files(&node_one, &leader, node_one_file.path.as_path()).await;
        compare_files(&node_two, &leader, node_two_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_file_to_node_and_wait_from_another() {
        let [leader, node_one, node_two] = crate::context::local_contexts().await;

        let leader_file = generate_file(&leader, "send.txt", 10240)
            .await
            .expect("failed to generate leader file");
        let node_two_file = generate_file(&node_two, "receive.txt", 8192)
            .await
            .expect("failed to generate node two file");

        let node_one_task = spawn_f(node_one.clone(), leader.config.node_id_hashed, vec![]);
        let node_two_task = spawn_f(
            node_two.clone(),
            leader.config.node_id_hashed,
            vec![(node_two_file.clone(), [leader.config.node_id_hashed].into())],
        );

        let leader_task = spawn_l(
            leader.clone(),
            vec![(leader_file.clone(), [node_one.config.node_id_hashed].into())],
        );

        let (leader_result, node_one_result, node_two_result) =
            tokio::join!(leader_task, node_one_task, node_two_task);
        let leader_result = leader_result.unwrap();
        let node_one_result = node_one_result.unwrap();
        let node_two_result = node_two_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_one_result.is_ok());
        assert!(node_two_result.is_ok());

        compare_files(&leader, &node_one, leader_file.path.as_path()).await;
        compare_files(&node_two, &leader, node_two_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn all_nodes_send_files_except_leader() {
        let [leader, node_one, node_two] = crate::context::local_contexts().await;

        // Generate files for both non-leader nodes
        let node_one_file = generate_file(&node_one, "node1.txt", 10240)
            .await
            .expect("failed to generate node one file");
        let node_two_file = generate_file(&node_two, "node2.txt", 8192)
            .await
            .expect("failed to generate node two file");

        // Each node sends its file to both other nodes
        let node_one_task = spawn_f(
            node_one.clone(),
            leader.config.node_id_hashed,
            vec![(
                node_one_file.clone(),
                [leader.config.node_id_hashed, node_two.config.node_id_hashed].into(),
            )],
        );

        let node_two_task = spawn_f(
            node_two.clone(),
            leader.config.node_id_hashed,
            vec![(
                node_two_file.clone(),
                [leader.config.node_id_hashed, node_one.config.node_id_hashed].into(),
            )],
        );

        // Leader only coordinates but doesn't send any files
        let leader_task = spawn_l(leader.clone(), vec![]);

        let (leader_result, node_one_result, node_two_result) =
            tokio::join!(leader_task, node_one_task, node_two_task);
        let leader_result = leader_result.unwrap();
        let node_one_result = node_one_result.unwrap();
        let node_two_result = node_two_result.unwrap();

        assert!(leader_result.is_ok());
        assert!(node_one_result.is_ok());
        assert!(node_two_result.is_ok());

        // Verify that node one's file is present on both leader and node two
        compare_files(&node_one, &leader, node_one_file.path.as_path()).await;
        compare_files(&node_one, &node_two, node_one_file.path.as_path()).await;

        // Verify that node two's file is present on both leader and node one
        compare_files(&node_two, &leader, node_two_file.path.as_path()).await;
        compare_files(&node_two, &node_one, node_two_file.path.as_path()).await;
    }

    #[tokio::test]
    async fn leader_send_file_with_missing_node() {
        let [leader, node_one] = crate::context::local_contexts().await;

        // Create a non-existent node ID that's different from both leader and node_one
        let missing_node: NodeId = 2.into();

        let leader_file = generate_file(&leader, "test.txt", 10240)
            .await
            .expect("failed to generate leader file");

        let node_task = spawn_f(node_one.clone(), leader.config.node_id_hashed, vec![]);

        // Try to send to both the existing node and a non-existent one
        let leader_task = spawn_l(
            leader.clone(),
            vec![(
                leader_file.clone(),
                [node_one.config.node_id_hashed, missing_node].into(),
            )],
        );

        let (leader_result, node_result) = tokio::join!(leader_task, node_task);
        let leader_result = leader_result.unwrap();
        let node_result = node_result.unwrap();

        // The operation should complete successfully even with a missing node
        assert!(leader_result.is_ok());
        assert!(node_result.is_ok());

        // Verify that the existing node received the file
        compare_files(&leader, &node_one, leader_file.path.as_path()).await;
    }

    async fn generate_file(
        context: &Context,
        file_name: &str,
        desired_size: u64,
    ) -> anyhow::Result<SyncFile> {
        let path = RelativePathBuf::from(file_name);
        let metadata = MetadataB::new()
            .len(desired_size)
            .permissions(777)
            .modified(1)
            .build();

        let info = ExistingFileInfo::new(path.as_path(), &metadata);
        let mut file = context
            .fs
            .open_w(&path.build_path(), metadata.len())
            .await?;

        // Generate random content in chunks to avoid allocating too much memory at once
        let mut rng = rand::rng();
        let chunk_size = 1024;
        let mut remaining = desired_size as usize;

        let mut chunk = vec![0u8; chunk_size];
        while remaining > 0 {
            let current_chunk_size = std::cmp::min(remaining, chunk_size) as usize;
            rng.fill(&mut chunk[..current_chunk_size]);
            file.write_all(&chunk[..current_chunk_size]).await?;
            remaining = remaining.saturating_sub(current_chunk_size);
        }

        context
            .fs
            .set_metadata(
                &path.build_path(),
                metadata.permissions(),
                metadata.modified_as_secs(),
            )
            .await?;

        Ok(SyncFile {
            storage: "a".to_string(),
            path,
            info,
        })
    }

    async fn compare_files(src: &Context, dst: &Context, path: RelativePath<'_>) {
        let src_m = src.fs.metadata(&path.build_path()).await.unwrap();
        let dst_m = dst.fs.metadata(&path.build_path()).await.unwrap();

        assert_eq!(src_m, dst_m);

        let src_c = src.fs.read_to_string(&path.build_path()).await.unwrap();
        let dst_c = dst.fs.read_to_string(&path.build_path()).await.unwrap();

        assert_eq!(src_c, dst_c);
    }

    fn spawn_l(
        context: Context,
        files_to_send: Vec<(SyncFile, HashSet<NodeId>)>,
    ) -> tokio::task::JoinHandle<std::result::Result<(), crate::StateMachineError>> {
        tokio::spawn(async move {
            TransferFiles::new(None, files_to_send)
                .execute(&context)
                .await
        })
    }

    fn spawn_f(
        context: Context,
        sync_leader_id: NodeId,
        files_to_send: Vec<(SyncFile, HashSet<NodeId>)>,
    ) -> tokio::task::JoinHandle<std::result::Result<(), crate::StateMachineError>> {
        tokio::spawn(async move {
            let mut events = context
                .rpc
                .subscribe(&[MessageTypes::TransferFilesStart])
                .await?;

            let request = events.next().await.unwrap();
            match request.type_id()? {
                MessageTypes::TransferFilesStart => {
                    request.ack().await?;
                    TransferFiles::new(Some(sync_leader_id), files_to_send)
                        .execute(&context)
                        .await
                }
                _ => unreachable!(),
            }
        })
    }
}
