mod common;

#[tokio::test]
async fn partial_file_from_interrupted_transfer_is_completed_on_next_sync(
) -> Result<(), Box<dyn std::error::Error>> {
    common::enable_logs();

    let _ = std::fs::remove_dir_all("/tmp/interrupted_sync");
    let configs =
        common::generate_configs("interrupted_sync", common::INTERRUPTED_SYNC_PORT, 2, 1, None);

    let node_a_storage = &configs[0].storages["storage_0"].path;
    let node_b_storage = &configs[1].storages["storage_0"].path;

    std::fs::create_dir_all(node_a_storage)?;
    std::fs::create_dir_all(node_b_storage)?;

    // Source file on node A — 8 KB of random content (spans multiple sync blocks)
    let file_name = "data.bin";
    let content: Vec<u8> = (0..8192u16).map(|i| (i % 251) as u8).collect();
    let node_a_file = node_a_storage.join(file_name);
    std::fs::write(&node_a_file, &content)?;

    // Simulate the post-interrupted-transfer state on node B:
    // same size (as set_len produces), zeroed content, mtime never updated by set_metadata
    let node_b_file = node_b_storage.join(file_name);
    std::fs::write(&node_b_file, vec![0u8; content.len()])?;

    // Write a Pending log entry — causes walk_path to exclude the file from node B's storage
    // hash, ensuring the sync always triggers regardless of accidental hash match
    let log = iron_carrier::transaction_log::TransactionLog::load(&configs[1].log_path)?;
    log.mark_write_pending("storage_0", std::path::Path::new(file_name), 0)
        .await?;

    // Start both daemons and wait for the sync to complete
    let (tx, mut when_sync_done) = tokio::sync::mpsc::channel(1);
    let handles: Vec<_> = configs
        .iter()
        .map(|config| {
            let tx = tx.clone();
            tokio::spawn(async {
                let _ = iron_carrier::start_daemon(config, Some(tx)).await;
            })
        })
        .collect();

    when_sync_done.recv().await;

    // The partial file must have been repaired — content now matches node A
    assert_eq!(
        std::fs::read(&node_a_file)?,
        std::fs::read(&node_b_file)?,
        "partial file from interrupted transfer must be completed by the next sync"
    );

    for handle in handles {
        handle.abort();
    }

    Ok(())
}
