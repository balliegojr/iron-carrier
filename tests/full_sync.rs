use std::fs;

mod common;

#[tokio::test]
async fn test_full_sync() {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/full_sync");
    let configs = common::generate_configs("full_sync", common::FULL_SYNC_PORT, 3, 2, None);

    for config in configs.iter() {
        common::generate_files(&config.storages["storage_0"].path, &config.node_id);
        common::generate_files(&config.storages["storage_1"].path, &config.node_id);
    }

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

    for config in configs.iter().skip(1) {
        for storage in configs[0].storages.keys() {
            common::tree_compare(
                &configs[0].storages[storage].path,
                &config.storages[storage].path,
            );
        }
    }

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test]
async fn test_full_sync_encrypted() {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/full_sync_encrypted");
    let configs = common::generate_configs(
        "full_sync_encrypted",
        common::FULL_SYNC_PORT_ENCRYPTED,
        3,
        2,
        Some("secret encryption key"),
    );

    for config in configs.iter() {
        common::generate_files(&config.storages["storage_0"].path, &config.node_id);
        common::generate_files(&config.storages["storage_1"].path, &config.node_id);
    }

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

    for config in configs.iter().skip(1) {
        for storage in configs[0].storages.keys() {
            common::tree_compare(
                &configs[0].storages[storage].path,
                &config.storages[storage].path,
            );
        }
    }

    for handle in handles {
        handle.abort();
    }
}
