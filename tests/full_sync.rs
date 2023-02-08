use std::fs;

mod common;

#[tokio::test]
async fn test_full_sync() {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/full_sync");
    let configs = common::generate_configs("full_sync", 8090, 3, 2);

    for config in configs.iter() {
        common::generate_files(&config.storages["storage_0"].path, &config.node_id);
        common::generate_files(&config.storages["storage_1"].path, &config.node_id);
    }

    let handles: Vec<_> = configs
        .iter()
        .map(|config| {
            tokio::spawn(async {
                let _ = iron_carrier::run_full_sync(config).await;
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Iron carrier failed");
    }

    for config in configs.iter().skip(1) {
        for storage in configs[0].storages.keys() {
            common::tree_compare(
                &configs[0].storages[storage].path,
                &config.storages[storage].path,
            );
        }
    }
}
