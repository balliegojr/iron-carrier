use std::fs;
use std::iter::Iterator;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

mod common;

#[tokio::test]
async fn test_partial_sync() -> Result<(), Box<dyn std::error::Error>> {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/partial_sync");
    let configs = common::generate_configs("partial_sync", common::PARTIAL_SYNC_PORT, 3, 2);

    let compare_all = || {
        for config in configs.iter().skip(1) {
            for storage in configs[0].storages.keys() {
                common::tree_compare(
                    &configs[0].storages[storage].path,
                    &config.storages[storage].path,
                );
            }
        }
    };

    let store_one = common::unchecked_generate_files(
        &configs[0].storages["storage_0"].path,
        &configs[0].node_id,
    );
    let store_two = common::unchecked_generate_files(
        &configs[0].storages["storage_1"].path,
        &configs[0].node_id,
    );

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
    compare_all();
    tokio::time::sleep(Duration::from_secs(1)).await;

    execute_file_operations(&store_one, &mut when_sync_done).await;
    compare_all();

    tokio::time::sleep(Duration::from_secs(1)).await;

    execute_file_operations(&store_two, &mut when_sync_done).await;
    compare_all();

    for handle in handles {
        handle.abort();
    }

    Ok(())
}

async fn execute_file_operations(files: &[PathBuf], when_sync_done: &mut Receiver<()>) {
    let chunk_size = files.len() / 3;
    let to_remove = &files[..chunk_size];
    let to_rename = &files[chunk_size..chunk_size * 2];
    let to_append = &files[chunk_size * 2..];

    for file in to_remove {
        std::fs::remove_file(file).expect("Failed to remove file");
    }

    for file in to_rename {
        let new_name = file.with_extension("ren");
        std::fs::rename(file, new_name).expect("Failed to rename");
    }

    for file in to_append {
        common::append_content(file, b"random content");
    }

    when_sync_done.recv().await;

    for file in to_remove {
        assert!(!file.exists());
    }

    for file in to_rename {
        assert!(!file.exists());

        let new_name = file.with_extension("ren");
        assert!(new_name.exists());
    }
}
