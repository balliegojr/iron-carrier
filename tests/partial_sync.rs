use rand::seq::SliceRandom;
use std::fs;
use std::iter::Iterator;
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_partial_sync() -> Result<(), Box<dyn std::error::Error>> {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/partial_sync");
    let configs = common::generate_configs("partial_sync", 8095, 3, 2);

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

    for (i, file) in store_one.iter().enumerate() {
        match i % 3 {
            0 => {
                let _ = std::fs::remove_file(file);
            }
            1 => {
                let _ = std::fs::rename(file, file.join("renamed.ren"));
            }
            2 => {
                common::append_content(file, b"random content");
            }
            _ => {}
        }
    }

    when_sync_done.recv().await;
    compare_all();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut rng = rand::thread_rng();
    for i in 0..100 {
        let file = store_two.choose(&mut rng).unwrap();
        match i % 3 {
            0 => {
                let _ = std::fs::remove_file(file);
            }
            1 => {
                let _ = std::fs::rename(file, file.join("renamed.ren"));
            }
            2 => {
                common::append_content(file, b"random content");
            }
            _ => {}
        }
    }

    when_sync_done.recv().await;
    compare_all();

    for handle in handles {
        handle.abort();
    }

    Ok(())
}
