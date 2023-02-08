use std::fs;
use std::iter::Iterator;
use std::time::Duration;

use rand::seq::SliceRandom;

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

    let handles: Vec<_> = configs
        .iter()
        .map(|config| {
            tokio::spawn(async {
                let _ = iron_carrier::start_daemon(config).await;
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_secs(10)).await;
    compare_all();

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

    // TODO: wait for sync to finish
    tokio::time::sleep(Duration::from_secs(10)).await;
    compare_all();

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

    tokio::time::sleep(Duration::from_secs(10)).await;
    compare_all();

    Ok(())
}
