use std::fs;
use std::time::Duration;

mod common;

#[test]
fn test_truncate() -> Result<(), Box<dyn std::error::Error>> {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/truncate");
    let configs = common::generate_configs("truncate", 8098, 2, 1);

    let compare_all = || {
        common::tree_compare(
            &configs[0].storages["storage_0"].path,
            &configs[1].storages["storage_0"].path,
        );
    };

    let store_one = common::unchecked_generate_files(
        &configs[0].storages["storage_0"].path,
        &configs[0].node_id,
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
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
    });

    rt.shutdown_timeout(Duration::from_secs(5));

    compare_all();

    for file in store_one.iter() {
        common::truncate_file(file).expect("failed to truncate file");
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
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
    });

    compare_all();

    Ok(())
}
