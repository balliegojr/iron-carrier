use std::collections::HashSet;
use std::fs;
use std::iter::Iterator;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

mod common;

#[tokio::test]
async fn test_file_watcher_folder_operations() -> Result<(), Box<dyn std::error::Error>> {
    common::enable_logs();

    let _ = fs::remove_dir_all("/tmp/folder_operation");
    let configs = common::generate_configs(
        "folder_operation",
        common::FOLDER_OPERATION_PORT,
        2,
        2,
        None,
    );

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

    remove_folder(&store_one, &mut when_sync_done).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    compare_all();

    rename_folder(&store_two, &mut when_sync_done).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    compare_all();

    for handle in handles {
        handle.abort();
    }

    Ok(())
}

async fn remove_folder(files: &[PathBuf], when_sync_done: &mut Receiver<()>) {
    let mut folders = Vec::from_iter(
        files
            .iter()
            .filter_map(|f| {
                let p = f.parent().unwrap();
                if p.ends_with("ignored_folder") {
                    None
                } else {
                    Some(p)
                }
            })
            .collect::<HashSet<_>>(),
    );

    folders.sort();

    let folder = folders.last().unwrap();
    std::fs::remove_dir_all(folder).expect("Failed to remove folder");

    when_sync_done.recv().await;

    assert!(!folder.exists());
}

async fn rename_folder(files: &[PathBuf], when_sync_done: &mut Receiver<()>) {
    let mut folders = Vec::from_iter(
        files
            .iter()
            .filter_map(|f| {
                let p = f.parent().unwrap();
                if p.ends_with("ignored_folder") {
                    None
                } else {
                    Some(p)
                }
            })
            .collect::<HashSet<_>>(),
    );

    folders.sort();
    let folder = folders.last().unwrap();

    let new_name = folder.parent().unwrap().join("renamed");
    std::fs::rename(folder, &new_name).expect("Failed to rename");

    when_sync_done.recv().await;

    assert!(!folder.exists());
    assert!(new_name.exists());
}
