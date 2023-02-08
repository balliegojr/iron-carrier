use iron_carrier::transaction_log::{EntryStatus, EntryType, LogEntry, TransactionLog};
use std::time::SystemTime;

mod common;

#[tokio::test]
async fn test_sync_deleted_files() {
    common::enable_logs();
    const TEST_NAME: &str = "full_sync_pre_deleted";

    let _ = std::fs::remove_dir_all(format!("/tmp/{TEST_NAME}"));
    let configs = common::generate_configs(TEST_NAME, 8100, 2, 1);

    let files = common::generate_files(&configs[1].storages["storage_0"].path, "del");
    let transaction_log = TransactionLog::load(&configs[0].log_path)
        .await
        .expect("Failed to load log");

    for file in files.iter().filter(|f| !common::is_ignored(f)) {
        transaction_log
            .append_entry(
                "storage_0",
                file.strip_prefix(&configs[1].storages["storage_0"].path)
                    .unwrap(),
                None,
                LogEntry {
                    timestamp: SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5,
                    event_type: EntryType::Delete,
                    event_status: EntryStatus::Done,
                },
            )
            .await
            .expect("Failed to add entry");
    }

    let handles = configs
        .into_iter()
        .map(|config| tokio::spawn(iron_carrier::run_full_sync(config)))
        .collect::<Vec<_>>();

    for handle in handles {
        let _ = handle.await;
    }

    for file in files {
        if common::is_ignored(&file) {
            common::assert_file_exists(file);
        } else {
            common::assert_file_deleted(file);
        }
    }
}
