use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use std::{fs, time::SystemTime};

use iron_carrier::config::Config;
use iron_carrier::constants::LINE_ENDING;
use iron_carrier::leak::Leak;
use iron_carrier::validation::Unverified;

mod common;

#[tokio::test]
async fn test_sync_deleted_files() {
    // common::enable_logs(5);
    let mut port = 8100;
    let peers = ["g", "h"];
    let mut configs = Vec::new();

    for peer_name in peers {
        let peer_path =
            PathBuf::from_str(&format!("/tmp/full_sync_pre_deleted/peer_{peer_name}")).unwrap();
        let log_path = peer_path.join("peer_log.log");
        let store_path = peer_path.join("store_one");

        let _ = fs::remove_dir_all(peer_path);

        let config = format!(
            r#"
node_id="{peer_name}"
group="pre_deleted"
port={port}
log_path = {log_path:?} 
delay_watcher_events=1
[storages]
store_one = {store_path:?}
"#
        );

        configs.push(
            config
                .parse::<Unverified<Config>>()
                .and_then(|config| config.validate())
                .unwrap()
                .leak(),
        );

        port += 1;
    }

    let store_path = PathBuf::from_str(&format!(
        "/tmp/full_sync_pre_deleted/peer_{}/store_one",
        peers[1]
    ))
    .unwrap();
    let files = common::generate_files(&store_path, "del");

    let log_path = PathBuf::from_str(&format!(
        "/tmp/full_sync_pre_deleted/peer_{}/peer_log.log",
        peers[0]
    ))
    .unwrap();

    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_path)
        .unwrap();

    for file in files.iter().filter(|f| !common::is_ignored(f)) {
        let log_line = format!(
            "{},store_one,Delete:{},Finished{}",
            SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5,
            file.strip_prefix(&store_path).unwrap().to_str().unwrap(),
            LINE_ENDING
        );
        log.write_all(log_line.as_bytes()).unwrap();
    }

    let mut handles = Vec::new();
    for config in configs {
        handles.push(tokio::spawn(async move {
            iron_carrier::run_full_sync(config)
                .await
                .expect("Iron carrier failed");
        }));
    }

    for handle in handles {
        handle.await.expect("Iron carrier failed");
    }

    for file in files {
        if common::is_ignored(&file) {
            common::assert_file_exists(file);
        } else {
            common::assert_file_deleted(file);
        }
    }
}
