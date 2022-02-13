use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use std::thread;
use std::{fs, time::SystemTime};

mod common;

#[test]
fn test_sync_deleted_files() {
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
port={port}
log_path = {:?} 
delay_watcher_events=1
[paths]
store_one = {:?}
"#,
            log_path, store_path
        );

        let config = iron_carrier::config::Config::new_from_str(config).unwrap();
        configs.push(config);

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
            "{},store_one,Delete:{},Finished\n",
            SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5,
            file.strip_prefix(&store_path).unwrap().to_str().unwrap()
        );
        log.write_all(log_line.as_bytes()).unwrap();
    }

    for config in configs {
        thread::spawn(move || {
            iron_carrier::run(config).expect("Iron carrier failed");
        });
    }

    for file in files {
        if common::is_ignored(&file) {
            common::assert_file_exists(file);
        } else {
            common::assert_file_deleted(file);
        }
    }
}
