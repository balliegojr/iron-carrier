use std::{fs, path::PathBuf, str::FromStr, thread, time::Duration};

use iron_carrier::config::Config;
use iron_carrier::leak::Leak;
use iron_carrier::validation::Unverified;

mod common;

#[tokio::test]
async fn test_full_sync() {
    // common::enable_logs(5);
    let mut port = 8090;
    let peers = ["a", "b", "c"];
    let mut store_one = Vec::new();
    let mut store_two = Vec::new();
    let mut configs = Vec::new();

    for peer_name in peers {
        let peer_path = PathBuf::from_str(&format!("/tmp/full_sync/peer_{peer_name}")).unwrap();
        let log_path = peer_path.join("peer_log.log");
        let store_one_path = peer_path.join("store_one");
        let store_two_path = peer_path.join("store_two");

        let _ = fs::remove_file(&log_path);

        store_one.extend(common::generate_files(&store_one_path, peer_name));
        store_two.extend(common::generate_files(&store_two_path, peer_name));

        let config = format!(
            r#"
node_id="{peer_name}"
group="full_sync"
port={port}
log_path = {log_path:?}
[storages]
store_one = {store_one_path:?}
store_two = {store_two_path:?}
"#,
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

    for config in configs {
        tokio::spawn(async move {
            iron_carrier::start_daemon(config)
                .await
                .expect("Iron carrier failed");
        });
    }

    thread::sleep(Duration::from_secs(15));

    common::tree_compare(
        PathBuf::from_str(&format!("/tmp/full_sync/peer_{}", peers[0])).unwrap(),
        PathBuf::from_str(&format!("/tmp/full_sync/peer_{}", peers[1])).unwrap(),
    );

    common::tree_compare(
        PathBuf::from_str(&format!("/tmp/full_sync/peer_{}", peers[0])).unwrap(),
        PathBuf::from_str(&format!("/tmp/full_sync/peer_{}", peers[2])).unwrap(),
    );
}
