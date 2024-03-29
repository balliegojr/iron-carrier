use std::{fs, path::PathBuf, str::FromStr, thread, time::Duration};

mod common;

#[test]
fn test_full_sync() {
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
[paths]
store_one = {store_one_path:?}
store_two = {store_two_path:?}
"#,
        );
        let config = iron_carrier::config::Config::new_from_str(config).unwrap();
        configs.push(config);

        port += 1;
    }

    for config in configs {
        thread::spawn(move || {
            iron_carrier::run(config).expect("Iron carrier failed");
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
