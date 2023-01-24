use std::fs;
use std::{thread, time::Duration};

mod common;
use iron_carrier::config::Config;
use iron_carrier::leak::Leak;
use iron_carrier::validation::Unverified;

#[tokio::test]
async fn test_truncate() -> Result<(), Box<dyn std::error::Error>> {
    // common::enable_logs(5);
    let [peer_1, peer_2] = ["g", "h"];

    let init_peer = |peer_name: &str, port: u16| {
        let config = format!(
            r#"
node_id="{peer_name}"
group="partial_sync"
port={port}
log_path = "/tmp/truncate/peer_{peer_name}/peer_log.log"
delay_watcher_events=1
[storages]
store_one = "/tmp/truncate/peer_{peer_name}/store_one"
"#,
        );

        let config = config
            .parse::<Unverified<Config>>()
            .and_then(|config| config.validate())
            .unwrap()
            .leak();
        tokio::spawn(async move {
            iron_carrier::start_daemon(config)
                .await
                .expect("Carrier failed");
        });
    };

    let compare_all = || {
        common::tree_compare(
            format!("/tmp/truncate/peer_{peer_1}/store_one"),
            format!("/tmp/truncate/peer_{peer_2}/store_one"),
        );
    };

    // cleanup from prev executions
    for peer_name in [peer_1, peer_2] {
        let _ = fs::remove_dir_all(format!("/tmp/truncate/peer_{peer_name}"));
        let _ = fs::create_dir_all(format!("/tmp/truncate/peer_{peer_name}/store_one"));
    }

    init_peer(peer_1, 8098);
    init_peer(peer_2, 8099);

    thread::sleep(Duration::from_secs(10));

    let store_one =
        common::unchecked_generate_files(format!("/tmp/truncate/peer_{peer_1}/store_one"), peer_1);

    thread::sleep(Duration::from_secs(10));
    compare_all();

    for file in store_one.iter() {
        common::truncate_file(file).expect("failed to truncate file");
    }

    thread::sleep(Duration::from_secs(10));
    compare_all();

    Ok(())
}
