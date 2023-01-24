use iron_carrier::leak::Leak;
use rand::seq::SliceRandom;
use std::fs;
use std::iter::Iterator;
use std::{thread, time::Duration};

mod common;
use iron_carrier::config::Config;
use iron_carrier::validation::Unverified;

#[tokio::test]
async fn test_partial_sync() -> Result<(), Box<dyn std::error::Error>> {
    // common::enable_logs(5);
    let [peer_1, peer_2, peer_3] = ["d", "e", "f"];

    let init_peer = |peer_name: &str, port: u16| {
        let config = format!(
            r#"
node_id="{peer_name}"
group="partial_sync"
port={port}
log_path = "/tmp/partial_sync/peer_{peer_name}/peer_log.log"
delay_watcher_events=1
[storages]
store_one = "/tmp/partial_sync/peer_{peer_name}/store_one"
store_two = "/tmp/partial_sync/peer_{peer_name}/store_two"
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
            format!("/tmp/partial_sync/peer_{peer_1}/store_one"),
            format!("/tmp/partial_sync/peer_{peer_2}/store_one"),
        );
        common::tree_compare(
            format!("/tmp/partial_sync/peer_{peer_1}/store_one"),
            format!("/tmp/partial_sync/peer_{peer_3}/store_one"),
        );
    };

    // cleanup from prev executions
    for peer_name in [peer_1, peer_2, peer_3] {
        let _ = fs::remove_dir_all(format!("/tmp/partial_sync/peer_{peer_name}"));
        let _ = fs::create_dir_all(format!("/tmp/partial_sync/peer_{peer_name}/store_one"));
        let _ = fs::create_dir_all(format!("/tmp/partial_sync/peer_{peer_name}/store_two"));
    }

    init_peer(peer_1, 8095);
    init_peer(peer_2, 8096);
    init_peer(peer_3, 8097);

    thread::sleep(Duration::from_secs(10));

    let store_one = common::unchecked_generate_files(
        format!("/tmp/partial_sync/peer_{peer_1}/store_one"),
        peer_1,
    );
    let store_two = common::unchecked_generate_files(
        format!("/tmp/partial_sync/peer_{peer_1}/store_two"),
        peer_1,
    );

    thread::sleep(Duration::from_secs(20));
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

    thread::sleep(Duration::from_secs(10));
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

    thread::sleep(Duration::from_secs(10));
    compare_all();

    Ok(())
}
