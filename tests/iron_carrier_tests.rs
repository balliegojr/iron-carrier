use std::io::Write;
use std::iter::Iterator;
use std::path::Path;
use std::{path::PathBuf, thread, time::Duration};

use bytes::Buf;
use rand::Rng;

fn prepare_files() -> ([u8; 1024], [u8; 1024]) {
    cleanup();
    let _ = std::fs::create_dir_all("tmp/peer_a");
    let _ = std::fs::create_dir_all("tmp/peer_b");

    let mut rng = rand::thread_rng();

    let mut contents_a = [0u8; 1024];
    rng.fill(&mut contents_a);

    let mut contents_b = [0u8; 1024];
    rng.fill(&mut contents_b);

    for file in 1..=2 {
        let _ = std::fs::write(format!("tmp/peer_a/a_file_{}", file), &contents_a);
        let _ = std::fs::write(format!("tmp/peer_b/b_file_{}", file), &contents_b);
    }

    (contents_a, contents_b)
}

fn cleanup() {
    let _ = std::fs::remove_dir_all("tmp/peer_a");
    let _ = std::fs::remove_dir_all("tmp/peer_b");
}

fn append_content<P: AsRef<Path>>(path: P, content: &[u8]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.write(content).unwrap();
}

#[test]
fn test_iron_carrier() {
    stderrlog::new()
        .verbosity(4)
        .module("iron_carrier")
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let (contents_a, contents_b) = prepare_files();

    let config_a = iron_carrier::config::Config::new_from_str(
        r#"
port=8090
delay_watcher_events=1
enable_service_discovery=false
peers = ["127.0.0.1:8091"]
[paths]
a = "./tmp/peer_a"
"#
        .to_string(),
    )
    .unwrap();

    let config_b = iron_carrier::config::Config::new_from_str(
        r#"
port=8091
delay_watcher_events=1
enable_service_discovery=false
peers = ["127.0.0.1:8090"]
[paths]
a = "./tmp/peer_b"
"#
        .to_string(),
    )
    .unwrap();
    thread::spawn(move || {
        iron_carrier::run(config_a);
    });
    thread::spawn(move || {
        iron_carrier::run(config_b);
    });

    thread::sleep(Duration::from_secs(5));

    assert_eq!(4, std::fs::read_dir("tmp/peer_a").unwrap().count());
    assert_eq!(4, std::fs::read_dir("tmp/peer_b").unwrap().count());

    for file in 1..=2 {
        assert_eq!(
            &contents_b[..],
            &std::fs::read(format!("tmp/peer_a/b_file_{}", file)).unwrap()[..]
        );

        assert_eq!(
            &contents_a[..],
            &std::fs::read(format!("tmp/peer_b/a_file_{}", file)).unwrap()[..]
        );
    }

    let _ = std::fs::write("tmp/peer_a/c_file_1", &contents_a);
    let _ = std::fs::remove_file("tmp/peer_a/a_file_1");
    let _ = std::fs::write("tmp/peer_a/c_file_2", b"some random content");

    thread::sleep(Duration::from_secs(2));

    append_content("tmp/peer_a/c_file_2", b" more content to the file");

    thread::sleep(Duration::from_secs(10));
    assert_eq!(
        &contents_a[..],
        &std::fs::read("./tmp/peer_b/c_file_1").unwrap()[..]
    );

    assert_eq!(
        b"some random content more content to the file",
        &std::fs::read("./tmp/peer_b/c_file_2").unwrap()[..]
    );
    assert!(!PathBuf::from("tmp/peer_b/a_file_1").exists());
}
