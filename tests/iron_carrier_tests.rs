use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::iter::Iterator;
use std::path::Path;
use std::time::SystemTime;
use std::{path::PathBuf, thread, time::Duration};

use iron_carrier::config::Config;
use rand::Rng;

fn prepare_files(peer_name: &str) -> [u8; 1024] {
    let _ = std::fs::remove_dir_all(format!("tmp/peer_{}", peer_name));
    let _ = std::fs::create_dir_all(format!("tmp/peer_{}", peer_name));

    let mut rng = rand::thread_rng();

    let mut file_content = [0u8; 1024];
    rng.fill(&mut file_content);

    for file in 1..=2 {
        let _ = std::fs::write(
            format!("tmp/peer_{}/{}_file_{}", peer_name, peer_name, file),
            &file_content,
        );
    }

    file_content
}

fn append_content<P: AsRef<Path>>(path: P, content: &[u8]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(content).unwrap();
}

fn enable_logs(verbosity: usize) {
    stderrlog::new()
        .verbosity(verbosity)
        .module("iron_carrier")
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();
}

#[test]
fn test_full_sync() {
    // enable_logs(2);
    let mut contents = HashMap::new();
    let mut port = 8090;
    for peer_name in ["a", "b", "c"] {
        contents.insert(peer_name, prepare_files(peer_name));
        let _ = fs::remove_file(format!("./tmp/peer_{}.log", peer_name));

        let config = iron_carrier::config::Config::new_from_str(format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
[paths]
a = "./tmp/peer_{}"
"#,
            port, peer_name, peer_name
        ))
        .expect("Failed to create config");

        port += 1;

        thread::spawn(move || {
            iron_carrier::run(config).expect("Iron carrier failed");
        });
    }

    thread::sleep(Duration::from_secs(5));

    assert_eq!(6, std::fs::read_dir("tmp/peer_a").unwrap().count());
    assert_eq!(6, std::fs::read_dir("tmp/peer_b").unwrap().count());
    assert_eq!(6, std::fs::read_dir("tmp/peer_c").unwrap().count());

    for peer_name in ["a", "b", "c"] {
        for file in 1..=2 {
            assert_eq!(
                &contents[peer_name][..],
                &std::fs::read(format!(
                    "tmp/peer_{}/{}_file_{}",
                    peer_name, peer_name, file
                ))
                .unwrap()[..]
            );
        }
    }
}

#[test]
fn test_partial_sync() {
    enable_logs(2);
    let mut port = 8090u16;
    for peer_name in ["a", "b", "c"] {
        let _ = fs::remove_dir_all(format!("./tmp/peer_{}", peer_name));
        let _ = fs::remove_file(format!("./tmp/peer_{}.log", peer_name));

        let config = format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
delay_watcher_events=1
[paths]
a = "./tmp/peer_{}"
"#,
            port, peer_name, peer_name
        );
        port += 1;

        let config = Config::new_from_str(config).unwrap();
        thread::spawn(move || {
            iron_carrier::run(config).expect("Carrier failed");
        });
    }

    thread::sleep(Duration::from_secs(5));

    let _ = std::fs::write("tmp/peer_a/new_file_1", b"some nice content for a new file");
    let _ = std::fs::write(
        "tmp/peer_a/new_file_2",
        b"some random content for another new file",
    );

    thread::sleep(Duration::from_secs(3));
    for peer_name in ["a", "b", "c"] {
        assert_eq!(
            b"some nice content for a new file",
            &std::fs::read(format!("./tmp/peer_{}/new_file_1", peer_name)).unwrap()[..]
        );
        assert_eq!(
            b"some random content for another new file",
            &std::fs::read(format!("./tmp/peer_{}/new_file_2", peer_name)).unwrap()[..]
        );
    }

    append_content("./tmp/peer_a/new_file_1", b" more content");
    append_content("./tmp/peer_b/new_file_2", b" more content for f2");

    thread::sleep(Duration::from_secs(3));

    for peer_name in ["a", "b", "c"] {
        assert_eq!(
            b"some nice content for a new file more content",
            &std::fs::read(format!("./tmp/peer_{}/new_file_1", peer_name)).unwrap()[..]
        );

        assert_eq!(
            b"some random content for another new file more content for f2",
            &std::fs::read(format!("./tmp/peer_{}/new_file_2", peer_name)).unwrap()[..]
        );
    }

    std::fs::remove_file("./tmp/peer_a/new_file_2").expect("failed to remove test file");
    thread::sleep(Duration::from_secs(3));
    for peer_name in ["a", "b", "c"] {
        assert!(!PathBuf::from(format!("./tmp/peer_{}/new_file_2", peer_name)).exists());
    }
}

#[test]
fn test_sync_deleted_files() {
    // enable_logs(2);
    let mut port = 8090u16;
    for peer_name in ["a", "b"] {
        fs::remove_dir_all(format!("./tmp/peer_{}", peer_name)).expect("Failed to cleanup tmp dir");
        fs::remove_file(format!("./tmp/peer_{}.log", peer_name))
            .expect("Faield to remove log file");
    }

    let log_line = format!(
        "{},FileDelete:a:deleted_file,Finished\n",
        SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5
    );
    let _ = std::fs::write("./tmp/peer_a.log", log_line.as_bytes());
    let _ = std::fs::write("./tmp/peer_b/deleted_file", b"this file will be deleted");

    for peer_name in ["a", "b"] {
        let config = format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
delay_watcher_events=1
[paths]
a = "./tmp/peer_{}"
"#,
            port, peer_name, peer_name
        );
        port += 1;

        let config = Config::new_from_str(config).unwrap();
        thread::spawn(move || {
            iron_carrier::run(config).expect("Carrier failed");
        });
    }

    thread::sleep(Duration::from_secs(5));

    for peer_name in ["a", "b"] {
        assert!(!PathBuf::from(format!("./tmp/peer_{}/deleted_file", peer_name)).exists());
    }
}
