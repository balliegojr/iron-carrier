use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::iter::Iterator;
use std::path::Path;
use std::time::{Instant, SystemTime};
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
        .modules(["iron_carrier", "iron_carrier_tests"])
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();
}

#[test]
fn test_full_sync() {
    // enable_logs(5);
    let mut contents = HashMap::new();
    let mut port = 8090;
    let peers = ["a", "b", "c"];
    for peer_name in peers {
        contents.insert(peer_name, prepare_files(peer_name));
        let _ = fs::remove_file(format!("./tmp/peer_{}.log", peer_name));

        let config = iron_carrier::config::Config::new_from_str(format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
[paths]
full_sync = "./tmp/peer_{}"
"#,
            port, peer_name, peer_name
        ))
        .expect("Failed to create config");

        port += 1;

        thread::spawn(move || {
            iron_carrier::run(config).expect("Iron carrier failed");
        });
    }

    thread::sleep(Duration::from_secs(8));

    for peer_name in peers {
        assert_eq!(
            6,
            std::fs::read_dir(format!("tmp/peer_{}", peer_name))
                .unwrap()
                .count()
        );
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
fn test_partial_sync() -> Result<(), Box<dyn std::error::Error>> {
    // enable_logs(5);

    let [peer_1, peer_2, peer_3] = ["d", "e", "f"];

    let init_peer = |peer_name: &str, port: u16| {
        let config = format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
delay_watcher_events=1
[paths]
part_sync = "./tmp/peer_{}"
"#,
            port, peer_name, peer_name
        );

        let config = Config::new_from_str(config).unwrap();
        thread::spawn(move || {
            iron_carrier::run(config).expect("Carrier failed");
        });
    };

    // cleanup from prev executions
    for peer_name in [peer_1, peer_2, peer_3] {
        let _ = fs::remove_dir_all(format!("./tmp/peer_{}", peer_name));
        let _ = fs::remove_file(format!("./tmp/peer_{}.log", peer_name));
    }

    init_peer(peer_1, 8095);
    init_peer(peer_2, 8096);

    std::fs::write(
        format!("./tmp/peer_{}/pre_existent", peer_1),
        b"file created",
    )?;
    check_file_contents(
        &format!("./tmp/peer_{}/pre_existent", peer_2),
        b"file created",
    );

    // create first file
    log::debug!("Writing files for write events");
    std::fs::write(format!("./tmp/peer_{peer_1}/c_{peer_1}"), b"file created")?;

    check_file_contents(&format!("./tmp/peer_{peer_2}/c_{peer_1}"), b"file created");

    // update first file
    append_content(format!("./tmp/peer_{peer_1}/c_{peer_1}"), b" update_1");
    check_file_contents(
        &format!("./tmp/peer_{peer_2}/c_{peer_1}"),
        b"file created update_1",
    );

    // update second file
    std::fs::write(format!("tmp/peer_{peer_2}/c_{peer_2}"), b"file created")?;
    check_file_contents(&format!("./tmp/peer_{peer_1}/c_{peer_2}"), b"file created");

    // move file
    std::fs::rename(
        format!("tmp/peer_{peer_1}/c_{peer_1}"),
        format!("tmp/peer_{peer_1}/m_{peer_1}"),
    )?;

    std::fs::rename(
        format!("tmp/peer_{peer_2}/c_{peer_2}"),
        format!("tmp/peer_{peer_2}/m_{peer_2}"),
    )?;

    check_file_contents(
        &format!("./tmp/peer_{peer_2}/m_{peer_1}"),
        b"file created update_1",
    );
    check_file_contents(&format!("./tmp/peer_{peer_1}/m_{peer_2}"), b"file created");

    std::fs::remove_file(&format!("./tmp/peer_{peer_1}/m_{peer_1}"))?;
    std::fs::remove_file(&format!("./tmp/peer_{peer_2}/m_{peer_2}"))?;

    check_file_deleted(&format!("./tmp/peer_{peer_1}/m_{peer_1}"));
    check_file_deleted(&format!("./tmp/peer_{peer_1}/m_{peer_2}"));
    check_file_deleted(&format!("./tmp/peer_{peer_2}/m_{peer_1}"));
    check_file_deleted(&format!("./tmp/peer_{peer_2}/m_{peer_2}"));

    init_peer(peer_3, 8097);
    std::thread::sleep(Duration::from_secs(5));

    check_file_contents(
        &format!("./tmp/peer_{peer_3}/pre_existent"),
        b"file created",
    );

    std::fs::write(format!("./tmp/peer_{peer_1}/c_{peer_1}"), b"file created")?;
    for p in [peer_1, peer_2, peer_3] {
        check_file_contents(&format!("./tmp/peer_{p}/c_{peer_1}"), b"file created");
    }

    append_content(format!("./tmp/peer_{peer_2}/c_{peer_1}"), b" update_1");
    for p in [peer_1, peer_2, peer_3] {
        check_file_contents(
            &format!("./tmp/peer_{p}/c_{peer_1}"),
            b"file created update_1",
        );
    }

    std::fs::rename(
        format!("tmp/peer_{peer_3}/c_{peer_1}"),
        format!("tmp/peer_{peer_3}/m_"),
    )?;

    for p in [peer_1, peer_2, peer_3] {
        check_file_contents(&format!("./tmp/peer_{p}/m_"), b"file created update_1");
    }

    std::fs::remove_file(&format!("./tmp/peer_{peer_1}/m_"))?;

    for p in [peer_1, peer_2, peer_3] {
        check_file_deleted(&format!("./tmp/peer_{p}/m_"));
    }
    Ok(())
}

#[test]
fn test_sync_deleted_files() {
    // enable_logs(5);
    let mut port = 8100u16;
    let peers = ["g", "h"];
    for peer_name in peers {
        let _ = fs::remove_dir_all(format!("./tmp/peer_{}", peer_name));
        let _ = fs::remove_file(format!("./tmp/peer_{}.log", peer_name));
    }

    let log_line = format!(
        "{},FileDelete:a:deleted_file,Finished\n",
        SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5
    );
    let _ = std::fs::write("./tmp/peer_g.log", log_line.as_bytes());
    let _ = std::fs::write("./tmp/peer_h/deleted_file", b"this file will be deleted");

    for peer_name in peers {
        let config = format!(
            r#"
port={}
log_path = "./tmp/peer_{}.log"
delay_watcher_events=1
[paths]
sync_deleted = "./tmp/peer_{}"
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

    for peer_name in peers {
        assert!(!PathBuf::from(format!("./tmp/peer_{}/deleted_file", peer_name)).exists());
    }
}

fn check_file_contents(path: &str, content: &[u8]) {
    let limit = Instant::now() + Duration::from_secs(20);

    let path = Path::new(path);
    while limit > Instant::now() {
        if path.exists() {
            if &std::fs::read(path).unwrap()[..] == content {
                return;
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    if !path.exists() {
        panic!("file does not exist {:?}", path);
    }

    panic!("file contents do not match {:?}", path);
}

fn check_file_deleted(path: &str) {
    let limit = Instant::now() + Duration::from_secs(10);
    let path = Path::new(path);
    while limit > Instant::now() {
        if !path.exists() {
            return;
        }
    }

    panic!("file {:?} still exists", path);
}
