#![allow(dead_code)]

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use std::{
    path::Path,
    time::{Duration, Instant},
};

use iron_carrier::config::Config;
use iron_carrier::leak::Leak;
use iron_carrier::validation::{Unvalidated, Validated};
use rand::distributions::{Alphanumeric, Standard};
use rand::Rng;

const FOLDERS: usize = 1;
const FILES_PER_FOLDER: usize = 2;

// Can't use an atomic for the ports, each test runs in a different process
pub const FULL_SYNC_PORT: u16 = 8090;
pub const PARTIAL_SYNC_PORT: u16 = 8095;
pub const FULL_SYNC_PRE_DELETED_PORT: u16 = 8100;
pub const TRUNCATE_PORT: u16 = 8105;
pub const FOLDER_OPERATION_PORT: u16 = 8110;
pub const FULL_SYNC_PORT_ENCRYPTED: u16 = 8115;

pub fn enable_logs() {
    let verbosity: usize = std::env::var("LOG_LEVEL")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .unwrap();

    if verbosity > 0 {
        stderrlog::new()
            .verbosity(verbosity)
            .modules(["iron_carrier", "iron_carrier_tests"])
            .timestamp(stderrlog::Timestamp::Second)
            .init()
            .unwrap();
    }
}

pub fn generate_configs(
    test_name: &str,
    initial_port: u16,
    nodes: u16,
    storages: usize,
    encryption_key: Option<&str>,
) -> Vec<&'static Validated<Config>> {
    assert!(nodes > 1);

    let final_port = initial_port + nodes;
    let peers = |node: u16| -> String {
        (initial_port..final_port)
            .filter(|p| *p != node)
            .map(|port| format!("\"127.0.0.1:{port}\""))
            .collect::<Vec<String>>()
            .join("\n,")
    };

    let storages = |node: u16| -> String {
        (0..storages)
            .map(|storage| {
                format!("storage_{storage} = \"/tmp/{test_name}/peer_{node}/storage_{storage}/\"")
            })
            .collect::<Vec<String>>()
            .join("\n")
    };

    let encryption = encryption_key
        .map(|key| format!(r#"encryption_key = "{key}""#))
        .unwrap_or_default();

    (initial_port..final_port)
        .map(|port| {
            let config = format!(
                r#"
node_id="peer_{port}"
group="{test_name}"
port={port}
log_path = "/tmp/{test_name}/peer_{port}.log"
delay_watcher_events=1
enable_service_discovery=false
{encryption}

peers = [
{}
]

[storages]
{}
"#,
                peers(port),
                storages(port)
            );

            config
                .parse::<Unvalidated<Config>>()
                .and_then(|config| config.validate())
                .unwrap()
                .leak()
        })
        .collect()
}

pub fn truncate_file<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();

    let cur_size = f.metadata()?.len();
    if cur_size > 0 {
        f.set_len(cur_size / 2)?;
        f.flush()?;
    }

    Ok(())
}

/// Append `content` to file in `path` if it exists
pub fn append_content<P: AsRef<Path>>(path: P, content: &[u8]) {
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    f.write_all(content).unwrap();
}

/// assert if the file at `path` have then contents of `content`
pub fn assert_file_content<P: AsRef<Path>>(path: P, content: &[u8]) {
    assert_file_exists(&path);

    let p = path.as_ref();
    if !timeout_check(move || &std::fs::read(p).unwrap()[..] == content) {
        panic!("file contents do not match {:?}", path.as_ref());
    }
}

/// assert if the file at `path` exists
pub fn assert_file_exists<P: AsRef<Path>>(path: P) {
    if !timeout_check(|| path.as_ref().exists()) {
        panic!("file {:?} does not exist", path.as_ref());
    }
}

/// assert if the file at `path` does not exist
pub fn assert_file_deleted<P: AsRef<Path>>(path: P) {
    if !timeout_check(|| !path.as_ref().exists()) {
        panic!("file {:?} still exists", path.as_ref());
    }
}

fn timeout_check<F: FnMut() -> bool>(mut check: F) -> bool {
    let limit = Instant::now() + Duration::from_secs(10);
    while limit > Instant::now() {
        if check() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    false
}

pub fn tree_compare<P: AsRef<Path>>(lhs: P, rhs: P) {
    let (l_files, l_ignored) = walk_path(lhs.as_ref());
    let (r_files, r_ignored) = walk_path(rhs.as_ref());

    assert!(l_ignored.is_disjoint(&r_ignored),);

    assert_eq!(
        l_files.len(),
        r_files.len(),
        "{:?} len() = {}, {:?} len() = {}",
        lhs.as_ref(),
        l_files.len(),
        rhs.as_ref(),
        r_files.len()
    );

    for (l, r) in l_files.into_iter().zip(r_files) {
        assert_eq!(l.file_name(), r.file_name());
        let l_metadata = l.metadata().unwrap();
        let r_metadata = r.metadata().unwrap();

        assert_eq!(
            l_metadata.len(),
            r_metadata.len(),
            "files sizes diverge for {l:?}",
        );
        assert_eq!(
            l_metadata
                .modified()
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            r_metadata
                .modified()
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            "modification time diverge for {l:?}"
        );

        assert_eq!(
            &std::fs::read(&l).unwrap()[..],
            &std::fs::read(&r).unwrap()[..],
            "contents diverge for {l:?}"
        )
    }
}

fn walk_path<P: AsRef<Path>>(root_path: P) -> (Vec<PathBuf>, HashSet<PathBuf>) {
    let mut paths = vec![root_path.as_ref().to_owned()];
    let mut files = Vec::new();
    let mut ignored = HashSet::new();

    while let Some(path) = paths.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_dir() {
                paths.push(path);
                continue;
            }

            if path
                .file_name()
                .map(|name| name == ".ignore")
                .unwrap_or_default()
            {
                continue;
            }

            if is_ignored(&path) {
                ignored.insert(path.strip_prefix(&root_path).unwrap().to_path_buf());
            } else {
                files.push(path);
            }
        }
    }

    files.sort();

    (files, ignored)
}
pub fn is_ignored(path: &Path) -> bool {
    path.extension().map(|ext| ext == "ig").unwrap_or_default()
        || path
            .parent()
            .and_then(|p| p.file_name())
            .map(|p| p.eq("ignored_folder"))
            .unwrap_or_default()
}
pub fn generate_files<P: AsRef<Path>>(path: P, prefix: &str) -> Vec<PathBuf> {
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::create_dir_all(&path);

    unchecked_generate_files(path, prefix)
}

pub fn unchecked_generate_files<P: AsRef<Path>>(path: P, prefix: &str) -> Vec<PathBuf> {
    let mut rng = rand::thread_rng();
    let mut files = Vec::new();

    gen_ignore_file_at(path.as_ref());
    let _ = std::fs::create_dir_all(path.as_ref().join("ignored_folder"));

    // files.push(gen_file_at(path.as_ref(), "zero", prefix, &mut rng, 0));
    for _ in 0..FILES_PER_FOLDER {
        files.push(gen_file_at(path.as_ref(), "rng", prefix, &mut rng, 512));
        files.push(gen_file_at(path.as_ref(), "ig", prefix, &mut rng, 256));

        files.push(gen_file_at(
            path.as_ref().join("ignored_folder"),
            "rng",
            prefix,
            &mut rng,
            128,
        ));
    }

    for _ in 0..FOLDERS {
        let folder = path.as_ref().join(get_name(&mut rng, 5));
        let _ = std::fs::create_dir_all(&folder);

        for _ in 0..FILES_PER_FOLDER {
            files.push(gen_file_at(&folder, "rng", prefix, &mut rng, 1024));
            files.push(gen_file_at(&folder, "ig", prefix, &mut rng, 512));
        }
    }

    files
}

fn gen_file_at<P: AsRef<Path>, R: Rng>(
    path: P,
    ext: &str,
    prefix: &str,
    rng: &mut R,
    file_length: usize,
) -> PathBuf {
    let file_name = format!("{prefix}_{}.{ext}", get_name(rng, 5));
    let file_path = path.as_ref().join(file_name);
    match file_length {
        0 => {
            let _ = std::fs::File::create(&file_path);
        }
        _ => gen_file_with_rnd_content(rng, &file_path, file_length),
    }
    file_path
}

fn gen_ignore_file_at<P: AsRef<Path>>(path: P) {
    let _ = std::fs::write(
        path.as_ref().join(".ignore"),
        r#"*.ig
**/*.ig
ignored_folder/**
"#,
    );
}

fn gen_file_with_rnd_content<P: AsRef<Path>, R: Rng>(
    rng: &mut R,
    file_path: P,
    file_length: usize,
) {
    let file_content: Vec<u8> = rng.sample_iter(Standard).take(file_length).collect();

    let _ = std::fs::write(&file_path, file_content);
    std::thread::sleep(Duration::from_millis(100))
}

fn get_name<T: Rng>(rng: &mut T, len: usize) -> String {
    rng.sample_iter(Alphanumeric)
        .map(char::from)
        .take(len)
        .collect()
}
