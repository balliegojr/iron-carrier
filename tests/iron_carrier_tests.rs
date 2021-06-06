use std::time::Duration;

use rand::Rng;

async fn prepare_files() -> ([u8; 1024], [u8; 1024]) {
    let _ = tokio::fs::remove_dir_all("tmp/peer_a").await;
    let _ = tokio::fs::remove_dir_all("tmp/peer_b").await;

    let _ = tokio::fs::create_dir_all("tmp/peer_a").await;
    let _ = tokio::fs::create_dir_all("tmp/peer_b").await;

    let mut rng = rand::thread_rng();

    let mut contents_a = [0u8; 1024];
    rng.fill(&mut contents_a);

    let mut contents_b = [0u8; 1024];
    rng.fill(&mut contents_b);

    for file in 1..=5 {
        let _ = tokio::fs::write(format!("tmp/peer_a/a_file_{}", file), &contents_a).await;
        let _ = tokio::fs::write(format!("tmp/peer_b/b_file_{}", file), &contents_b).await;
    }

    (contents_a, contents_b)
}

#[tokio::test]
async fn test_iron_carrier() {
    let (contents_a, contents_b) = prepare_files().await;

    let config_a = iron_carrier::config::Config::new_from_str(
        r#"
port=8090
delay_watcher_events=1
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
[paths]
a = "./tmp/peer_b"
"#
        .to_string(),
    )
    .unwrap();

    tokio::spawn(async move { iron_carrier::run(config_a).await });
    tokio::spawn(async move { iron_carrier::run(config_b).await });

    tokio::time::sleep(Duration::from_secs(5)).await;

    assert_eq!(10, std::fs::read_dir("tmp/peer_a").unwrap().count());
    assert_eq!(10, std::fs::read_dir("tmp/peer_b").unwrap().count());

    for file in 1..=5 {
        assert_eq!(
            &contents_b[..],
            &std::fs::read(format!("tmp/peer_a/b_file_{}", file)).unwrap()[..]
        );

        assert_eq!(
            &contents_a[..],
            &std::fs::read(format!("tmp/peer_b/a_file_{}", file)).unwrap()[..]
        );
    }

    let _ = tokio::fs::write("tmp/peer_a/c_file_1", &contents_a).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_eq!(
        &contents_a[..],
        &std::fs::read("./tmp/peer_b/c_file_1").unwrap()[..]
    );
}
