use std::fs;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use carbon_rs::app::{App, now};
use carbon_rs::config::Config;
use carbon_rs::receiver::{tcp, udp};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;
use whisper_rs::Point;

fn test_app() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("schemas.conf");
    fs::write(&schema, "[default]\npattern = .*\nretentions = 1:600\n").unwrap();
    let mut config = Config::default();
    config.whisper.data_dir = dir.path().join("whisper").to_string_lossy().into_owned();
    config.whisper.schemas_file = schema.to_string_lossy().into_owned();
    config.dump.path = dir.path().join("dump").to_string_lossy().into_owned();
    config.tcp.max_line_bytes = 32;
    config.tcp.read_timeout = Duration::from_secs(2);
    config.udp.max_line_bytes = 32;
    (dir, App::new(config).unwrap())
}

async fn wait_for(app: &App, metric: &str, count: usize) {
    for _ in 0..100 {
        if app.cache.get(metric).len() == count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("metric {metric} did not reach {count} cached points");
}

#[tokio::test]
async fn tcp_handles_fragmentation_oversize_recovery_and_eof() {
    let (_dir, app) = test_app();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);
    let task = tokio::spawn(tcp(listener, app.clone(), app.config.tcp.clone(), stop_rx));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"tcp.good 1 ").await.unwrap();
    stream
        .write_all(format!("{}\n", now()).as_bytes())
        .await
        .unwrap();
    stream.write_all(b"x 1 1 this-is-invalid\n").await.unwrap();
    stream
        .write_all(b"012345678901234567890123456789012\n")
        .await
        .unwrap();
    stream
        .write_all(format!("tcp.recovered 2 {}\n", now()).as_bytes())
        .await
        .unwrap();
    stream.write_all(b"tcp.eof 3").await.unwrap();
    drop(stream);
    wait_for(&app, "tcp.good", 1).await;
    wait_for(&app, "tcp.recovered", 1).await;
    assert!(app.cache.get("tcp.eof").is_empty());
    assert!(app.invalid.load(Ordering::Relaxed) >= 2);
    stop_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn udp_accepts_a_final_line_without_newline() {
    let (_dir, app) = test_app();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);
    let task = tokio::spawn(udp(socket, app.clone(), app.config.udp.clone(), stop_rx));
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(format!("udp.final 4 {}", now()).as_bytes(), address)
        .await
        .unwrap();
    wait_for(&app, "udp.final", 1).await;
    stop_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[test]
fn cache_render_flush_scan_retry_and_dump_restore() {
    let (dir, app) = test_app();
    let timestamp = now();
    app.ingest(
        "live.metric".into(),
        Point {
            timestamp,
            value: 7.0,
        },
    )
    .unwrap();
    let rendered = app
        .fetch("live.metric", timestamp - 1, timestamp + 1, timestamp)
        .unwrap()
        .unwrap();
    assert!(rendered.values.contains(&Some(7.0))); // cache-only before a file exists
    let batch = app.cache.take().unwrap();
    assert_eq!(
        app.fetch("live.metric", timestamp - 1, timestamp + 1, timestamp)
            .unwrap()
            .unwrap()
            .values
            .iter()
            .filter(|x| **x == Some(7.0))
            .count(),
        1
    ); // in-flight remains visible
    assert!(app.cache.retry(batch.id));
    assert!(app.flush_one().unwrap());
    assert!(app.path("live.metric").unwrap().exists());
    app.scan().unwrap();
    assert!(app.index.get("live.metric").is_some());

    // A non-file at the target makes write fail after take; retry must retain its batch.
    fs::create_dir_all(app.path("failed.metric").unwrap()).unwrap();
    app.ingest(
        "failed.metric".into(),
        Point {
            timestamp,
            value: 1.0,
        },
    )
    .unwrap();
    assert!(app.flush_one().is_err());
    assert_eq!(
        app.cache.get("failed.metric"),
        vec![Point {
            timestamp,
            value: 1.0
        }]
    );

    app.ingest(
        "dump.metric".into(),
        Point {
            timestamp,
            value: 9.0,
        },
    )
    .unwrap();
    let dump = app.dump().unwrap();
    assert!(dump.exists());
    let (_second_dir, second) = test_app();
    fs::create_dir_all(&second.config.dump.path).unwrap();
    fs::copy(
        &dump,
        std::path::Path::new(&second.config.dump.path).join(dump.file_name().unwrap()),
    )
    .unwrap();
    assert_eq!(second.restore().unwrap().len(), 1);
    assert_eq!(
        second.cache.get("dump.metric"),
        vec![Point {
            timestamp,
            value: 9.0
        }]
    );
    drop(dir);
}
