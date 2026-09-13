use carbon_rs::{
    app::{App, now},
    config::Config,
    receiver,
};
use std::fs;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let metrics = argument("--metrics", 100);
    let points = argument("--points", 100);
    if !(1..=10_000).contains(&metrics) || !(1..=3000).contains(&points) {
        return Err(std::io::Error::other(
            "metrics must be 1..10000 and points 1..3000",
        ));
    }
    let root = tempfile::tempdir()?;
    let schema = root.path().join("schemas.conf");
    fs::write(&schema, "[default]\npattern = .*\nretentions = 1:3600\n")?;
    let mut config = Config::default();
    config.whisper.data_dir = root.path().join("whisper").to_string_lossy().into_owned();
    config.whisper.schemas_file = schema.to_string_lossy().into_owned();
    config.cache.max_size = 50_000;
    let app = App::new(config)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let receiver = tokio::spawn(receiver::tcp(
        listener,
        app.clone(),
        app.config.tcp.clone(),
        stop_rx.clone(),
    ));
    let writer_app = app.clone();
    let mut writer_stop = stop_rx;
    let writer = tokio::spawn(async move {
        while !*writer_stop.borrow() {
            let app = writer_app.clone();
            if !tokio::task::spawn_blocking(move || app.flush_one())
                .await
                .map_err(std::io::Error::other)??
            {
                tokio::select! { _ = writer_stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(1)) => {} }
            }
        }
        Ok::<_, std::io::Error>(())
    });
    let started = Instant::now();
    let start = now() - points as i64 - 2;
    let mut stream = TcpStream::connect(address).await?;
    for point in 0..points {
        for metric in 0..metrics {
            stream
                .write_all(
                    format!("bench.metric.{metric} {point} {}\n", start + point as i64).as_bytes(),
                )
                .await?;
        }
    }
    drop(stream);
    let expected = metrics * points;
    while app.received.load(Ordering::Relaxed) < expected as u64 || !app.cache.is_empty() {
        if writer.is_finished()
            || started.elapsed() > Duration::from_secs(60)
            || app.cache.stats().dropped_points > 0
        {
            stop_tx.send_replace(true);
            receiver.abort();
            writer.abort();
            return Err(std::io::Error::other(
                "benchmark failed: persister stopped, cache dropped points, or drain exceeded 60 seconds",
            ));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let elapsed = started.elapsed();
    stop_tx.send_replace(true);
    receiver.await??;
    writer.await??;
    let mut stored = 0;
    for metric in 0..metrics {
        let series = app
            .fetch(
                &format!("bench.metric.{metric}"),
                start - 1,
                start + points as i64 - 1,
                now(),
            )?
            .expect("stored metric missing");
        assert_eq!(
            series.values,
            (0..points).map(|p| Some(p as f64)).collect::<Vec<_>>()
        );
        stored += series.values.len();
    }
    let stats = app.cache.stats();
    let accepted = app.received.load(Ordering::Relaxed);
    let invalid = app.invalid.load(Ordering::Relaxed);
    println!(
        "input={expected} accepted={accepted} stored_unique={stored} dropped={} invalid={invalid} cache_highwater={} elapsed={:.3}s rate={:.0}/s",
        stats.dropped_points,
        stats.high_water_points,
        elapsed.as_secs_f64(),
        accepted as f64 / elapsed.as_secs_f64()
    );
    assert_eq!(stored, expected);
    assert_eq!(accepted as usize, expected);
    assert_eq!(stats.dropped_points, 0);
    assert_eq!(invalid, 0);
    assert!(app.cache.is_empty());
    Ok(())
}

fn argument(name: &str, default: usize) -> usize {
    std::env::args()
        .skip_while(|arg| arg != name)
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
