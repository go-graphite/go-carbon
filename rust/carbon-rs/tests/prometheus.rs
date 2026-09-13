use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use carbon_rs::{
    app::{App, now},
    config::Config,
    metrics::Metrics,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use whisper_rs::Point;

fn config(dir: &Path) -> Config {
    let schemas = dir.join("schemas.conf");
    fs::write(
        &schemas,
        "[default]\npattern = .*\nretentions = 1:600,10:600\n",
    )
    .unwrap();
    let mut config = Config::default();
    config.whisper.data_dir = dir.join("wsp").display().to_string();
    config.whisper.schemas_file = schemas.display().to_string();
    config.dump.path = dir.join("dump").display().to_string();
    config.prometheus.enabled = true;
    config
        .prometheus
        .labels
        .insert("cluster".into(), "test\"\\\ncluster".into());
    config.tcp.enabled = true;
    config.carbonserver.enabled = true;
    config.carbonserver.query_cache_size = 1 << 20;
    config
}

async fn serve(router: axum::Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (
        addr,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}

async fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[test]
fn application_schema_matches_pinned_go() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let metrics = Metrics::new(&config).unwrap();
    let server = metrics.carbonserver.as_ref().unwrap();
    server.requests.with_label_values(&["test", "test"]);
    server.cache_requests.with_label_values(&["test", "test"]);
    let mut process =
        Command::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/go-whisper-reference"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("build Go oracle: make -C rust reference");
    writeln!(
        process.stdin.take().unwrap(),
        "{}",
        json!({"op":"prometheus_schema", "labels":config.prometheus.labels})
    )
    .unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(output.status.success());
    let go: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(go.get("error").is_none(), "{go}");
    let rust: BTreeMap<_, _> = metrics.registry.gather().into_iter().filter(|f|
        !f.name().starts_with("process_") && f.name() != "carbon_rs_build_info")
        .map(|f| {
            let metric = &f.get_metric()[0];
            let mut labels: Vec<_> = metric.get_label().iter().map(|l| l.name()).collect();
            labels.sort();
            (f.name().to_owned(), json!({"help":f.help(), "type":format!("{:?}", f.get_field_type()),
                "labels":labels, "buckets":metric.get_histogram().get_bucket().iter().map(|b|b.upper_bound()).collect::<Vec<_>>()}))
        }).collect();
    assert_eq!(serde_json::to_value(rust).unwrap(), go["result"]);
    // Authorized Go omission fixes: nonzero cache counter and registered duration.
    server.cache_request("metric", true);
    server
        .cache_durations
        .with_label_values(&["work"])
        .observe(0.25);
    assert_eq!(
        server
            .cache_requests
            .with_label_values(&["metric", "true"])
            .get(),
        1
    );
    let families = metrics.registry.gather();
    let duration = families
        .iter()
        .find(|f| f.name() == "cache_duration_seconds_exp")
        .unwrap();
    assert_eq!(
        duration.get_metric()[0].get_histogram().get_sample_count(),
        1
    );
    assert_eq!(
        duration.get_metric()[0].get_histogram().get_bucket().len(),
        20
    );
}

#[test]
fn defaults_label_validation_and_optional_collectors() {
    let mut config = Config::default();
    assert!(!config.prometheus.enabled);
    assert_eq!(config.prometheus.endpoint, "/metrics");
    assert_eq!(config.pprof.listen, "127.0.0.1:7007");
    config.prometheus.enabled = true;
    for endpoint in [
        "", "metrics", "/{route}", "/a/*", "/:old", "/a?b", "/a#b", "/a\nb",
    ] {
        config.prometheus.endpoint = endpoint.into();
        assert!(config.prometheus.validate(false).is_err(), "{endpoint}");
    }
    config.prometheus.endpoint = "/custom/metrics".into();
    for label in ["", "1bad", "__internal", "bad-name", "le", "version"] {
        config.prometheus.labels = [(label.into(), "value".into())].into();
        assert!(Metrics::new(&config).is_err(), "{label}");
    }
    config.carbonserver.enabled = true;
    for label in ["code", "handler", "type", "hit"] {
        config.prometheus.labels = [(label.into(), "value".into())].into();
        assert!(Metrics::new(&config).is_err(), "{label}");
    }
    config.prometheus.labels.clear();
    config.carbonserver.enabled = false;
    let metrics = Metrics::new(&config).unwrap();
    assert!(metrics.carbonserver.is_none());
    assert!(metrics.tcp_received.is_none());
    assert!(
        !metrics
            .registry
            .gather()
            .iter()
            .any(|m| m.name() == "disk_requests_total")
    );
    #[cfg(target_os = "linux")]
    for name in [
        "process_cpu_seconds_total",
        "process_open_fds",
        "process_max_fds",
        "process_virtual_memory_bytes",
        "process_virtual_memory_max_bytes",
        "process_resident_memory_bytes",
        "process_start_time_seconds",
        "process_network_receive_bytes_total",
        "process_network_transmit_bytes_total",
    ] {
        assert!(
            metrics.registry.gather().iter().any(|m| m.name() == name),
            "{name}"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("config");
    fs::write(&file, "[prometheus]\nenabled=true\nendpoint='/custom'\n[prometheus.labels]\nregion='ams'\n[pprof]\nlisten='127.0.0.1:7100'\n").unwrap();
    let config = Config::load(&file).unwrap();
    assert_eq!(config.prometheus.labels["region"], "ams");
    assert_eq!(config.pprof.listen, "127.0.0.1:7100");
    assert_eq!(config.prometheus.endpoint, "/custom");
}

#[test]
fn storage_counters_measure_fetches_slots_and_attempted_writes() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(config(dir.path())).unwrap();
    let metrics = app.prometheus.as_ref().unwrap();
    let server = metrics.carbonserver.as_ref().unwrap();
    let at = now();
    for timestamp in [at - 2, at + 3600] {
        app.ingest(
            "a.b".into(),
            Point {
                timestamp,
                value: 2.0,
            },
        )
        .unwrap();
    }
    let series = app.fetch("a.b", at - 10, at, at).unwrap().unwrap();
    assert_eq!(server.disk_requests.get(), 0);
    assert_eq!(server.returned_points.get(), series.values.len() as u64);
    assert_eq!(
        server
            .cache_requests
            .with_label_values(&["metric", "true"])
            .get(),
        1
    );
    app.fetch("a.b", at - 30, at - 20, at).unwrap();
    assert_eq!(
        server
            .cache_requests
            .with_label_values(&["metric", "false"])
            .get(),
        1
    );
    app.flush_one().unwrap();
    assert_eq!(metrics.write_lag.get_sample_count(), 2);
    assert!(metrics.write_lag.get_sample_sum() < 0.0); // Future samples remain negative.
    let wait = server
        .cache_durations
        .with_label_values(&["wait"])
        .get_sample_count();
    app.fetch("a.b", at - 1000, at, at).unwrap(); // Coarse archive: no cache instrumentation.
    assert_eq!(
        server
            .cache_durations
            .with_label_values(&["wait"])
            .get_sample_count(),
        wait
    );
    app.fetch("a.b", at - 10, at, at).unwrap();
    assert_eq!(server.disk_requests.get(), 2);
    assert_eq!(server.disk_wait.get_sample_count(), 2);
    assert_eq!(server.returned_metrics.get(), 4);
    assert!(app.fetch("missing", at - 10, at, at).is_err());
    assert_eq!(server.disk_requests.get(), 2);
    assert_eq!(server.returned_metrics.get(), 4);
    app.ingest(
        "failed.write".into(),
        Point {
            timestamp: at,
            value: 1.0,
        },
    )
    .unwrap();
    fs::create_dir_all(app.path("failed.write").unwrap()).unwrap();
    assert!(app.flush_one().is_err());
    assert_eq!(metrics.write_lag.get_sample_count(), 2); // Failed open never reaches UpdateMany.
}

#[tokio::test]
async fn tcp_counts_parsed_points_before_admission_but_not_udp_or_invalid_lines() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.cache.max_size = 1;
    let app = App::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(carbon_rs::receiver::tcp(
        listener,
        app.clone(),
        app.config.tcp.clone(),
        stop_rx.clone(),
    ));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "tcp.first 1 {}\ninvalid\ntcp.dropped 2 {}\npartial 3",
                now(),
                now()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    drop(stream);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let udp = tokio::spawn(carbon_rs::receiver::udp(
        socket,
        app.clone(),
        app.config.udp.clone(),
        stop_rx,
    ));
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(format!("udp.dropped 1 {}", now()).as_bytes(), addr)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while app.rejected.load(Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(app.received.load(Ordering::Relaxed), 1);
    assert_eq!(app.invalid.load(Ordering::Relaxed), 1);
    assert_eq!(
        app.prometheus
            .as_ref()
            .unwrap()
            .tcp_received
            .as_ref()
            .unwrap()
            .get(),
        2
    );
    stop_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
    udp.await.unwrap().unwrap();
}

#[tokio::test]
async fn http_instrumentation_and_separate_custom_scrape_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.prometheus.endpoint = "/custom/metrics".into();
    let app = App::new(config).unwrap();
    let at = now();
    app.ingest(
        "a.b".into(),
        Point {
            timestamp: at - 2,
            value: 2.0,
        },
    )
    .unwrap();
    let metrics = app.prometheus.as_ref().unwrap();
    let server = metrics.carbonserver.as_ref().unwrap();
    let (http, task) = serve(carbon_rs::http::router(app.clone())).await;
    let (scrape, scrape_task) = serve(carbon_rs::metrics::router(app.clone())).await;
    for _ in 0..2 {
        assert!(
            get(http, "/metrics/find/?query=a.*&query=a.b")
                .await
                .starts_with("HTTP/1.1 200")
        );
    }
    assert_eq!(
        server
            .cache_requests
            .with_label_values(&["find", "false"])
            .get(),
        1
    );
    assert_eq!(
        server
            .cache_requests
            .with_label_values(&["find", "true"])
            .get(),
        1
    );
    for (path, code, handler) in [
        ("/metrics/find/", "400", "/metrics/find"),
        ("/info/?target=missing", "404", "/info"),
    ] {
        assert!(
            get(http, path)
                .await
                .starts_with(&format!("HTTP/1.1 {code}"))
        );
        assert_eq!(server.requests.with_label_values(&[code, handler]).get(), 1);
    }
    let render = format!("/render/?target=a.b&from={}&until={at}", at - 10);
    for _ in 0..4 {
        assert!(get(http, &render).await.starts_with("HTTP/1.1 200"));
        if server
            .cache_requests
            .with_label_values(&["query", "true"])
            .get()
            > 0
        {
            break;
        }
    }
    assert!(
        server
            .cache_requests
            .with_label_values(&["query", "true"])
            .get()
            > 0
    );
    assert_eq!(
        server.returned_metrics.get(),
        server
            .cache_requests
            .with_label_values(&["query", "false"])
            .get()
    );
    let requests = server.durations.get_sample_count();
    for path in ["/metrics", "/random/client/path", "/admin/info"] {
        get(http, path).await;
    }
    assert_eq!(server.durations.get_sample_count(), requests);
    assert!(get(scrape, "/metrics").await.starts_with("HTTP/1.1 404"));
    let body = get(scrape, "/custom/metrics").await;
    assert!(body.starts_with("HTTP/1.1 200"));
    assert!(body.contains("text/plain; version=0.0.4"));
    assert!(body.contains("# TYPE cache_duration_seconds_exp histogram"));
    assert!(!body.contains("carbon_rs_received_total"));
    assert!(!body.contains("go_goroutines"));
    for line in body.lines().filter(|line| {
        line.starts_with("cache_")
            || line.starts_with("http_")
            || line.starts_with("out_of_order_")
            || line.starts_with("carbon_rs_build_info")
            || line.starts_with("process_")
    }) {
        assert!(
            line.contains("cluster=\"test\\\"\\\\\\ncluster\""),
            "{line}"
        );
    }
    task.abort();
    scrape_task.abort();
    let mut config = app.config.clone();
    config.prometheus.enabled = false;
    let app = App::new(config).unwrap();
    assert!(app.prometheus.is_none());
    let (scrape, task) = serve(carbon_rs::metrics::router(app)).await;
    assert!(
        get(scrape, "/custom/metrics")
            .await
            .starts_with("HTTP/1.1 404")
    );
    task.abort();
}
