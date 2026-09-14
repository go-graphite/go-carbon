//! Periodic Go-style Graphite self-metrics, independent of Prometheus exposition.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use prometheus::core::Collector as _;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};
use whisper_rs::Point;

use crate::{
    app::{App, invalid, now},
    config::Common,
};

#[derive(Default)]
pub(crate) struct ReceiverStats {
    pub received: AtomicU64,
    pub errors: AtomicU64,
    pub active: AtomicU64,
}

#[derive(Default)]
pub(crate) struct Stats {
    pub tcp: ReceiverStats,
    pub udp: ReceiverStats,
    pub created: AtomicU64,
    pub updates: AtomicU64,
    pub committed: AtomicU64,
    pub ooo_discarded: AtomicU64,
    pub ooo_diverted: AtomicU64,
    pub ooo_compactions: AtomicU64,
    pub ooo_compact_errors: AtomicU64,
    pub scan_ns: AtomicU64,
}

pub(crate) struct Active<'a>(&'a AtomicU64);
impl<'a> Active<'a> {
    pub(crate) fn new(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, Relaxed);
        Self(counter)
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

#[derive(Debug, PartialEq)]
enum Endpoint {
    Local,
    Tcp(String),
    Udp(String),
}
impl Endpoint {
    fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() || value == "local" {
            return Ok(Self::Local);
        }
        let (scheme, address) = value
            .split_once("://")
            .ok_or("common.metric-endpoint must be local, tcp://host:port or udp://host:port")?;
        let authority = address
            .parse::<axum::http::uri::Authority>()
            .map_err(|_| "invalid common.metric-endpoint address")?;
        if authority.host().is_empty()
            || authority.port_u16().is_none_or(|port| port == 0)
            || address.contains('@')
        {
            return Err(
                "common.metric-endpoint requires a host and nonzero port, without credentials"
                    .into(),
            );
        }
        match scheme {
            "tcp" => Ok(Self::Tcp(address.into())),
            "udp" => Ok(Self::Udp(address.into())),
            _ => Err("common.metric-endpoint supports only tcp and udp".into()),
        }
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 4090
        && !name
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '/' | '\\'))
        && name
            .split('.')
            .all(|part| !part.is_empty() && part.len() <= 251)
}

pub(crate) fn validate(common: &Common) -> Result<(), String> {
    Endpoint::parse(&common.metric_endpoint)?;
    if common.metric_interval.is_zero() {
        return Err("common.metric-interval must be positive".into());
    }
    if !valid_name(&common.graph_prefix.replace("{host}", "localhost")) {
        return Err("common.graph-prefix must be a nonempty Graphite metric path".into());
    }
    Ok(())
}

fn prefix(template: &str, hostname: &str) -> String {
    template.replace("{host}", &hostname.trim().replace('.', "_"))
}

fn hostname() -> String {
    // No extra runtime crate: procfs on Linux, the Unix hostname utility elsewhere.
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| {
            let output = std::process::Command::new("hostname").output().ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8(output.stdout).ok())
                .flatten()
        })
        .filter(|host| !host.trim().is_empty())
        .unwrap_or_else(|| "localhost".into())
}

#[derive(Default)]
struct Collector {
    previous: BTreeMap<String, u64>,
}
impl Collector {
    // Never reset shared counters: Prometheus and administrative readers stay cumulative.
    fn delta(&mut self, name: &str, total: u64) -> f64 {
        total.wrapping_sub(self.previous.insert(name.into(), total).unwrap_or(0)) as f64
    }

    fn collect(&mut self, app: &App, mut send: impl FnMut(&str, f64)) {
        let cache = app.cache.stats();
        let (metrics, unconfirmed, queries) = app.cache.graphite_counts();
        for (name, value) in [
            ("cache.size", cache.pending_points),
            ("cache.metrics", metrics),
            ("cache.notConfirmed", unconfirmed as u64),
            ("cache.maxSize", app.config.cache.max_size),
            (
                "persister.workers",
                app.config.whisper.workers.max(1) as u64,
            ),
        ] {
            send(name, value as f64);
        }
        for (name, total) in [
            ("cache.queries", queries),
            ("cache.overflow", cache.dropped_points),
            ("persister.created", app.graphite.created.load(Relaxed)),
            ("persister.errors", app.write_errors.load(Relaxed)),
            (
                "persister.oooDiscardedPoints",
                app.graphite.ooo_discarded.load(Relaxed),
            ),
        ] {
            send(name, self.delta(name, total));
        }
        if app.config.whisper.out_of_order {
            for (name, counter) in [
                ("persister.oooDiverted", &app.graphite.ooo_diverted),
                ("persister.oooCompactions", &app.graphite.ooo_compactions),
                (
                    "persister.oooCompactErrors",
                    &app.graphite.ooo_compact_errors,
                ),
            ] {
                send(name, self.delta(name, counter.load(Relaxed)));
            }
        }
        let updates = self.delta(
            "persister.updateOperations",
            app.graphite.updates.load(Relaxed),
        );
        let points = self.delta(
            "persister.committedPoints",
            app.graphite.committed.load(Relaxed),
        );
        send("persister.updateOperations", updates);
        send("persister.committedPoints", points);
        send(
            "persister.pointsPerUpdate",
            if updates > 0.0 { points / updates } else { 0.0 },
        );
        for (name, enabled, stats) in [
            ("tcp", app.config.tcp.enabled, &app.graphite.tcp),
            ("udp", app.config.udp.enabled, &app.graphite.udp),
        ] {
            if enabled {
                for (suffix, counter) in [
                    ("metricsReceived", &stats.received),
                    ("errors", &stats.errors),
                ] {
                    let name = format!("{name}.{suffix}");
                    send(&name, self.delta(&name, counter.load(Relaxed)));
                }
                if name == "tcp" {
                    send("tcp.active", stats.active.load(Relaxed) as f64);
                }
            }
        }
        let Some(server) = &app.metrics.carbonserver else {
            return;
        };
        send(
            "carbonserver.metrics_known",
            app.index.metric_count() as f64,
        );
        send(
            "carbonserver.inflight_requests_count",
            server.inflight.load(Relaxed) as f64,
        );
        send(
            "carbonserver.inflight_requests_limit",
            app.config.carbonserver.concurrent_requests as f64,
        );
        for (name, total) in [
            ("disk_requests", server.disk_requests.get()),
            ("metrics_returned", server.returned_metrics.get()),
            ("points_returned", server.returned_points.get()),
            ("file_scan_time_ns", app.graphite.scan_ns.load(Relaxed)),
        ] {
            let name = format!("carbonserver.{name}");
            send(&name, self.delta(&name, total));
        }
        for (kind, prefix) in [
            ("metric", "cache"),
            ("query", "query_cache"),
            ("find", "find_cache"),
        ] {
            for (hit, suffix) in [("true", "hit"), ("false", "miss")] {
                let name = format!("carbonserver.{prefix}_{suffix}");
                send(
                    &name,
                    self.delta(
                        &name,
                        server.cache_requests.with_label_values(&[kind, hit]).get(),
                    ),
                );
            }
        }
        for (name, seconds) in [
            ("disk_wait_time_ns", server.disk_wait.get_sample_sum()),
            (
                "cache_wait_time_fetch_ns",
                server
                    .cache_durations
                    .with_label_values(&["wait"])
                    .get_sample_sum(),
            ),
            (
                "cache_work_time_ns",
                server
                    .cache_durations
                    .with_label_values(&["work"])
                    .get_sample_sum(),
            ),
        ] {
            let name = format!("carbonserver.{name}");
            send(&name, self.delta(&name, (seconds * 1e9) as u64));
        }
        let requests = ["true", "false"]
            .iter()
            .map(|hit| {
                server
                    .cache_requests
                    .with_label_values(&["metric", hit])
                    .get()
            })
            .sum();
        send(
            "carbonserver.cache_requests",
            self.delta("carbonserver.cache_requests", requests),
        );
        let mut requests = BTreeMap::<String, u64>::new();
        for name in [
            "render_requests",
            "find_requests",
            "list_requests",
            "details_requests",
            "notfound",
            "rejected_too_many_requests",
        ] {
            requests.insert(name.into(), 0);
        }
        for handler in [
            "combined",
            "render",
            "find",
            "list",
            "details",
            "info",
            "capabilities",
        ] {
            for class in 1..=5 {
                requests.insert(format!("request_codes.{handler}.{class}xx"), 0);
            }
        }
        for family in server.requests.collect() {
            for metric in family.get_metric() {
                let label = |name| {
                    metric
                        .get_label()
                        .iter()
                        .find(|l| l.name() == name)
                        .map(|l| l.value())
                        .unwrap_or("")
                };
                let count = metric.get_counter().value() as u64;
                let handler = match label("handler") {
                    "/render" => "render",
                    "/metrics/find" => "find",
                    "/metrics/list" | "/metrics/list_query" => "list",
                    "/metrics/details" => "details",
                    "/info" => "info",
                    "/_internal/capabilities" => "capabilities",
                    _ => continue,
                };
                if let Some(total) = requests.get_mut(&format!("{handler}_requests")) {
                    *total += count;
                }
                if let Some(class) = label("code").chars().next() {
                    *requests
                        .entry(format!("request_codes.{handler}.{class}xx"))
                        .or_default() += count;
                    *requests
                        .entry(format!("request_codes.combined.{class}xx"))
                        .or_default() += count;
                }
                for (code, name) in [("404", "notfound"), ("429", "rejected_too_many_requests")] {
                    if label("code") == code {
                        *requests.entry(name.into()).or_default() += count;
                    }
                }
            }
        }
        for (name, total) in requests {
            let name = format!("carbonserver.{name}");
            send(&name, self.delta(&name, total));
        }
    }
}

/// The collector has no listener of its own. Stop it before draining/dumping the cache.
pub async fn run(app: Arc<App>, stop: watch::Receiver<bool>) -> io::Result<()> {
    let endpoint = Endpoint::parse(&app.config.common.metric_endpoint).map_err(invalid)?;
    // hostname() may fork a subprocess; keep that off the async worker.
    let host = tokio::task::spawn_blocking(hostname)
        .await
        .map_err(io::Error::other)?;
    let prefix = prefix(&app.config.common.graph_prefix, &host);
    if !valid_name(&prefix) {
        return Err(invalid("invalid expanded common.graph-prefix"));
    }
    tracing::info!(target: "stat", prefix, endpoint = ?endpoint,
        interval_seconds = app.config.common.metric_interval.as_secs_f64(), "self-metrics started");
    let (tx, rx) = mpsc::channel(4096);
    let local = endpoint == Endpoint::Local;
    let collecting = collect_loop(app, prefix, local, tx, stop.clone());
    if local {
        collecting.await
    } else {
        let (result, ()) = tokio::join!(collecting, send_loop(endpoint, rx, stop));
        result
    }
}

async fn collect_loop(
    app: Arc<App>,
    prefix: String,
    local: bool,
    tx: mpsc::Sender<String>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let period = app.config.common.metric_interval;
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut collector = Collector::default();
    while !*stop.borrow() {
        tokio::select! { biased; _ = stop.changed() => break, _ = ticks.tick() => {} }
        let (app, prefix, tx, stop) = (app.clone(), prefix.clone(), tx.clone(), stop.clone());
        collector = tokio::task::spawn_blocking(move || {
            let timestamp = now();
            let mut dropped = 0;
            collector.collect(&app, |name, value| {
                if *stop.borrow() { return; }
                let name = format!("{prefix}.{name}");
                if !valid_name(&name) { dropped += 1; return; }
                let failed = if local {
                    app.ingest(name, Point { timestamp, value }).is_err()
                } else {
                    tx.try_send(format!("{name} {value} {timestamp}\n")).is_err()
                };
                if failed { dropped += 1; }
            });
            if dropped > 0 {
                tracing::warn!(target: "stat", dropped, "self-metrics dropped: invalid path, admission failure or full send queue");
            }
            collector
        }).await.map_err(io::Error::other)?;
    }
    Ok(())
}

async fn send_loop(
    endpoint: Endpoint,
    mut rx: mpsc::Receiver<String>,
    mut stop: watch::Receiver<bool>,
) {
    let chunk_size = if matches!(endpoint, Endpoint::Udp(_)) {
        1000
    } else {
        32768
    };
    let mut pending = None;
    while !*stop.borrow() {
        let mut chunk = match pending.take() {
            Some(line) => line,
            None => {
                tokio::select! { biased; _ = stop.changed() => return, line = rx.recv() => match line { Some(line) => line, None => return } }
            }
        };
        let deadline = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(deadline);
        while chunk.len() < chunk_size {
            let line = tokio::select! { biased; _ = stop.changed() => return, _ = &mut deadline => break, line = rx.recv() => match line { Some(line) => line, None => break } };
            if chunk.len() + line.len() > chunk_size {
                pending = Some(line);
                break;
            }
            chunk.push_str(&line);
        }
        loop {
            let result = tokio::select! {
                biased;
                _ = stop.changed() => return,
                result = tokio::time::timeout(Duration::from_secs(5), send_chunk(&endpoint, chunk.as_bytes())) => result,
            };
            if matches!(result, Ok(Ok(()))) {
                break;
            }
            tracing::error!(target: "stat", endpoint = ?endpoint, error = ?result, "self-metrics delivery failed; retrying");
            tokio::select! { biased; _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
        }
    }
}

async fn send_chunk(endpoint: &Endpoint, bytes: &[u8]) -> io::Result<()> {
    match endpoint {
        Endpoint::Tcp(address) => {
            let mut socket = TcpStream::connect(address).await?;
            socket.write_all(bytes).await?;
            socket.shutdown().await
        }
        Endpoint::Udp(address) => {
            let mut last = io::Error::other("metric endpoint resolved to no addresses");
            for address in tokio::net::lookup_host(address).await? {
                let result = async {
                    let socket = UdpSocket::bind(if address.is_ipv4() {
                        "0.0.0.0:0"
                    } else {
                        "[::]:0"
                    })
                    .await?;
                    let sent = socket.send_to(bytes, address).await?;
                    if sent != bytes.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "partial UDP datagram",
                        ));
                    }
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => return Ok(()),
                    Err(error) => last = error,
                }
            }
            Err(last)
        }
        Endpoint::Local => unreachable!("local samples go directly to admission"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn app(dir: &std::path::Path, prometheus: bool) -> Arc<App> {
        let schemas = dir.join("schemas");
        std::fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:600\n").unwrap();
        let mut config = Config::default();
        config.whisper.schemas_file = schemas.display().to_string();
        config.whisper.data_dir = dir.join("wsp").display().to_string();
        config.common.graph_prefix = "test.agent".into();
        config.common.metric_interval = Duration::from_millis(200);
        config.tcp.enabled = true;
        config.udp.enabled = true;
        config.carbonserver.enabled = true;
        config.prometheus.enabled = prometheus;
        App::new(config).unwrap()
    }

    fn snapshot(collector: &mut Collector, app: &App) -> BTreeMap<String, f64> {
        let mut values = BTreeMap::new();
        collector.collect(app, |name, value| {
            values.insert(name.into(), value);
        });
        values
    }

    /// Like Go, compaction rides on the write path: a flush merges the sidecar once it reaches
    /// the size threshold, at most `out-of-order-compact-rate` merges per second.
    #[test]
    fn flush_compacts_sidecar_at_threshold_within_rate_budget() {
        for (threshold, compacted) in [(1u64, true), (1 << 40, false)] {
            let dir = tempfile::tempdir().unwrap();
            let schemas = dir.path().join("schemas");
            std::fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:600\n").unwrap();
            let mut config = Config::default();
            config.whisper.schemas_file = schemas.display().to_string();
            config.whisper.data_dir = dir.path().join("wsp").display().to_string();
            config.whisper.compressed = true;
            config.whisper.out_of_order = true;
            config.whisper.out_of_order_compact_rate = 1;
            config.whisper.out_of_order_compact_threshold = threshold;
            config.carbonserver.enabled = false;
            config.prometheus.enabled = false;
            let app = App::new(config).unwrap();
            let mut collector = Collector::default();
            let at = now();
            let sidecar = whisper_rs::out_of_order_sidecar_path(app.path("ooo.test").unwrap());
            let write = |timestamp: i64, value: f64| {
                app.ingest("ooo.test".into(), Point { timestamp, value })
                    .unwrap();
                app.flush_one().unwrap();
            };
            write(at - 2, 2.0);
            write(at - 3, 1.0);
            assert_eq!(sidecar.exists(), !compacted, "threshold {threshold}");
            let after = snapshot(&mut collector, &app);
            assert_eq!(after["persister.oooDiverted"], 1.0);
            assert_eq!(after["persister.oooCompactions"], f64::from(compacted));
            assert_eq!(after["persister.oooCompactErrors"], 0.0);
            if compacted {
                let merged = app.fetch("ooo.test", at - 4, at - 2, at).unwrap().unwrap();
                assert_eq!(merged.values, vec![Some(1.0), Some(2.0)]);
                // The one merge per second is spent; the next sidecar waits for a later flush.
                write(at - 4, 0.5);
                assert!(sidecar.exists());
                let budget = snapshot(&mut collector, &app);
                assert_eq!(budget["persister.oooCompactions"], 0.0);
            }
        }
    }

    #[test]
    fn ooo_metrics_report_real_writes_compactions_errors_and_interval_deltas() {
        for enabled in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let schemas = dir.path().join("schemas");
            std::fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:600\n").unwrap();
            let mut config = Config::default();
            config.whisper.schemas_file = schemas.display().to_string();
            config.whisper.data_dir = dir.path().join("wsp").display().to_string();
            config.whisper.compressed = true;
            config.whisper.out_of_order = enabled;
            // These counters must work without carbonserver or Prometheus enabled.
            config.carbonserver.enabled = false;
            config.prometheus.enabled = false;
            let app = App::new(config).unwrap();
            let mut collector = Collector::default();
            let names = [
                "persister.oooDiverted",
                "persister.oooCompactions",
                "persister.oooCompactErrors",
            ];
            let zero = snapshot(&mut collector, &app);
            assert_eq!(zero["persister.oooDiscardedPoints"], 0.0);
            for name in names {
                assert_eq!(zero.get(name).copied(), enabled.then_some(0.0));
            }
            let at = now();
            app.ingest(
                "ooo.test".into(),
                Point {
                    timestamp: at - 2,
                    value: 2.0,
                },
            )
            .unwrap();
            app.flush_one().unwrap();
            let sidecar = whisper_rs::out_of_order_sidecar_path(app.path("ooo.test").unwrap());
            assert!(!app.compact_one("ooo.test").unwrap());
            if enabled {
                std::fs::create_dir(&sidecar).unwrap();
            }
            app.ingest(
                "ooo.test".into(),
                Point {
                    timestamp: at - 3,
                    value: 1.0,
                },
            )
            .unwrap();
            if enabled {
                assert!(app.flush_one().is_err());
                let failed = snapshot(&mut collector, &app);
                assert_eq!(failed["persister.oooDiscardedPoints"], 1.0);
                assert_eq!(failed["persister.oooDiverted"], 0.0);
                assert_eq!(failed["persister.oooCompactErrors"], 0.0);
                std::fs::remove_dir(&sidecar).unwrap();
            }
            app.flush_one().unwrap();
            let written = snapshot(&mut collector, &app);
            assert_eq!(written["persister.oooDiscardedPoints"], 1.0);
            if enabled {
                assert_eq!(written["persister.oooDiverted"], 1.0);
                assert_eq!(written["persister.oooCompactions"], 0.0);
                let original = std::fs::read(&sidecar).unwrap();
                std::fs::write(&sidecar, b"broken").unwrap();
                assert!(app.compact_one("ooo.test").is_err());
                let failed = snapshot(&mut collector, &app);
                assert_eq!(failed["persister.oooCompactErrors"], 1.0);
                assert_eq!(failed["persister.oooCompactions"], 0.0);
                std::fs::write(&sidecar, original).unwrap();
                assert!(app.compact_one("ooo.test").unwrap());
                assert!(!app.compact_one("ooo.test").unwrap());
                let compacted = snapshot(&mut collector, &app);
                assert_eq!(compacted["persister.oooCompactions"], 1.0);
                assert_eq!(compacted["persister.oooCompactErrors"], 0.0);
                assert_eq!(compacted["persister.oooDiverted"], 0.0);
            } else {
                assert!(!sidecar.exists());
            }
            let idle = snapshot(&mut collector, &app);
            assert_eq!(idle["persister.oooDiscardedPoints"], 0.0);
            for name in names {
                assert_eq!(idle.get(name).copied(), enabled.then_some(0.0));
            }
        }
    }

    #[test]
    fn configuration_defaults_host_expansion_and_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();
        let default = Config::load(&path).unwrap();
        assert_eq!(default.common.metric_endpoint, "local");
        assert_eq!(default.common.metric_interval, Duration::from_secs(60));
        assert_eq!(
            prefix(&default.common.graph_prefix, "carbon01.example.net\n"),
            "carbon.agents.carbon01_example_net"
        );
        for endpoint in [
            "",
            "local",
            "tcp://localhost:2003",
            "udp://127.0.0.1:2003",
            "tcp://[::1]:2003",
        ] {
            assert!(Endpoint::parse(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "http://localhost:2003",
            "localhost:2003",
            "tcp://host",
            "tcp://:2003",
            "tcp://host:0",
            "tcp://host:65536",
            "tcp://user:pass@host:2003",
            "udp://host:2003/path",
            "tcp://host:2003?foo",
            "tcp://host:2003#fragment",
            "tcp://host:2003\n",
        ] {
            assert!(Endpoint::parse(endpoint).is_err(), "{endpoint}");
        }
        for text in [
            "[common]\nmetric-interval='0s'",
            "[common]\nmetric-interval='-1s'",
            "[common]\ngraph-prefix='bad prefix'",
            "[common]\ngraph-prefix='bad..prefix'",
            "[common]\ngraph-prefix=''",
            "[common]\nmetric-endpoint='udp://host:2003/path'",
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(Config::load(&path).is_err(), "{text}");
        }
        std::fs::write(&path, "[common]\nmetric-interval='1m0.5s'\nmetric-endpoint='tcp://localhost:2003'\ngraph-prefix='custom.{host}'").unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.common.metric_interval, Duration::from_millis(60_500));
    }

    #[test]
    fn gauges_and_deltas_preserve_prometheus_and_count_inflight_batches() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path(), true);
        let point = Point {
            timestamp: now() - 2,
            value: 2.0,
        };
        app.ingest("a.b".into(), point).unwrap();
        app.ingest("a.b".into(), point).unwrap();
        let batch = app.cache.take().unwrap();
        let mut collector = Collector::default();
        let values = snapshot(&mut collector, &app);
        assert_eq!(values["cache.size"], 0.0);
        assert_eq!(values["cache.metrics"], 0.0);
        assert_eq!(values["cache.notConfirmed"], 1.0); // batches, not points
        app.cache.retry(batch.id);
        let values = snapshot(&mut collector, &app);
        assert_eq!(values["cache.size"], 2.0);
        assert_eq!(values["cache.metrics"], 1.0);
        assert_eq!(values["cache.notConfirmed"], 0.0);
        app.flush_one().unwrap();
        app.fetch("a.b", now() - 10, now(), now()).unwrap();
        let server = app
            .prometheus
            .as_ref()
            .unwrap()
            .carbonserver
            .as_ref()
            .unwrap();
        server
            .requests
            .with_label_values(&["200", "/render"])
            .inc_by(3);
        app.graphite.tcp.received.fetch_add(2, Relaxed);
        app.graphite.udp.received.fetch_add(4, Relaxed);
        let values = snapshot(&mut collector, &app);
        assert_eq!(values["persister.created"], 1.0);
        assert_eq!(values["persister.updateOperations"], 1.0);
        assert_eq!(values["persister.committedPoints"], 2.0);
        assert_eq!(values["persister.pointsPerUpdate"], 2.0);
        assert_eq!(values["carbonserver.disk_requests"], 1.0);
        assert_eq!(values["carbonserver.metrics_known"], 1.0);
        assert_eq!(values["carbonserver.render_requests"], 3.0);
        assert_eq!(values["carbonserver.request_codes.render.2xx"], 3.0);
        assert_eq!(values["tcp.metricsReceived"], 2.0);
        assert_eq!(values["udp.metricsReceived"], 4.0);
        let values = snapshot(&mut collector, &app);
        for name in [
            "persister.created",
            "persister.pointsPerUpdate",
            "tcp.metricsReceived",
            "udp.metricsReceived",
            "carbonserver.disk_requests",
            "carbonserver.render_requests",
        ] {
            assert_eq!(values[name], 0.0, "{name}");
        }
        assert_eq!(server.disk_requests.get(), 1);
        assert_eq!(
            server.requests.with_label_values(&["200", "/render"]).get(),
            3
        );
    }

    #[tokio::test]
    async fn periodic_local_delivery_without_prometheus_and_clean_stop() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path(), false);
        assert!(app.prometheus.is_none());
        let (stop_tx, stop) = watch::channel(false);
        let task = tokio::spawn(run(app.clone(), stop));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(app.cache.is_empty()); // no immediate first tick
        tokio::time::timeout(Duration::from_secs(3), async {
            while app.cache.get("test.agent.cache.maxSize").len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        stop_tx.send_replace(true);
        task.await.unwrap().unwrap();
        let received = app.received.load(Relaxed);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(app.received.load(Relaxed), received);
        while app.flush_one().unwrap() {}
        assert!(app.path("test.agent.cache.maxSize").unwrap().exists());
    }

    #[tokio::test]
    async fn receiver_and_http_counters_work_without_prometheus() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path(), false);
        let (stop_tx, stop) = watch::channel(false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let tcp = tokio::spawn(crate::receiver::tcp(
            listener,
            app.clone(),
            app.config.tcp.clone(),
            stop.clone(),
        ));
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(format!("a.b 1 {}\nbad\n", now() - 2).as_bytes())
            .await
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let udp = tokio::spawn(crate::receiver::udp(
            socket,
            app.clone(),
            app.config.udp.clone(),
            stop,
        ));
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender
            .send_to(format!("bad\na.c 1 {}", now() - 2).as_bytes(), address)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while app.received.load(Relaxed) < 2 || app.invalid.load(Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = crate::http::router(app.clone());
        let http = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut connection = TcpStream::connect(address).await.unwrap();
        connection.write_all(format!("GET /render/?target=a.b&from={}&until={} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", now() - 10, now()).as_bytes()).await.unwrap();
        let mut response = String::new();
        connection.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let mut collector = Collector::default();
        let values = snapshot(&mut collector, &app);
        for name in [
            "tcp.metricsReceived",
            "udp.metricsReceived",
            "tcp.errors",
            "udp.errors",
            "tcp.active",
            "carbonserver.render_requests",
            "carbonserver.metrics_returned",
        ] {
            assert_eq!(values[name], 1.0, "{name}");
        }
        assert_eq!(values["carbonserver.inflight_requests_count"], 0.0);
        assert_eq!(values["carbonserver.request_codes.combined.2xx"], 1.0);
        stop_tx.send_replace(true);
        tcp.await.unwrap().unwrap();
        udp.await.unwrap().unwrap();
        http.abort();
        assert_eq!(snapshot(&mut collector, &app)["tcp.active"], 0.0);
    }

    #[tokio::test]
    async fn tcp_udp_plaintext_delivery_retry_and_cancellation() {
        let line = format!("test.agent.cache.size 42 {}\n", now());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener); // Force the first connection attempt to fail.
        let (tx, rx) = mpsc::channel(2);
        tx.try_send(line.clone()).unwrap();
        drop(tx);
        let (stop_tx, stop) = watch::channel(false);
        let task = tokio::spawn(send_loop(Endpoint::Tcp(address.to_string()), rx, stop));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let listener = TcpListener::bind(address).await.unwrap();
        let wire = tokio::time::timeout(Duration::from_secs(4), async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut wire = String::new();
            socket.read_to_string(&mut wire).await.unwrap();
            wire
        })
        .await
        .unwrap();
        assert_eq!(wire, line);
        task.await.unwrap();
        drop(listener);

        let (tx, rx) = mpsc::channel(2);
        tx.try_send(line.clone()).unwrap();
        drop(tx);
        let task = tokio::spawn(send_loop(
            Endpoint::Tcp(address.to_string()),
            rx,
            stop_tx.subscribe(),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send_replace(true);
        tokio::time::timeout(Duration::from_millis(250), task)
            .await
            .unwrap()
            .unwrap();

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = mpsc::channel(128);
        for _ in 0..100 {
            tx.try_send(line.clone()).unwrap();
        }
        drop(tx);
        let (_stop_tx, stop) = watch::channel(false);
        let task = tokio::spawn(send_loop(
            Endpoint::Udp(socket.local_addr().unwrap().to_string()),
            rx,
            stop,
        ));
        let mut count = 0;
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut bytes = [0; 4096];
            while count < 100 {
                let n = socket.recv(&mut bytes).await.unwrap();
                assert!(n <= 1000);
                let packet = std::str::from_utf8(&bytes[..n]).unwrap();
                assert!(packet.ends_with('\n'));
                for sample in packet.split_inclusive('\n') {
                    assert_eq!(sample, line);
                    count += 1;
                }
            }
        })
        .await
        .unwrap();
        task.await.unwrap();
    }
}
