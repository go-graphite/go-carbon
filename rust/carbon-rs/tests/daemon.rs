use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use carbon_rs::app::write_dump;
use whisper_rs::Point;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn graphite_common_settings_drive_local_tcp_and_udp_delivery_without_prometheus() {
    for transport in ["local", "tcp", "udp"] {
        let dir = tempfile::tempdir().unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        tcp.set_nonblocking(true).unwrap();
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        udp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let endpoint = match transport {
            "tcp" => format!("tcp://{}", tcp.local_addr().unwrap()),
            "udp" => format!("udp://{}", udp.local_addr().unwrap()),
            _ => "local".into(),
        };
        let schemas = dir.path().join("schemas");
        fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:600\n").unwrap();
        let config = dir.path().join("config");
        fs::write(
            &config,
            format!(
                r#"
[common]
graph-prefix = "self.agent"
metric-endpoint = "{endpoint}"
metric-interval = "0.1s"
[whisper]
data-dir = "{}/wsp"
schemas-file = "{}"
[dump]
path = "{}/dump"
[tcp]
enabled = true
listen = "127.0.0.1:0"
[[logging]]
file = "none"
"#,
                dir.path().display(),
                schemas.display(),
                dir.path().display()
            ),
        )
        .unwrap();
        let mut child = ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_carbon-rs"))
                .arg("--config")
                .arg(&config)
                .spawn()
                .unwrap(),
        );
        let metric_path = dir.path().join("wsp/self/agent/cache/maxSize.wsp");
        let mut wire = String::new();
        match transport {
            "local" => wait_for(&metric_path),
            "tcp" => {
                let until = Instant::now() + Duration::from_secs(5);
                let mut socket = loop {
                    match tcp.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < until =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        Err(error) => panic!("self-metrics not delivered: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                socket.read_to_string(&mut wire).unwrap();
            }
            _ => {
                let mut packet = [0; 4096];
                let n = udp.recv(&mut packet).unwrap();
                assert!(n <= 1000);
                wire = String::from_utf8(packet[..n].to_vec()).unwrap();
            }
        }
        signal(&child, "-TERM");
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                Instant::now() < until,
                "Graphite reporter prevented shutdown"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        if transport == "local" {
            let at = carbon_rs::app::now();
            let mut whisper =
                whisper_rs::Whisper::open(metric_path, whisper_rs::Options::default()).unwrap();
            let series = whisper.fetch(at - 10, at, at).unwrap().unwrap();
            assert!(series.values.contains(&Some(1_000_000.0)));
        } else {
            assert!(wire.contains("self.agent.cache.maxSize 1000000 "), "{wire}");
            assert!(wire.ends_with('\n'));
            for line in wire.lines() {
                let (name, point) = carbon_rs::plaintext::parse_line(line.as_bytes()).unwrap();
                assert!(name.starts_with("self.agent."));
                assert!(point.timestamp > 0);
            }
            assert_eq!(fs::read_dir(dir.path().join("wsp")).unwrap().count(), 0);
        }
    }
}

#[test]
fn diagnostics_start_without_carbonserver_and_honor_enable_flags() {
    for (pprof_enabled, prometheus_enabled) in [(false, true), (true, false), (true, true)] {
        if pprof_enabled && !carbon_rs::profiling::SUPPORTED {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let schemas = dir.path().join("schemas");
        fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:60\n").unwrap();
        let config = dir.path().join("config.toml");
        fs::write(
            &config,
            format!(
                r#"
[whisper]
data-dir = "{}/wsp"
schemas-file = "{}"
[dump]
path = "{}/dump"
[pprof]
enabled = {pprof_enabled}
listen = "127.0.0.1:{port}"
[prometheus]
enabled = {prometheus_enabled}
endpoint = "/custom"
[prometheus.labels]
region = "ams"
"#,
                dir.path().display(),
                schemas.display(),
                dir.path().display()
            ),
        )
        .unwrap();
        let bin = env!("CARGO_BIN_EXE_carbon-rs");
        let mut child = ChildGuard(
            Command::new(bin)
                .arg("--config")
                .arg(&config)
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let until = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < until => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => panic!("Prometheus listener did not start: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .write_all(b"GET /custom HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        if prometheus_enabled {
            assert!(response.starts_with("HTTP/1.1 200"));
            assert!(response.contains("out_of_order_write_lag_exp_count{region=\"ams\"} 0"));
        } else {
            assert!(response.starts_with("HTTP/1.1 404"));
        }
        assert!(!response.contains("metrics_received_tcp_total"));
        assert!(!response.contains("http_requests_total"));
        let expected = if pprof_enabled { "200" } else { "404" };
        assert!(http_get(port, "/debug/pprof/").starts_with(&format!("HTTP/1.1 {expected}")));
        let expected = if pprof_enabled { "400" } else { "404" };
        for seconds in ["0", "301", "-1", "0.5", "bad", "18446744073709551616"] {
            assert!(
                http_get(port, &format!("/debug/pprof/profile?seconds={seconds}"))
                    .starts_with(&format!("HTTP/1.1 {expected}"))
            );
        }
        assert!(http_get(port, "/debug/pprof/heap").starts_with("HTTP/1.1 404"));
        if pprof_enabled && prometheus_enabled {
            let recording =
                std::thread::spawn(move || http_get_bytes(port, "/debug/pprof/profile?seconds=1"));
            assert!(http_get(port, "/custom").starts_with("HTTP/1.1 200"));
            let response = recording.join().unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200"));
            let body = &response[response.windows(4).position(|s| s == b"\r\n\r\n").unwrap() + 4..];
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(body)
                .read_to_end(&mut decoded)
                .unwrap();
            assert!(!decoded.is_empty());
        }
        assert!(
            Command::new("kill")
                .arg("-TERM")
                .arg(child.0.id().to_string())
                .status()
                .unwrap()
                .success()
        );
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                Instant::now() < until,
                "diagnostics daemon did not stop after SIGTERM"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn http_get(port: u16, path: &str) -> String {
    String::from_utf8(http_get_bytes(port, path)).unwrap()
}

fn http_get_bytes(port: u16, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    response
}

fn wait_for(path: &Path) {
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {}", path.display());
}

fn wait_log(path: &Path, logger: &str, message: &str) -> serde_json::Value {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let text = fs::read_to_string(path).unwrap_or_default();
        if let Some(row) = text
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|row| row["logger"] == logger && row["message"] == message)
        {
            return row;
        }
        assert!(
            Instant::now() < until,
            "missing {logger}: {message} in {text}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn signal(child: &ChildGuard, name: &str) {
    assert!(
        Command::new("kill")
            .arg(name)
            .arg(child.0.id().to_string())
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn go_style_listeners_bind_all_ipv4_interfaces() {
    let dir = tempfile::tempdir().unwrap();
    let schemas = dir.path().join("schemas");
    fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:60\n").unwrap();
    let log = dir.path().join("carbon.log");
    let config = dir.path().join("config");
    fs::write(
        &config,
        format!(
            r#"
[whisper]
data-dir = "{}/wsp"
schemas-file = "{}"
[dump]
path = "{}/dump"
[tcp]
enabled = true
listen = ":0"
[udp]
enabled = true
listen = ":0"
[carbonserver]
enabled = true
listen = ":0"
[prometheus]
enabled = true
[pprof]
listen = ":0"
[[logging]]
file = "{}"
encoding = "json"
"#,
            dir.path().display(),
            schemas.display(),
            dir.path().display(),
            log.display()
        ),
    )
    .unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_carbon-rs"))
            .arg("--config")
            .arg(&config)
            .spawn()
            .unwrap(),
    );
    wait_log(&log, "main", "started");
    for (logger, message) in [
        ("tcp", "listening"),
        ("udp", "listening"),
        ("carbonserver", "starting carbonserver"),
        ("pprof", "diagnostics listening"),
    ] {
        let row = wait_log(&log, logger, message);
        let address: std::net::SocketAddr = row["address"].as_str().unwrap().parse().unwrap();
        assert_eq!(address.ip(), std::net::Ipv4Addr::UNSPECIFIED, "{logger}");
        assert_ne!(address.port(), 0, "{logger}");
        if logger == "pprof" {
            assert!(http_get(address.port(), "/metrics").starts_with("HTTP/1.1 200"));
        }
    }
    signal(&child, "-TERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline, "daemon failed to stop");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn structured_logging_covers_receivers_access_reload_rotation_and_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let schemas = dir.path().join("schemas");
    let schema = "[all]\npattern = .*\nretentions = 1:60\n";
    fs::write(&schemas, schema).unwrap();
    let log = dir.path().join("logs/carbon.log");
    let config = dir.path().join("config");
    fs::write(
        &config,
        format!(
            r#"
[whisper]
data-dir = "{}/wsp"
schemas-file = "{}"
[tcp]
enabled = true
listen = "127.0.0.1:0"
[udp]
enabled = true
listen = "127.0.0.1:0"
[carbonserver]
enabled = true
listen = "127.0.0.1:0"
[dump]
path = "{}/dump"
[[logging]]
file = "{}"
encoding = "json"
[[logging]]
logger = "tcp"
file = "{}"
encoding = "json"
level = "debug"
[[logging]]
logger = "udp"
file = "{}"
encoding = "json"
level = "debug"
[[logging]]
logger = "access"
file = "{}"
encoding = "json"
encoding-duration = "nanos"
"#,
            dir.path().display(),
            schemas.display(),
            dir.path().display(),
            log.display(),
            log.display(),
            log.display(),
            log.display()
        ),
    )
    .unwrap();
    let bin = env!("CARGO_BIN_EXE_carbon-rs");
    let checked = Command::new(bin)
        .args(["--config", config.to_str().unwrap(), "--check-config"])
        .output()
        .unwrap();
    assert!(checked.status.success(), "{:?}", checked);
    assert!(!log.parent().unwrap().exists());
    let mut child = ChildGuard(
        Command::new(bin)
            .arg("--config")
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_log(&log, "main", "started");
    let tcp = wait_log(&log, "tcp", "listening");
    let udp = wait_log(&log, "udp", "listening");
    let http = wait_log(&log, "carbonserver", "starting carbonserver");
    let mut stream = TcpStream::connect(tcp["address"].as_str().unwrap()).unwrap();
    stream
        .write_all(format!("invalid\nlive.metric 1 {}\n", carbon_rs::app::now()).as_bytes())
        .unwrap();
    drop(stream);
    let udp_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    udp_socket
        .send_to(b"invalid\n", udp["address"].as_str().unwrap())
        .unwrap();
    assert_eq!(wait_log(&log, "tcp", "parse failed")["level"], "DEBUG");
    assert_eq!(wait_log(&log, "udp", "parse failed")["level"], "DEBUG");
    wait_log(&log, "whisper:new", "new whisper file");
    let http: std::net::SocketAddr = http["address"].as_str().unwrap().parse().unwrap();
    assert!(http_get(http.port(), "/metrics/find/?query=live.*").starts_with("HTTP/1.1 200"));
    let access = wait_log(&log, "access", "request served");
    assert_eq!(access["handler"], "find");
    assert_eq!(access["http_code"], 200);
    assert_eq!(access["method"], "GET");
    assert!(access["peer"].as_str().unwrap().starts_with("127.0.0.1:"));
    assert!(access["runtime_seconds"].is_u64());
    assert!(http_get(http.port(), "/info/?target=missing").starts_with("HTTP/1.1 404"));
    assert_eq!(wait_log(&log, "access", "request failed")["http_code"], 404);

    fs::write(&schemas, "broken").unwrap();
    signal(&child, "-HUP");
    assert_eq!(
        wait_log(&log, "main", "config reload failed")["level"],
        "ERROR"
    );
    fs::write(&schemas, schema).unwrap();
    signal(&child, "-HUP");
    wait_log(&log, "main", "config successfully reloaded");

    let rotated = dir.path().join("carbon.log.1");
    fs::rename(&log, &rotated).unwrap();
    let previous = fs::read(&rotated).unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    signal(&child, "-HUP");
    wait_log(&log, "main", "config successfully reloaded");
    assert_eq!(fs::read(&rotated).unwrap(), previous);
    signal(&child, "-TERM");
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < until, "daemon failed to stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    wait_log(&log, "main", "stopped");
    for path in [&log, &rotated] {
        for line in fs::read_to_string(path).unwrap().lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(row["timestamp"].is_string());
            assert!(row.get("go_level").is_none());
        }
    }
}

#[test]
fn logging_routes_fatal_startup_errors_and_check_config_never_creates_logs() {
    let dir = tempfile::tempdir().unwrap();
    let schemas = dir.path().join("schemas");
    fs::write(&schemas, "[all]\npattern = .*\nretentions = 1:60\n").unwrap();
    let config = dir.path().join("config");
    let bin = env!("CARGO_BIN_EXE_carbon-rs");
    for (destination, encoding, level) in [
        ("stdout", "json", "info"),
        ("stderr", "mixed", "info"),
        ("none", "json", "info"),
        ("stdout", "json", "fatal"),
    ] {
        fs::write(
            &config,
            format!(
                r#"
[whisper]
data-dir = "{}/wsp"
schemas-file = "{}"
[dump]
path = "{}/dump"
[[logging]]
file = "{destination}"
encoding = "{encoding}"
level = "{level}"
"#,
                dir.path().display(),
                schemas.display(),
                dir.path().display()
            ),
        )
        .unwrap();
        // No listener enabled: a startup failure after logging initialization.
        let output = Command::new(bin)
            .arg("--config")
            .arg(&config)
            .output()
            .unwrap();
        assert!(!output.status.success());
        match destination {
            "stdout" => {
                assert!(output.stderr.is_empty(), "{:?}", output.stderr);
                let stdout = String::from_utf8(output.stdout).unwrap();
                let rows: Vec<serde_json::Value> = stdout
                    .lines()
                    .map(|s| serde_json::from_str(s).unwrap())
                    .collect();
                if level == "fatal" {
                    assert_eq!(rows.len(), 1);
                }
                let row = rows.last().unwrap();
                assert_eq!(row["level"], "FATAL");
                assert_eq!(row["message"], "daemon failed");
                assert!(row["error"].as_str().unwrap().contains("no receivers"));
            }
            "stderr" => {
                assert!(output.stdout.is_empty());
                assert!(
                    String::from_utf8(output.stderr)
                        .unwrap()
                        .contains(" FATAL [main] daemon failed ")
                );
            }
            "none" => {
                assert!(output.stdout.is_empty());
                assert!(output.stderr.is_empty());
            }
            _ => unreachable!(),
        }
    }
    let log = dir.path().join("must-not-exist");
    fs::write(
        &config,
        format!(
            "[[logging]]\nfile='{}'\nencoding='invalid'\n",
            log.display()
        ),
    )
    .unwrap();
    let output = Command::new(bin)
        .arg("--config")
        .arg(&config)
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("logging.encoding")
    );
    assert!(!log.exists());
}

#[test]
fn daemon_restores_dump_ingests_tcp_and_shuts_down() {
    let dir = tempfile::tempdir().unwrap();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let data = dir.path().join("wsp");
    let dump = dir.path().join("dump");
    fs::create_dir_all(&dump).unwrap();
    let schemas = dir.path().join("schemas.conf");
    fs::write(&schemas, "[default]\npattern = .*\nretentions = 1:60\n").unwrap();
    let config = dir.path().join("carbon.toml");
    fs::write(&config, format!("[whisper]\ndata-dir = \"{}\"\nschemas-file = \"{}\"\nworkers = 1\n\n[tcp]\nenabled = true\nlisten = \"127.0.0.1:{port}\"\n\n[udp]\nenabled = false\n\n[carbonserver]\nenabled = false\n\n[dump]\nenabled = true\npath = \"{}\"\n", data.display(), schemas.display(), dump.display())).unwrap();
    let timestamp = carbon_rs::app::now() - 1;
    let pending = dump.join("cache.bin");
    let mut file = fs::File::create(&pending).unwrap();
    write_dump(
        &mut file,
        "restored.metric",
        &[Point {
            timestamp,
            value: 1.0,
        }],
    )
    .unwrap();
    drop(file);
    let original = fs::read(&pending).unwrap();
    let input = dump.join("input.1.2");
    let line = format!("restored.input 3 {timestamp}\n");
    let previous = dump.join("cache.old.bin.restored");
    fs::write(&previous, b"previous backup").unwrap();
    let bin = env!("CARGO_BIN_EXE_carbon-rs");
    assert!(
        Command::new(bin)
            .arg("--config")
            .arg(&config)
            .arg("--check-config")
            .status()
            .unwrap()
            .success()
    );
    // Both a truncated input and a failed write must keep every pending dump intact.
    for truncated in [true, false] {
        let contents = if truncated {
            line.trim_end()
        } else {
            fs::create_dir_all(&data).unwrap();
            fs::write(data.join("restored"), b"blocks metric creation").unwrap();
            &line
        };
        fs::write(&input, contents).unwrap();
        let output = Command::new(bin)
            .arg("--config")
            .arg(&config)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(fs::read(&pending).unwrap(), original);
        assert_eq!(fs::read_to_string(&input).unwrap(), contents);
        assert_eq!(fs::read_dir(&dump).unwrap().count(), 3);
    }
    fs::remove_file(data.join("restored")).unwrap();
    let mut child = ChildGuard(
        Command::new(bin)
            .arg("--config")
            .arg(&config)
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream.write_all(b"live.metric 2 101\n").unwrap();
                break;
            }
            Err(_) if Instant::now() < until => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("TCP listener did not start: {e}"),
        }
    }
    // The listener starts only after successful persistence and dump cleanup.
    assert_eq!(
        fs::read_dir(&dump)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>(),
        vec![previous.clone()]
    );
    assert_eq!(fs::read(previous).unwrap(), b"previous backup");
    for (metric, value) in [("metric", 1.0), ("input", 3.0)] {
        let mut whisper = whisper_rs::Whisper::open(
            data.join(format!("restored/{metric}.wsp")),
            whisper_rs::Options::default(),
        )
        .unwrap();
        let series = whisper
            .fetch(timestamp - 1, timestamp + 1, timestamp + 1)
            .unwrap()
            .unwrap();
        assert!(series.values.contains(&Some(value)));
    }
    wait_for(&data.join("live/metric.wsp"));
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()
            .unwrap()
            .success()
    );
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("daemon did not stop after SIGTERM");
}
