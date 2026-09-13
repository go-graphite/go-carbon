use std::fs;
use std::io::Write;
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
    let pending = dump.join("cache.bin");
    let mut file = fs::File::create(&pending).unwrap();
    write_dump(
        &mut file,
        "restored.metric",
        &[Point {
            timestamp: 100,
            value: 1.0,
        }],
    )
    .unwrap();
    drop(file);
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
    let mut child = ChildGuard(
        Command::new(bin)
            .arg("--config")
            .arg(&config)
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for(&pending.with_extension("bin.restored"));
    wait_for(&data.join("restored/metric.wsp"));
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
