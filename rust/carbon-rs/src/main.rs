use carbon_rs::{app::App, config::Config, receiver};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinSet;

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config = PathBuf::from("/etc/go-carbon/go-carbon.conf");
    let mut check = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-config" | "--config" => {
                config = args
                    .next()
                    .ok_or_else(|| carbon_rs::app::invalid("missing config path"))?
                    .into()
            }
            "--check-config" => check = true,
            "-version" | "--version" => {
                println!("carbon-rs {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "-h" | "--help" => {
                println!(
                    "carbon-rs --config FILE [--check-config]\nTCP/UDP plaintext and HTTP carbonserver; noop cache scheduling."
                );
                return Ok(());
            }
            _ => return Err(carbon_rs::app::invalid(format!("unknown argument {arg}"))),
        }
    }
    let config = Config::load(config).map_err(carbon_rs::app::invalid)?;
    if check {
        carbon_rs::config::Rules::load(
            &config.whisper.schemas_file,
            &config.whisper.aggregation_file,
            &config.whisper,
        )
        .map_err(carbon_rs::app::invalid)?;
        if !config.whisper.quotas_file.is_empty() {
            carbon_rs::quotas::Engine::load(
                &config.whisper.quotas_file,
                config.carbonserver.quota_usage_report_frequency,
            )
            .map_err(carbon_rs::app::invalid)?;
        }
        println!("configuration valid");
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.common.max_cpu.max(1))
        .enable_all()
        .build()?
        .block_on(run(config))
}

async fn run(config: Config) -> io::Result<()> {
    let app = App::new(config)?;
    // A corrupt cache is only an acceleration failure; scanning remains authoritative.
    if !app.config.carbonserver.file_list_cache.is_empty()
        && let Err(error) = carbon_rs::file_list::read(&app.config.carbonserver.file_list_cache)
            .and_then(|(_, entries)| app.load_file_list(entries))
    {
        eprintln!("file-list cache ignored: {error}");
    }
    // FLC v1 has no size metadata and any version can be stale. Reconcile disk
    // before quota-constrained admission; the loaded cache only shortens index readiness.
    app.scan()?;
    save_file_list(&app)?;
    if app.config.dump.enabled {
        let restored = app.restore()?;
        while !app.cache.is_empty() {
            if !app.flush_one()? {
                return Err(io::Error::other("recovery cache has no writable batch"));
            }
        }
        for path in restored {
            let mut name = path.as_os_str().to_owned();
            name.push(".restored");
            let restored = PathBuf::from(name);
            if restored.exists() {
                return Err(carbon_rs::app::invalid(format!(
                    "refusing to overwrite restored dump {}",
                    restored.display()
                )));
            }
            std::fs::rename(path, restored)?;
        }
        app.scan()?;
        save_file_list(&app)?;
    } else if has_pending_dumps(&app.config.dump.path)? {
        return Err(carbon_rs::app::invalid(
            "recovery dumps exist but dump.enabled is false",
        ));
    }
    // Bind everything before admitting points: a later bind failure must not
    // discard metrics already accepted by another receiver during startup.
    let tcp = if app.config.tcp.enabled {
        Some(TcpListener::bind(&app.config.tcp.listen).await?)
    } else {
        None
    };
    let udp = if app.config.udp.enabled {
        Some(UdpSocket::bind(&app.config.udp.listen).await?)
    } else {
        None
    };
    let http = if app.config.carbonserver.enabled {
        Some(TcpListener::bind(&app.config.carbonserver.listen).await?)
    } else {
        None
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut listeners = JoinSet::new();
    if let Some(listener) = tcp {
        eprintln!("TCP listening on {}", listener.local_addr()?);
        listeners.spawn(receiver::tcp(
            listener,
            app.clone(),
            app.config.tcp.clone(),
            stop_rx.clone(),
        ));
    }
    if let Some(socket) = udp {
        eprintln!("UDP listening on {}", socket.local_addr()?);
        listeners.spawn(receiver::udp(
            socket,
            app.clone(),
            app.config.udp.clone(),
            stop_rx.clone(),
        ));
    }
    if let Some(listener) = http {
        eprintln!("HTTP listening on {}", listener.local_addr()?);
        let router = carbon_rs::http::router(app.clone());
        let mut stopped = stop_rx.clone();
        listeners.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopped.changed().await;
                })
                .await
        });
    }
    if listeners.is_empty() {
        return Err(carbon_rs::app::invalid(
            "no receivers or carbonserver enabled",
        ));
    }
    let mut workers = JoinSet::new();
    for _ in 0..app.config.whisper.workers.max(1) {
        workers.spawn(worker(app.clone(), stop_rx.clone()));
    }
    let scanner_app = app.clone();
    let mut scanner_stop = stop_rx.clone();
    let scanner = tokio::spawn(async move {
        let frequency = scanner_app.config.carbonserver.scan_frequency;
        if frequency.is_zero() {
            return;
        }
        loop {
            tokio::select! { _ = scanner_stop.changed() => break, _ = tokio::time::sleep(frequency) => {} }
            let app = scanner_app.clone();
            match tokio::task::spawn_blocking(move || app.scan()).await {
                Ok(Ok(())) => {
                    if let Err(error) = save_file_list(&scanner_app) {
                        eprintln!("file-list cache write failed: {error}")
                    }
                }
                result => eprintln!("filesystem scan failed: {result:?}"),
            }
        }
    });
    let compactor = tokio::spawn(carbon_rs::lifecycle::compact(app.clone(), stop_rx.clone()));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut dump_stop =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())?;
    let mut reload = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let dump_immediately = loop {
        let stop = tokio::select! {
            _ = terminate.recv() => false,
            _ = interrupt.recv() => false,
        _ = dump_stop.recv() => { if app.config.dump.enabled { true } else { eprintln!("SIGUSR2 ignored: dump is disabled"); continue } },
            _ = reload.recv() => { if let Err(error) = app.reload_rules() { eprintln!("configuration reload failed: {error}"); } continue },
            failure = listeners.join_next() => { eprintln!("listener terminated: {failure:?}"); false },
            failure = workers.join_next() => { eprintln!("persister terminated: {failure:?}"); false },
        };
        break stop;
    };
    stop_tx.send_replace(true);
    let timeout = app
        .config
        .tcp
        .shutdown_timeout
        .max(app.config.udp.shutdown_timeout);
    if tokio::time::timeout(timeout, async {
        while listeners.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        listeners.abort_all();
    }
    while let Some(result) = workers.join_next().await {
        result.map_err(io::Error::other)?;
    }
    scanner.await.map_err(io::Error::other)?;
    compactor.await.map_err(io::Error::other)?;
    let deadline = Instant::now() + timeout;
    if !dump_immediately {
        while !app.cache.is_empty() && Instant::now() < deadline {
            match app.flush_one() {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => {
                    eprintln!("shutdown write failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    if !app.cache.is_empty() {
        // Accepted data must not disappear because shutdown hit a disk error or its deadline.
        let path = app.dump()?;
        eprintln!(
            "saved unpersisted points to {}; enable dump restoration before restart",
            path.display()
        );
    }
    Ok(())
}

fn save_file_list(app: &App) -> io::Result<()> {
    if app.config.carbonserver.file_list_cache.is_empty() {
        return Ok(());
    }
    let version = match app.config.carbonserver.file_list_cache_version {
        1 => carbon_rs::file_list::Version::V1,
        2 => carbon_rs::file_list::Version::V2,
        _ => {
            return Err(carbon_rs::app::invalid(
                "file-list-cache-version must be 1 or 2",
            ));
        }
    };
    carbon_rs::file_list::write(
        &app.config.carbonserver.file_list_cache,
        version,
        &app.save_file_list(),
    )
}

fn has_pending_dumps(path: &str) -> io::Result<bool> {
    let dir = std::path::Path::new(path);
    if !dir.exists() {
        return Ok(false);
    }
    Ok(std::fs::read_dir(dir)?.filter_map(Result::ok).any(|entry| {
        entry.file_name().to_str().is_some_and(|name| {
            (name.starts_with("cache.") || name.starts_with("input."))
                && !name.ends_with(".tmp")
                && !name.ends_with(".restored")
        })
    }))
}

async fn worker(app: Arc<App>, mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            break;
        }
        let writer = app.clone();
        match tokio::task::spawn_blocking(move || writer.flush_one()).await {
            Ok(Ok(true)) => continue,
            Ok(Ok(false)) => {
                tokio::select! { _ = stop.changed() => break, _ = app.wake.notified() => {}, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
            }
            result => {
                eprintln!("Whisper write failed; batch requeued: {result:?}");
                tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        }
    }
}
