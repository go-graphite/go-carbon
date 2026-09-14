use carbon_rs::{app::App, config::Config, receiver};
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinSet;

fn main() -> ExitCode {
    match start() {
        Ok(code) => code,
        Err(error) => {
            // Configuration/output initialization errors happen before tracing is available.
            let _ = std::io::Write::write_fmt(
                &mut io::stderr().lock(),
                format_args!("carbon-rs: {error}\n"),
            );
            ExitCode::FAILURE
        }
    }
}

fn start() -> io::Result<ExitCode> {
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
                return Ok(ExitCode::SUCCESS);
            }
            "-h" | "--help" => {
                println!(
                    "carbon-rs --config FILE [--check-config]\nTCP/UDP plaintext and HTTP carbonserver; noop cache scheduling."
                );
                return Ok(ExitCode::SUCCESS);
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
        return Ok(ExitCode::SUCCESS);
    }
    let _logging = carbon_rs::logging::init(&config.logging)?;
    if config.common.log_level.is_some() || config.common.logfile.is_some() {
        tracing::warn!(target: "main", "common.log-level and common.logfile are deprecated; use [[logging]]");
    }
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.common.max_cpu.max(1))
        .enable_all()
        .build()
        .and_then(|runtime| runtime.block_on(run(config)));
    match result {
        Ok(()) => {
            tracing::info!(target: "main", "stopped");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            tracing::error!(target: "main", go_level = "FATAL", error = %error, "daemon failed");
            Ok(ExitCode::FAILURE)
        }
    }
}

async fn run(config: Config) -> io::Result<()> {
    let app = App::new(config)?;
    // A corrupt cache is only an acceleration failure; scanning remains authoritative.
    if !app.config.carbonserver.file_list_cache.is_empty()
        && let Err(error) = carbon_rs::file_list::read(&app.config.carbonserver.file_list_cache)
            .and_then(|(_, entries)| app.load_file_list(entries))
    {
        tracing::warn!(target: "carbonserver", error = %error, "file-list cache ignored");
    }
    // FLC v1 has no size metadata and any version can be stale. Reconcile disk
    // before quota-constrained admission; the loaded cache only shortens index readiness.
    app.scan()?;
    save_file_list(&app)?;
    if app.config.dump.enabled {
        let restored = app.restore()?;
        let restored_files = restored.len();
        while !app.cache.is_empty() {
            if !app.flush_one()? {
                return Err(io::Error::other("recovery cache has no writable batch"));
            }
        }
        for path in restored {
            std::fs::remove_file(path)?;
        }
        if restored_files > 0 {
            tracing::info!(target: "restore", files = restored_files, "restored points persisted");
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
    let diagnostics = if app.config.prometheus.enabled || app.config.pprof.enabled {
        // Prometheus and pprof share this listener, so name the key on failure.
        let listen = &app.config.pprof.listen;
        Some(
            TcpListener::bind(listen)
                .await
                .map_err(|e| io::Error::new(e.kind(), format!("pprof.listen {listen}: {e}")))?,
        )
    } else {
        None
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut listeners = JoinSet::new();
    if let Some(listener) = tcp {
        tracing::info!(target: "tcp", address = %listener.local_addr()?, "listening");
        listeners.spawn(receiver::tcp(
            listener,
            app.clone(),
            app.config.tcp.clone(),
            stop_rx.clone(),
        ));
    }
    if let Some(socket) = udp {
        tracing::info!(target: "udp", address = %socket.local_addr()?, "listening");
        listeners.spawn(receiver::udp(
            socket,
            app.clone(),
            app.config.udp.clone(),
            stop_rx.clone(),
        ));
    }
    if let Some(listener) = http {
        tracing::info!(target: "carbonserver", address = %listener.local_addr()?, "starting carbonserver");
        let router = carbon_rs::http::router(app.clone());
        let mut stopped = stop_rx.clone();
        listeners.spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = stopped.changed().await;
            })
            .await
        });
    }
    if let Some(listener) = diagnostics {
        tracing::info!(target: "pprof", address = %listener.local_addr()?, "diagnostics listening");
        let mut router = carbon_rs::metrics::router(app.clone());
        if app.config.pprof.enabled {
            router = router.merge(carbon_rs::profiling::router(stop_rx.clone()));
        }
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
            "no receivers, carbonserver, prometheus or pprof enabled",
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
                        tracing::error!(target: "carbonserver", error = %error, "file-list cache write failed");
                    }
                }
                result => {
                    tracing::error!(target: "carbonserver", error = ?result, "filesystem scan failed")
                }
            }
        }
    });
    let collector_app = app.clone();
    let collector_stop = stop_rx.clone();
    let collector = tokio::spawn(async move {
        if let Err(error) = carbon_rs::graphite::run(collector_app, collector_stop).await {
            tracing::error!(target: "stat", error = %error, "self-metrics collector stopped");
        }
    });
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut dump_stop =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())?;
    let mut reload = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    tracing::info!(target: "main", version = env!("CARGO_PKG_VERSION"), "started");
    let dump_immediately = loop {
        let stop = tokio::select! {
            _ = terminate.recv() => { tracing::info!(target: "main", signal = "SIGTERM", "stopping"); false },
            _ = interrupt.recv() => { tracing::info!(target: "main", signal = "SIGINT", "stopping"); false },
            _ = dump_stop.recv() => {
                if app.config.dump.enabled {
                    tracing::info!(target: "main", signal = "SIGUSR2", "dump and stop"); true
                } else { tracing::warn!(target: "main", "SIGUSR2 ignored: dump is disabled"); continue }
            },
            _ = reload.recv() => {
                tracing::info!(target: "main", "HUP received. Reload config");
                match app.reload_rules() {
                    Ok(()) => tracing::info!(target: "main", "config successfully reloaded"),
                    Err(error) => tracing::error!(target: "main", error = %error, "config reload failed"),
                }
                continue
            },
            failure = listeners.join_next() => { tracing::error!(target: "main", error = ?failure, "listener terminated"); false },
            failure = workers.join_next() => { tracing::error!(target: "main", error = ?failure, "persister terminated"); false },
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
        tracing::warn!(target: "main", "listener shutdown timed out");
        listeners.abort_all();
    }
    // One budget for background teardown plus drain; failures and hangs must not skip the dump.
    let deadline = Instant::now() + timeout;
    let background = async {
        while let Some(result) = workers.join_next().await {
            if let Err(error) = result {
                tracing::error!(target: "persister", error = %error, "Whisper worker failed");
            }
        }
        for (name, task) in [("scanner", scanner), ("collector", collector)] {
            if let Err(error) = task.await {
                tracing::error!(target: "main", task = name, error = %error, "background task failed");
            }
        }
    };
    if tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), background)
        .await
        .is_err()
    {
        tracing::warn!(target: "main", "background shutdown timed out");
        workers.abort_all();
    }
    if !dump_immediately {
        while !app.cache.is_empty() && Instant::now() < deadline {
            let writer = app.clone();
            let flushed = tokio::task::spawn_blocking(move || writer.flush_one())
                .await
                .map_err(io::Error::other)
                .and_then(|result| result);
            match flushed {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => {
                    tracing::error!(target: "main", error = %e, "shutdown write failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    if !app.cache.is_empty() {
        // Accepted data must not disappear because shutdown hit a disk error or its deadline.
        let path = app.dump()?;
        tracing::warn!(target: "dump", filename = %path.display(), "saved unpersisted points; enable dump restoration before restart");
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
                // flush_one logs storage errors with the metric before requeueing the batch.
                if let Err(error) = result {
                    tracing::error!(target: "persister", error = %error, "Whisper worker failed");
                }
                tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        }
    }
}
