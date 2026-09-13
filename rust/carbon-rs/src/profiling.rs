//! On-demand CPU profiles in Go's gzip-compressed pprof protobuf format.
use std::sync::mpsc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::{Router, routing::get};
use serde::Deserialize;
use tokio::sync::{Semaphore, watch};

// SIGPROF and ITIMER_PROF are process-wide, including across router instances.
static RECORDING: Semaphore = Semaphore::const_new(1);

pub const SUPPORTED: bool = cfg!(all(target_os = "linux", target_pointer_width = "64"));

pub fn router(stop: watch::Receiver<bool>) -> Router {
    Router::new()
        .route(
            "/debug/pprof",
            get(|| async { Redirect::permanent("/debug/pprof/") }),
        )
        .route(
            "/debug/pprof/",
            get(|| async {
                Html(
                    "<!doctype html><title>carbon-rs pprof</title><h1>CPU profiling</h1>\
             <p><a href=\"profile?seconds=30\">Record a 30-second CPU profile</a></p>\
             <p>seconds: 1–300; default: 30. One recording at a time. \
             Heap, allocation, goroutine, mutex, block and trace profiles are not implemented.</p>",
                )
            }),
        )
        .route("/debug/pprof/profile", get(profile))
        .with_state(stop)
}

#[derive(Deserialize)]
struct Parameters {
    seconds: Option<u64>,
}

async fn profile(
    State(mut stop): State<watch::Receiver<bool>>,
    Query(parameters): Query<Parameters>,
) -> Result<Response, (StatusCode, String)> {
    if !SUPPORTED {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "CPU profiling requires 64-bit Linux".into(),
        ));
    }
    let seconds = parameters.seconds.unwrap_or(30);
    if !(1..=300).contains(&seconds) {
        return Err((
            StatusCode::BAD_REQUEST,
            "seconds must be an integer from 1 to 300".into(),
        ));
    }
    if *stop.borrow() {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "server stopping".into()));
    }
    let permit = RECORDING.try_acquire().map_err(|_| {
        (
            StatusCode::CONFLICT,
            "CPU profile already in progress".into(),
        )
    })?;
    // Dropping the request or stopping the daemon wakes the blocking worker.
    // The worker owns the permit until the profiler has actually been stopped.
    let (cancel, cancelled) = mpsc::channel::<()>();
    let recording = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        record(Duration::from_secs(seconds), cancelled)
    });
    let result = tokio::select! {
        result = recording => result.map_err(internal)?.map_err(internal),
        _ = stop.changed() => Err((StatusCode::SERVICE_UNAVAILABLE, "server stopping".into())),
    };
    drop(cancel);
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"profile\"",
            ),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        result?,
    )
        .into_response())
}

#[cfg(all(
    any(target_os = "linux", target_os = "macos"),
    target_pointer_width = "64"
))]
fn record(duration: Duration, cancelled: mpsc::Receiver<()>) -> Result<Vec<u8>, String> {
    use flate2::{Compression, write::GzEncoder};
    use pprof::protos::Message;
    use std::io::Write;
    use std::sync::mpsc::RecvTimeoutError;
    let builder = pprof::ProfilerGuardBuilder::default().frequency(100);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    let builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso", "libunwind"]);
    let guard = builder.build().map_err(|e| e.to_string())?;
    match cancelled.recv_timeout(duration) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => return Err("CPU profile cancelled".into()),
    }
    let report = guard
        .report()
        .frames_post_processor(|frames| {
            // pprof 0.15 keeps the sampler's inner unwinder frames. Excluding that
            // prefix prevents attributing application CPU time to the profiler.
            let prefix = frames
                .frames
                .iter()
                .take_while(|symbols| {
                    symbols.iter().all(|symbol| {
                        let name = symbol.name();
                        name.starts_with("backtrace::backtrace::")
                            || name.contains("pprof::backtrace::")
                    })
                })
                .count();
            frames.frames.drain(..prefix);
        })
        .build()
        .map_err(|e| e.to_string())?;
    drop(guard);
    let profile = report.pprof().map_err(|e| e.to_string())?;
    let mut compressed = GzEncoder::new(Vec::new(), Compression::default());
    compressed
        .write_all(&profile.write_to_bytes().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    compressed.finish().map_err(|e| e.to_string())
}

#[cfg(not(all(
    any(target_os = "linux", target_os = "macos"),
    target_pointer_width = "64"
)))]
fn record(_: Duration, _: mpsc::Receiver<()>) -> Result<Vec<u8>, String> {
    Err("CPU profiling requires 64-bit Linux".into())
}

fn internal(error: impl std::fmt::Display) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("CPU profiling failed: {error}"),
    )
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64"))]
mod tests {
    use super::*;
    use pprof::protos::Message;
    use std::io::Read;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[inline(never)]
    fn burn_cpu(stop: &AtomicBool) {
        let mut value = 1u64;
        while !stop.load(Ordering::Relaxed) {
            for _ in 0..10_000 {
                value =
                    std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
            }
        }
        std::hint::black_box(value);
    }

    async fn wait_for_permits(expected: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while RECORDING.available_permits() != expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_recording_is_valid_exclusive_and_cancellable() {
        let (stop_tx, stop_rx) = watch::channel(false);
        assert_eq!(
            profile(
                State(stop_rx.clone()),
                Query(Parameters { seconds: Some(0) })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            profile(
                State(stop_rx.clone()),
                Query(Parameters { seconds: Some(301) })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::BAD_REQUEST
        );
        let stop_burn = Arc::new(AtomicBool::new(false));
        let burn_stop = stop_burn.clone();
        let burner = std::thread::spawn(move || burn_cpu(&burn_stop));
        let request = tokio::spawn(profile(
            State(stop_rx.clone()),
            Query(Parameters { seconds: Some(1) }),
        ));
        wait_for_permits(0).await;
        assert_eq!(
            profile(
                State(stop_rx.clone()),
                Query(Parameters { seconds: Some(1) })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::CONFLICT
        );
        let response = tokio::time::timeout(Duration::from_secs(20), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        stop_burn.store(true, Ordering::Relaxed);
        burner.join().unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/octet-stream"
        );
        let compressed = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut bytes = Vec::new();
        flate2::read::GzDecoder::new(compressed.as_ref())
            .read_to_end(&mut bytes)
            .unwrap();
        let profile_data = pprof::protos::Profile::parse_from_bytes(&bytes).unwrap();
        // Optional artifact for go tool pprof verification, including failed assertions.
        if let Some(path) = std::env::var_os("CARBON_RS_TEST_CPU_PROFILE") {
            std::fs::write(path, &compressed).unwrap();
        }
        assert!(profile_data.duration_nanos >= 1_000_000_000);
        assert_eq!(profile_data.period, 10_000_000);
        assert_eq!(
            profile_data.string_table[profile_data.period_type.as_ref().unwrap().ty as usize],
            "cpu"
        );
        assert!(profile_data.sample.iter().any(|sample| sample.value[0] > 0));
        assert!(
            profile_data
                .string_table
                .iter()
                .any(|symbol| symbol.contains("burn_cpu"))
        );
        for sample in &profile_data.sample {
            let location = profile_data
                .location
                .iter()
                .find(|location| location.id == sample.location_id[0])
                .unwrap();
            let function = profile_data
                .function
                .iter()
                .find(|function| function.id == location.line[0].function_id)
                .unwrap();
            let name = &profile_data.string_table[function.name as usize];
            assert!(
                !name.starts_with("backtrace::backtrace::") && !name.contains("pprof::backtrace::"),
                "sampler frame leaked: {name}"
            );
        }

        // Abandoning a handler must release the process-wide profiler, not wait 300 seconds.
        let request = tokio::spawn(profile(
            State(stop_rx.clone()),
            Query(Parameters { seconds: Some(300) }),
        ));
        wait_for_permits(0).await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        wait_for_permits(1).await;
        let request = tokio::spawn(profile(
            State(stop_rx.clone()),
            Query(Parameters { seconds: Some(300) }),
        ));
        wait_for_permits(0).await;
        stop_tx.send_replace(true);
        assert_eq!(
            request.await.unwrap().unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        wait_for_permits(1).await;
        assert_eq!(
            profile(State(stop_rx), Query(Parameters { seconds: Some(1) }))
                .await
                .unwrap_err()
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
