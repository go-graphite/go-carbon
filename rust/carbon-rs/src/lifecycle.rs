use std::sync::Arc;
use std::time::Duration;

use crate::app::App;

/// Runs small compaction passes. The Go setting is metrics per second, not MiB/s.
pub async fn compact(app: Arc<App>, mut stop: tokio::sync::watch::Receiver<bool>) {
    let rate = app.config.whisper.out_of_order_compact_rate;
    if rate == 0 || !app.config.whisper.out_of_order {
        return;
    }
    let delay = Duration::from_secs_f64(1.0 / rate as f64);
    loop {
        for metric in app.index.list() {
            if *stop.borrow() {
                return;
            }
            let Ok(path) = app.path(&metric) else {
                continue;
            };
            let sidecar = std::path::PathBuf::from(format!("{}.ooo", path.display()));
            if std::fs::metadata(sidecar).map_or(true, |meta| {
                meta.len() < app.config.whisper.out_of_order_compact_threshold
            }) {
                continue;
            }
            let app = app.clone();
            let metric = metric.clone();
            match tokio::task::spawn_blocking(move || app.compact_one(&metric)).await {
                Ok(Ok(true)) => {
                    tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(delay) => {} }
                }
                Ok(Ok(false)) => {}
                result => eprintln!("out-of-order compaction failed: {result:?}"),
            }
        }
        tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
}
