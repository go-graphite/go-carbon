use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cache::Cache;
use crate::config::{Config, Rules};
use crate::index::{Index, IndexMode, MetricMeta};
use crate::quotas::{Costs, Engine};
use tokio::sync::Notify;
use whisper_rs::{Metadata, Options, Point, TimeSeries, Whisper};

pub struct App {
    pub config: Config,
    pub cache: Cache,
    pub index: Index,
    pub quotas: Option<Engine>,
    pub prometheus: Option<Arc<crate::metrics::Metrics>>,
    pub wake: Notify,
    pub received: AtomicU64,
    pub rejected: AtomicU64,
    pub invalid: AtomicU64,
    pub write_errors: AtomicU64,
    pub read_generation: AtomicU64,
    rejection_logged: AtomicI64,
    rules: RwLock<Rules>,
    mutation: Mutex<()>,
    files: Vec<Mutex<()>>,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

impl App {
    pub fn new(config: Config) -> io::Result<Arc<Self>> {
        // Config::load already ran validate_diagnostics; Metrics::new re-checks its own part.
        let prometheus = if config.prometheus.enabled {
            Some(Arc::new(
                crate::metrics::Metrics::new(&config).map_err(io::Error::other)?,
            ))
        } else {
            None
        };
        fs::create_dir_all(&config.whisper.data_dir)?;
        let rules = Rules::load(
            &config.whisper.schemas_file,
            &config.whisper.aggregation_file,
            &config.whisper,
        )
        .map_err(invalid)?;
        let quotas = if config.whisper.quotas_file.is_empty() {
            None
        } else {
            Some(
                Engine::load(
                    &config.whisper.quotas_file,
                    config.carbonserver.quota_usage_report_frequency,
                )
                .map_err(invalid)?,
            )
        };
        Ok(Arc::new(Self {
            cache: Cache::new(config.cache.max_size, config.cache.max_bytes),
            index: Index::new(if config.carbonserver.trie_index {
                IndexMode::Trie
            } else {
                IndexMode::Trigram
            }),
            config,
            rules: RwLock::new(rules),
            quotas,
            prometheus,
            mutation: Mutex::new(()),
            files: (0..1024).map(|_| Mutex::new(())).collect(),
            wake: Notify::new(),
            received: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            invalid: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            read_generation: AtomicU64::new(0),
            rejection_logged: AtomicI64::new(0),
        }))
    }

    /// Name checks plus the on-disk location. No I/O: this runs once per ingested point.
    pub fn path(&self, metric: &str) -> io::Result<PathBuf> {
        // PATH_MAX (4096, NUL included) bounds data_dir + '/' + metric-as-path + ".wsp".
        if metric.is_empty()
            || self.config.whisper.data_dir.len() + metric.len() + 5 > 4095
            || metric.bytes().any(|b| matches!(b, b'/' | b'\\' | 0))
        {
            return Err(invalid("invalid metric path"));
        }
        if metric
            .split('.')
            .any(|component| component.is_empty() || component.len() > 251)
        {
            return Err(invalid("empty or oversized metric component"));
        }
        let mut path = PathBuf::from(&self.config.whisper.data_dir);
        path.extend(metric.split('.'));
        path.set_extension("wsp");
        Ok(path)
    }

    /// Refuses to create a file through a symlink planted inside data_dir.
    fn check_symlinks(&self, metric: &str) -> io::Result<()> {
        let mut path = PathBuf::from(&self.config.whisper.data_dir);
        for component in metric.split('.') {
            path.push(component);
            if path
                .symlink_metadata()
                .is_ok_and(|m| m.file_type().is_symlink())
            {
                return Err(invalid("metric path contains symlink"));
            }
        }
        path.set_extension("wsp");
        if path
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err(invalid("metric file is a symlink"));
        }
        Ok(())
    }

    fn options(&self) -> Options {
        Options {
            sparse: self.config.whisper.sparse_create,
            flock: self.config.whisper.flock,
            compressed: self.config.whisper.compressed,
            out_of_order: self.config.whisper.out_of_order,
        }
    }
    fn file_lock(&self, metric: &str) -> &Mutex<()> {
        &self.files[crc32fast::hash(metric.as_bytes()) as usize % self.files.len()]
    }
    /// Counts a dropped point. Per-point logs would flood under sustained overflow, so the
    /// operator-visible warning fires at most once a minute.
    fn reject(&self, metric: &str, reason: &str) -> io::Error {
        let rejected = self.rejected.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::debug!(target: "cache", metric, reason, "point rejected");
        let at = now();
        let last = self.rejection_logged.load(Ordering::Relaxed);
        if at - last >= 60
            && self
                .rejection_logged
                .compare_exchange(last, at, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(target: "cache", metric, reason, rejected, "points rejected");
        }
        io::Error::new(io::ErrorKind::WouldBlock, reason.to_owned())
    }

    /// Admission reserves quota and publishes a cache-only metric before returning success.
    pub fn ingest(&self, metric: String, point: Point) -> io::Result<()> {
        self.path(&metric)?;
        if !point.timestamp.is_positive()
            || point.timestamp > u32::MAX as i64
            || point.value.is_nan()
        {
            return Err(invalid("invalid Whisper point"));
        }
        let metadata = self
            .rules
            .read()
            .unwrap()
            .metadata(&metric)
            .ok_or_else(|| invalid("no matching storage schema"))?;
        // ponytail: serialize catalog admission; partition by root only after measuring contention.
        let _mutation = self.mutation.lock().unwrap_or_else(PoisonError::into_inner);
        let existing = self.index.get(&metric);
        let meta = existing
            .clone()
            .unwrap_or_else(|| estimated_meta(&metadata, self.config.whisper.sparse_create));
        let reservation = self
            .quotas
            .as_ref()
            .map(|q| q.admit(&metric, 1, costs(&meta)))
            .transpose()
            .map_err(|e| self.reject(&metric, &format!("quota exceeded: {e:?}")))?;
        if self.cache.add(metric.clone(), point).is_err() {
            if let (Some(q), Some(r)) = (&self.quotas, reservation) {
                q.release(r);
            }
            return Err(self.reject(&metric, "cache full"));
        }
        if let (Some(q), Some(r)) = (&self.quotas, reservation) {
            q.commit(r);
        }
        if existing.is_none() {
            self.index.upsert(&metric, meta);
        }
        self.received.fetch_add(1, Ordering::Relaxed);
        self.read_generation.fetch_add(1, Ordering::Relaxed);
        self.wake.notify_one();
        Ok(())
    }

    pub fn metadata(&self, metric: &str) -> io::Result<Metadata> {
        let path = self.path(metric)?;
        let _lock = self
            .file_lock(metric)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match Whisper::open(path, self.options()) {
            Ok(w) => Ok(w.metadata().clone()),
            Err(e) if e.kind() == io::ErrorKind::NotFound && !self.cache.get(metric).is_empty() => {
                self.rules
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .metadata(metric)
                    .ok_or(e)
            }
            Err(e) => Err(e),
        }
    }

    pub fn fetch(
        &self,
        metric: &str,
        from: i64,
        until: i64,
        at: i64,
    ) -> io::Result<Option<TimeSeries>> {
        self.fetch_with_metadata(metric, from, until, at)
            .map(|(_, series)| series)
    }

    /// The series and the metadata it was read with, under one file lock.
    pub fn fetch_with_metadata(
        &self,
        metric: &str,
        from: i64,
        until: i64,
        at: i64,
    ) -> io::Result<(Metadata, Option<TimeSeries>)> {
        let path = self.path(metric)?;
        let _lock = self
            .file_lock(metric)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let metrics = self
            .prometheus
            .as_ref()
            .and_then(|m| m.carbonserver.as_ref());
        let wait = metrics.map(|_| Instant::now());
        let cached = self.cache.get(metric);
        let wait = wait.map(|wait| wait.elapsed().as_secs_f64());
        // Instrumentation mirrors Go's fetchfromdisk.go: cache "wait" is observed only when the
        // finest archive serves the query; disk_requests counts attempts, while disk_wait covers
        // successful fetches only and includes the file close.
        let (metadata, mut series) = match Whisper::open(path, self.options()) {
            Ok(mut w) => {
                let metadata = w.metadata().clone();
                if at.saturating_sub(from)
                    <= i64::from(metadata.retentions[0].seconds_per_point)
                        * i64::from(metadata.retentions[0].points)
                    && let (Some(metrics), Some(wait)) = (metrics, wait)
                {
                    metrics
                        .cache_durations
                        .with_label_values(&["wait"])
                        .observe(wait);
                }
                let start = metrics.map(|m| {
                    m.disk_requests.inc();
                    Instant::now()
                });
                let series = w.fetch(from, until, at)?;
                drop(w);
                if series.is_some()
                    && let (Some(metrics), Some(start)) = (metrics, start)
                {
                    metrics.disk_wait.observe(start.elapsed().as_secs_f64());
                }
                (metadata, series)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let (Some(metrics), Some(wait)) = (metrics, wait) {
                    metrics
                        .cache_durations
                        .with_label_values(&["wait"])
                        .observe(wait);
                }
                if cached.is_empty() {
                    return Err(e);
                }
                let meta = self
                    .rules
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .metadata(metric)
                    .ok_or(e)?;
                let archive = &meta.retentions[0];
                let step = archive.seconds_per_point as i64;
                let start =
                    from.max(at - step * archive.points as i64).div_euclid(step) * step + step;
                let end = until.min(at).div_euclid(step) * step + step;
                let series = if end <= start {
                    None
                } else {
                    Some(TimeSeries {
                        from: start,
                        until: end,
                        step: step as u32,
                        values: vec![None; ((end - start) / step) as usize],
                    })
                };
                (meta, series)
            }
            Err(e) => return Err(e),
        };
        if let Some(series) = &mut series
            && series.step == metadata.retentions[0].seconds_per_point
            && !cached.is_empty()
        {
            let start = metrics.map(|_| Instant::now());
            let mut hit = false;
            for point in cached {
                let timestamp = point.timestamp.div_euclid(series.step as i64) * series.step as i64;
                if timestamp >= series.from && timestamp < series.until {
                    series.values[((timestamp - series.from) / series.step as i64) as usize] =
                        Some(point.value);
                    hit = true;
                }
            }
            if let (Some(metrics), Some(start)) = (metrics, start) {
                metrics.cache_request("metric", hit);
                metrics
                    .cache_durations
                    .with_label_values(&["work"])
                    .observe(start.elapsed().as_secs_f64());
            }
        }
        if let (Some(metrics), Some(series)) = (metrics, &series) {
            metrics.returned_metrics.inc();
            metrics.returned_points.inc_by(series.values.len() as u64);
        }
        Ok((metadata, series))
    }

    /// One metric has at most one in-flight batch; its lock spans write and confirmation.
    pub fn flush_one(&self) -> io::Result<bool> {
        let Some(batch) = self.cache.take() else {
            return Ok(false);
        };
        let _lock = self
            .file_lock(&batch.metric)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let result = (|| {
            let path = self.path(&batch.metric)?;
            let mut w = match Whisper::open(&path, self.options()) {
                Ok(w) => w,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.check_symlinks(&batch.metric)?;
                    fs::create_dir_all(path.parent().unwrap())?;
                    let metadata = self
                        .rules
                        .read()
                        .unwrap()
                        .metadata(&batch.metric)
                        .ok_or_else(|| invalid("no storage schema"))?;
                    let mut options = self.options();
                    options.compressed = metadata.compressed;
                    let file = Whisper::create(&path, metadata, options)?;
                    tracing::info!(target: "whisper:new", metric = %batch.metric, path = %path.display(), "new whisper file");
                    file
                }
                Err(e) => return Err(e),
            };
            if let Some(metrics) = &self.prometheus {
                // Go observes all points reaching UpdateMany, including failed attempts.
                let at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                for point in batch.points.iter() {
                    metrics.write_lag.observe(at - point.timestamp as f64);
                }
            }
            w.update_many(&batch.points, now())?;
            w.sync()?;
            let meta = disk_meta(
                &path,
                w.metadata(),
                self.index
                    .get(&batch.metric)
                    .map(|m| m.first_seen_at)
                    .unwrap_or_else(now),
            )?;
            let _mutation = self.mutation.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(q) = &self.quotas {
                q.sync_metric(&batch.metric, costs(&meta));
            }
            self.index.upsert(&batch.metric, meta);
            self.cache.confirm(batch.id);
            self.read_generation.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })();
        if let Err(e) = result {
            self.cache.retry(batch.id);
            self.write_errors.fetch_add(1, Ordering::Relaxed);
            tracing::error!(target: "persister", metric = %batch.metric, error = %e, "fail to update metric; batch requeued");
            return Err(e);
        }
        Ok(true)
    }

    pub fn scan(&self) -> io::Result<()> {
        let started = Instant::now();
        let before: BTreeMap<_, _> = self
            .index
            .snapshot()
            .into_iter()
            .map(|(name, meta, revision)| (name, (meta, revision)))
            .collect();
        let root = Path::new(&self.config.whisper.data_dir);
        let mut files = vec![];
        walk(root, &mut files)?;
        let mut entries = BTreeMap::new();
        for path in files {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| invalid(e.to_string()))?;
            let metric = relative
                .with_extension("")
                .components()
                .map(|c| {
                    c.as_os_str()
                        .to_str()
                        .ok_or_else(|| invalid("non-UTF8 metric path"))
                })
                .collect::<io::Result<Vec<_>>>()?
                .join(".");
            self.path(&metric)?;
            let _lock = self
                .file_lock(&metric)
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Files removed since walk() are skipped rather than failing the whole scan.
            let Some(w) = skip_missing(Whisper::open(&path, self.options()))? else {
                continue;
            };
            let first_seen = before
                .get(&metric)
                .map(|(m, _)| m.first_seen_at)
                .unwrap_or_else(now);
            let Some(meta) = skip_missing(disk_meta(&path, w.metadata(), first_seen))? else {
                continue;
            };
            entries.insert(metric, meta);
        }
        let _mutation = self.mutation.lock().unwrap_or_else(PoisonError::into_inner);
        // A filesystem walk is not a snapshot. Keep arrivals and writes made during it.
        for (name, meta, revision) in self.index.snapshot() {
            if before.get(&name).map(|(_, rev)| *rev) != Some(revision)
                || !self.cache.get(&name).is_empty()
            {
                entries.insert(name, meta);
            }
        }
        if let Some(q) = &self.quotas {
            q.reconcile(entries.iter().map(|(n, m)| (n.clone(), costs(m))).collect());
        }
        let generation = self.index.generation();
        let metrics = entries.len();
        self.index.reconcile_if_generation(generation, entries);
        self.read_generation.fetch_add(1, Ordering::Relaxed);
        drop(_mutation);
        tracing::info!(target: "carbonserver", metrics, "runtime_seconds.duration_ns" = started.elapsed().as_nanos() as u64, "file list updated");
        Ok(())
    }

    pub fn dump(&self) -> io::Result<PathBuf> {
        tracing::info!(target: "dump", dir = %self.config.dump.path, "dump started");
        fs::create_dir_all(&self.config.dump.path)?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = PathBuf::from(&self.config.dump.path)
            .join(format!("cache.{}.{stamp}.bin", std::process::id()));
        let tmp = path.with_extension("bin.tmp");
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        for (metric, points) in self.cache.dump() {
            write_dump(&mut file, &metric, &points)?;
        }
        file.sync_all()?;
        fs::rename(&tmp, &path)?;
        // Directory fsync is best effort: NFS and tmpfs reject it, and the file is already durable.
        let _ = File::open(&self.config.dump.path).and_then(|dir| dir.sync_all());
        tracing::info!(target: "dump", filename = %path.display(), "dump finished");
        Ok(path)
    }

    pub fn restore(&self) -> io::Result<Vec<PathBuf>> {
        let dir = Path::new(&self.config.dump.path);
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut files: Vec<_> = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name().and_then(|x| x.to_str()).is_some_and(|n| {
                    (n.starts_with("cache.") || n.starts_with("input."))
                        && !n.ends_with(".tmp")
                        && !n.ends_with(".restored")
                })
            })
            .collect();
        files.sort_by_key(|p| {
            let n = p.file_name().unwrap().to_string_lossy();
            let mut parts = n.split('.');
            let kind = parts.next().unwrap_or("");
            let timestamp = parts
                .nth(1)
                .and_then(|t| t.parse::<u128>().ok())
                .unwrap_or(0);
            (timestamp, kind.to_owned())
        });
        for path in &files {
            tracing::info!(target: "restore", filename = %path.display(), "restore started");
            let mut reader = BufReader::new(File::open(path)?);
            if path.extension().is_some_and(|e| e == "bin") {
                while let Some((metric, points)) = read_dump(&mut reader)? {
                    self.restore_points(metric, points)?;
                }
            } else {
                let mut line = String::new();
                while reader.read_line(&mut line)? != 0 {
                    if !line.ends_with('\n') {
                        return Err(invalid("unfinished dump line"));
                    }
                    match crate::plaintext::parse_line(line.as_bytes()) {
                        Ok((metric, point)) => self.restore_points(metric, vec![point])?,
                        Err(_) => {
                            self.invalid.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    line.clear();
                }
            }
            tracing::info!(target: "restore", filename = %path.display(), "dump loaded into cache");
        }
        // Keep originals until all restored points have reached durable storage.
        Ok(files)
    }

    fn restore_points(&self, metric: String, points: Vec<Point>) -> io::Result<()> {
        self.path(&metric)?;
        let meta = self
            .rules
            .read()
            .unwrap()
            .metadata(&metric)
            .ok_or_else(|| invalid("no storage schema for restored metric"))?;
        self.cache.restore(metric.clone(), points);
        self.read_generation.fetch_add(1, Ordering::Relaxed);
        if self.index.get(&metric).is_none() {
            let meta = estimated_meta(&meta, self.config.whisper.sparse_create);
            if let Some(q) = &self.quotas {
                q.register_existing(&metric, costs(&meta));
            }
            self.index.upsert(&metric, meta);
        }
        Ok(())
    }

    pub fn reload_rules(&self) -> io::Result<()> {
        let rules = Rules::load(
            &self.config.whisper.schemas_file,
            &self.config.whisper.aggregation_file,
            &self.config.whisper,
        )
        .map_err(invalid)?;
        *self.rules.write().unwrap_or_else(PoisonError::into_inner) = rules;
        self.read_generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn compact_one(&self, metric: &str) -> io::Result<bool> {
        let path = self.path(metric)?;
        let sidecar = whisper_rs::out_of_order_sidecar_path(&path);
        let _lock = self
            .file_lock(metric)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Checked under the lock: flush_one removes the sidecar once it is merged.
        if !sidecar.try_exists()? {
            return Ok(false);
        }
        let mut w = Whisper::open(&path, self.options())?;
        w.compact_out_of_order(now())?;
        let _mutation = self.mutation.lock().unwrap_or_else(PoisonError::into_inner);
        let first_seen = self
            .index
            .get(metric)
            .map(|m| m.first_seen_at)
            .unwrap_or_else(now);
        let meta = disk_meta(&path, w.metadata(), first_seen)?;
        if let Some(q) = &self.quotas {
            q.sync_metric(metric, costs(&meta));
        }
        self.index.upsert(metric, meta);
        self.read_generation.fetch_add(1, Ordering::Relaxed);
        drop(_mutation);
        tracing::debug!(target: "persister", metric, path = %path.display(), "merged out-of-order sidecar into cwhisper file");
        Ok(true)
    }

    pub fn load_file_list(&self, entries: Vec<crate::file_list::Entry>) -> io::Result<()> {
        let _mutation = self.mutation.lock().unwrap_or_else(PoisonError::into_inner);
        let mut catalog = Vec::with_capacity(entries.len());
        for entry in entries {
            let name = entry
                .path
                .trim_start_matches('/')
                .trim_end_matches(".wsp")
                .replace('/', ".");
            self.path(&name)?;
            let meta = MetricMeta {
                data_points: entry.data_points,
                logical_size: entry.logical_size,
                physical_size: entry.physical_size,
                first_seen_at: entry.first_seen_at,
            };
            catalog.push((name, meta));
        }
        if let Some(q) = &self.quotas {
            q.reconcile(catalog.iter().map(|(n, m)| (n.clone(), costs(m))).collect());
        }
        self.index
            .reconcile_if_generation(self.index.generation(), catalog);
        Ok(())
    }

    pub fn save_file_list(&self) -> Vec<crate::file_list::Entry> {
        self.index
            .snapshot()
            .into_iter()
            .map(|(name, m, _)| crate::file_list::Entry {
                path: format!("/{}.wsp", name.replace('.', "/")),
                logical_size: m.logical_size,
                physical_size: m.physical_size,
                data_points: m.data_points,
                first_seen_at: m.first_seen_at,
            })
            .collect()
    }
}

fn walk(path: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(&entry.path(), out)?;
        } else if kind.is_file() && entry.path().extension().is_some_and(|e| e == "wsp") {
            out.push(entry.path());
        }
    }
    Ok(())
}
fn estimated_meta(metadata: &Metadata, sparse: bool) -> MetricMeta {
    let data_points = metadata
        .retentions
        .iter()
        .map(|r| r.points as u64)
        .sum::<u64>();
    let logical_size = 4096 + data_points * 12;
    MetricMeta {
        data_points,
        logical_size,
        physical_size: if sparse { 4096 } else { logical_size },
        first_seen_at: now(),
    }
}
fn skip_missing<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn disk_meta(path: &Path, metadata: &Metadata, first_seen_at: i64) -> io::Result<MetricMeta> {
    use std::os::unix::fs::MetadataExt;
    let file = fs::metadata(path)?;
    let sidecar = fs::metadata(format!("{}.ooo", path.display()));
    let (side_logical, side_physical) = match sidecar {
        Ok(m) => (m.len(), m.blocks() * 512),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (0, 0),
        Err(e) => return Err(e),
    };
    Ok(MetricMeta {
        data_points: metadata.retentions.iter().map(|r| r.points as u64).sum(),
        logical_size: file.len() + side_logical,
        physical_size: file.blocks() * 512 + side_physical,
        first_seen_at,
    })
}
fn costs(meta: &MetricMeta) -> Costs {
    Costs::metric(
        meta.data_points.min(i64::MAX as u64) as i64,
        meta.logical_size.min(i64::MAX as u64) as i64,
        meta.physical_size.min(i64::MAX as u64) as i64,
    )
}

pub fn write_dump(mut out: impl Write, metric: &str, points: &[Point]) -> io::Result<()> {
    write_varint(&mut out, metric.len() as i64)?;
    out.write_all(metric.as_bytes())?;
    write_varint(&mut out, points.len() as i64)?;
    let (mut value, mut time) = (0i64, 0i64);
    for p in points {
        let bits = p.value.to_bits() as i64;
        write_varint(&mut out, bits.wrapping_sub(value))?;
        write_varint(&mut out, p.timestamp.wrapping_sub(time))?;
        value = bits;
        time = p.timestamp;
    }
    Ok(())
}
pub fn read_dump(input: &mut impl BufRead) -> io::Result<Option<(String, Vec<Point>)>> {
    if input.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let length = read_varint(input)?;
    if !(1..=4096).contains(&length) {
        return Err(invalid("invalid dump metric length"));
    }
    let mut name = vec![0; length as usize];
    input.read_exact(&mut name)?;
    let name = String::from_utf8(name).map_err(|_| invalid("non-UTF8 dump metric"))?;
    let count = read_varint(input)?;
    if !(0..=10_000_000).contains(&count) {
        return Err(invalid("invalid dump point count"));
    }
    let (mut value, mut timestamp) = (0i64, 0i64);
    let mut points = Vec::new();
    for _ in 0..count {
        value = value.wrapping_add(read_varint(input)?);
        timestamp = timestamp.wrapping_add(read_varint(input)?);
        points.push(Point {
            timestamp,
            value: f64::from_bits(value as u64),
        });
    }
    Ok(Some((name, points)))
}
fn write_varint(out: &mut impl Write, value: i64) -> io::Result<()> {
    let mut v = (value as u64) << 1;
    if value < 0 {
        v = !v;
    }
    let mut bytes = [0; 10];
    let mut n = 0;
    while v >= 128 {
        bytes[n] = v as u8 | 128;
        n += 1;
        v >>= 7;
    }
    bytes[n] = v as u8;
    out.write_all(&bytes[..=n])
}
fn read_varint(input: &mut impl Read) -> io::Result<i64> {
    let mut v = 0u64;
    for i in 0..10 {
        let mut b = [0];
        input.read_exact(&mut b)?;
        if i == 9 && b[0] > 1 {
            return Err(invalid("dump varint overflow"));
        }
        v |= ((b[0] & 127) as u64) << (7 * i);
        if b[0] < 128 {
            return Ok(if v & 1 == 0 {
                (v >> 1) as i64
            } else {
                !(v >> 1) as i64
            });
        }
    }
    Err(invalid("dump varint overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dump_roundtrip_and_truncation() {
        let points = vec![
            Point {
                timestamp: 8,
                value: -2.5,
            },
            Point {
                timestamp: 7,
                value: f64::INFINITY,
            },
        ];
        let mut bytes = vec![];
        write_dump(&mut bytes, "a.b", &points).unwrap();
        assert_eq!(
            read_dump(&mut bytes.as_slice()).unwrap(),
            Some(("a.b".into(), points))
        );
        for n in 1..bytes.len() {
            assert!(read_dump(&mut &bytes[..n]).is_err());
        }
    }
}
