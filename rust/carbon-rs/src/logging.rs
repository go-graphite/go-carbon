//! Go's zapwriter configuration and output conventions over Rust tracing events.
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use tracing::{Event, Level, Metadata, Subscriber, field::Visit};
use tracing_subscriber::fmt::{
    format::Writer,
    time::{ChronoLocal, FormatTime},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    pub logger: String,
    pub file: String,
    pub level: String,
    pub encoding: String,
    pub encoding_time: String,
    pub encoding_duration: String,
    pub sample_tick: String,
    pub sample_initial: u64,
    pub sample_thereafter: u64,
}

impl Default for Config {
    fn default() -> Self {
        // An explicit, partially filled [[logging]] has zapwriter's zero-value defaults.
        Self {
            logger: String::new(),
            file: "stderr".into(),
            level: "info".into(),
            encoding: "mixed".into(),
            encoding_time: "iso8601".into(),
            encoding_duration: "seconds".into(),
            sample_tick: String::new(),
            sample_initial: 0,
            sample_thereafter: 0,
        }
    }
}

impl Config {
    pub fn application_default() -> Self {
        Self {
            file: "stdout".into(),
            encoding: "console".into(),
            ..Self::default()
        }
    }

    fn prepare(&self) -> Result<(Self, Option<Duration>), String> {
        let mut config = self.clone();
        let (path, query) = self.file.split_once('?').unwrap_or((&self.file, ""));
        if !query.is_empty() {
            let uri = format!("/?{query}")
                .parse()
                .map_err(|e| format!("logging.file: {e}"))?;
            let axum::extract::Query(params) =
                axum::extract::Query::<HashMap<String, String>>::try_from_uri(&uri)
                    .map_err(|e| format!("logging.file: {e}"))?;
            for (key, value) in params {
                let field = match key.as_str() {
                    "level" => &mut config.level,
                    "encoding" => &mut config.encoding,
                    "encoding-time" => &mut config.encoding_time,
                    "encoding-duration" => &mut config.encoding_duration,
                    _ => return Err(format!("unsupported logging.file parameter: {key}")),
                };
                if !value.is_empty() {
                    *field = value;
                }
            }
        }
        let path = if let Some(path) = path.strip_prefix("file://") {
            if !path.starts_with('/') {
                return Err("logging.file requires a local file URI".into());
            }
            path
        } else if let Some(path) = path.strip_prefix("file:") {
            if !path.starts_with('/') {
                return Err("logging.file requires an absolute file URI".into());
            }
            path
        } else {
            if path.split('/').next().unwrap_or("").contains(':') {
                return Err("logging supports only local files, stdout, stderr and none".into());
            }
            path
        };
        config.file = decode_path(path)?;
        config.level = config.level.to_ascii_lowercase();
        severity(&config.level)?;
        for (value, default, allowed, name) in [
            (
                &mut config.encoding,
                "mixed",
                &["mixed", "console", "json"][..],
                "encoding",
            ),
            (
                &mut config.encoding_time,
                "iso8601",
                &["iso8601", "millis", "nanos", "epoch"][..],
                "encoding-time",
            ),
            (
                &mut config.encoding_duration,
                "seconds",
                &["seconds", "nanos", "string"][..],
                "encoding-duration",
            ),
        ] {
            *value = if value.is_empty() {
                default.into()
            } else {
                value.to_ascii_lowercase()
            };
            if !allowed.contains(&value.as_str()) {
                return Err(format!("invalid logging.{name}: {value}"));
            }
        }
        let tick = if config.sample_tick.is_empty() {
            None
        } else {
            if config.sample_thereafter == 0 {
                return Err(
                    "logging.sample-thereafter must be positive when sampling is enabled".into(),
                );
            }
            Some(
                sample_tick(&config.sample_tick)
                    .ok_or("logging.sample-tick must be a positive duration")?,
            )
        };
        Ok((config, tick))
    }
}

pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Config>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Entries {
        Many(Vec<Config>),
        One(Config),
    }
    Ok(match Entries::deserialize(d)? {
        Entries::Many(v) => v,
        Entries::One(v) => vec![v],
    })
}

pub fn validate(config: &[Config]) -> Result<(), String> {
    for entry in config {
        entry.prepare()?;
    }
    Ok(())
}

fn severity(level: &str) -> Result<u8, String> {
    match level {
        "debug" => Ok(0),
        "" | "info" => Ok(1),
        "warn" => Ok(2),
        "error" => Ok(3),
        "dpanic" => Ok(4),
        "panic" => Ok(5),
        "fatal" => Ok(6),
        _ => Err(format!("invalid logging.level: {level}")),
    }
}
fn event_severity(level: &Level) -> u8 {
    match *level {
        Level::TRACE | Level::DEBUG => 0,
        Level::INFO => 1,
        Level::WARN => 2,
        Level::ERROR => 3,
    }
}

fn decode_path(path: &str) -> Result<String, String> {
    let mut bytes = Vec::with_capacity(path.len());
    let mut input = path.bytes();
    while let Some(b) = input.next() {
        if b == b'%' {
            let hi = input.next().and_then(|b| char::from(b).to_digit(16));
            let lo = input.next().and_then(|b| char::from(b).to_digit(16));
            let (hi, lo) = hi.zip(lo).ok_or("invalid escape in logging.file")?;
            bytes.push((hi * 16 + lo) as u8);
        } else {
            bytes.push(b);
        }
    }
    if bytes.iter().any(|b| b.is_ascii_control() || *b == b'#') {
        return Err("control characters and fragments are not supported in logging.file".into());
    }
    String::from_utf8(bytes).map_err(|_| "logging.file must be UTF-8".into())
}

fn sample_tick(value: &str) -> Option<Duration> {
    static PARTS: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)").unwrap()
    });
    let value = value.strip_prefix('+').unwrap_or(value);
    let mut end = 0;
    let mut seconds = 0.0;
    for cap in PARTS.captures_iter(value) {
        let part = cap.get(0)?;
        if part.start() != end {
            return None;
        }
        end = part.end();
        seconds += cap[1].parse::<f64>().ok()?
            * match &cap[2] {
                "ns" => 1e-9,
                "us" | "µs" | "μs" => 1e-6,
                "ms" => 1e-3,
                "s" => 1.0,
                "m" => 60.0,
                "h" => 3600.0,
                _ => return None,
            };
    }
    if end != value.len() || seconds <= 0.0 || seconds > i64::MAX as f64 / 1e9 {
        return None;
    }
    Duration::try_from_secs_f64(seconds)
        .ok()
        .filter(|d| !d.is_zero())
}

struct Sampler {
    tick: Duration,
    first: u64,
    thereafter: u64,
    counts: Vec<(Instant, u64)>,
}
impl Sampler {
    fn allow(&mut self, level: u8, message: &str, now: Instant) -> bool {
        // Same fixed-size FNV buckets as Zap: bounded memory even for dynamic messages.
        let hash = message.bytes().fold(2166136261u32, |h, b| {
            (h ^ u32::from(b)).wrapping_mul(16777619)
        });
        let (start, count) = &mut self.counts[usize::from(level) * 4096 + hash as usize % 4096];
        if *count == 0 || now.duration_since(*start) >= self.tick {
            *start = now;
            *count = 0;
        }
        *count = count.saturating_add(1);
        *count <= self.first || (*count - self.first).is_multiple_of(self.thereafter)
    }
}

enum Sink {
    Stdout,
    Stderr,
    File {
        file: File,
        path: String,
        checked: Instant,
    },
}
impl Sink {
    fn open(path: &str) -> io::Result<Self> {
        match path {
            "" | "stderr" => Ok(Self::Stderr),
            "stdout" => Ok(Self::Stdout),
            _ => {
                if let Some(parent) = Path::new(path)
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                {
                    fs::create_dir_all(parent)?;
                }
                Ok(Self::File {
                    file: open_file(path)?,
                    path: path.into(),
                    checked: Instant::now(),
                })
            }
        }
    }
    fn write(&mut self, line: &[u8]) -> io::Result<()> {
        match self {
            Self::Stdout => io::stdout().lock().write_all(line),
            Self::Stderr => io::stderr().lock().write_all(line),
            Self::File {
                file,
                path,
                checked,
            } => {
                // ponytail: check rotation on writes, at most once/second; add an idle checker only if needed.
                if checked.elapsed() >= Duration::from_secs(1) {
                    *checked = Instant::now();
                    let current = file.metadata()?;
                    if fs::metadata(&*path).map_or(true, |m| {
                        m.ino() != current.ino() || m.dev() != current.dev()
                    }) {
                        match open_file(path) {
                            Ok(next) => *file = next,
                            Err(error) => report_error(&format!("cannot reopen {path}: {error}")),
                        }
                    }
                }
                file.write_all(line)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stdout => io::stdout().flush(),
            Self::Stderr => io::stderr().flush(),
            Self::File { file, .. } => file.sync_data(),
        }
    }
}
fn open_file(path: &str) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o644)
        .open(path)
}
fn report_error(error: &str) {
    // Do not recurse into tracing when a sink fails. Never panic on a broken stderr pipe.
    let _ = writeln!(
        io::stderr().lock(),
        "{}",
        json!({"level":"ERROR", "logger":"logging", "message":"log output failed", "error":error})
    );
}

struct Output {
    config: Config,
    minimum: u8,
    sink: Option<Arc<Mutex<Sink>>>,
    sampler: Option<Mutex<Sampler>>,
}
struct LoggingLayer {
    outputs: Vec<Output>,
    names: HashSet<String>,
}

/// Holds the outputs until the async runtime has shut down, then synchronizes files.
pub struct Guard {
    sinks: Vec<Arc<Mutex<Sink>>>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        for sink in &self.sinks {
            if let Err(error) = sink.lock().unwrap_or_else(|e| e.into_inner()).flush() {
                report_error(&error.to_string());
            }
        }
    }
}

fn build(config: &[Config]) -> io::Result<(LoggingLayer, Guard)> {
    let defaults = [Config::application_default()];
    let config = if config.is_empty() { &defaults } else { config };
    // Validate every entry before opening any output.
    let prepared = config
        .iter()
        .map(Config::prepare)
        .collect::<Result<Vec<_>, _>>()
        .map_err(crate::app::invalid)?;
    let mut sinks = HashMap::new();
    let mut outputs = Vec::new();
    for (config, tick) in prepared {
        let sink = if config.file.eq_ignore_ascii_case("none") {
            None
        } else {
            let path = if config.file.is_empty() {
                "stderr"
            } else {
                &config.file
            };
            let sink = match sinks.entry(path.to_owned()) {
                std::collections::hash_map::Entry::Occupied(v) => v.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(Arc::new(Mutex::new(Sink::open(path).map_err(|e| {
                        io::Error::new(e.kind(), format!("logging.file {path}: {e}"))
                    })?)))
                }
            };
            Some(sink.clone())
        };
        let sampler = tick.map(|tick| {
            Mutex::new(Sampler {
                tick,
                first: config.sample_initial,
                thereafter: config.sample_thereafter,
                counts: vec![(Instant::now(), 0); 7 * 4096],
            })
        });
        outputs.push(Output {
            minimum: severity(&config.level).unwrap(),
            config,
            sink,
            sampler,
        });
    }
    let names = outputs.iter().map(|o| o.config.logger.clone()).collect();
    Ok((
        LoggingLayer { outputs, names },
        Guard {
            sinks: sinks.into_values().collect(),
        },
    ))
}

pub fn init(config: &[Config]) -> io::Result<Guard> {
    let (layer, guard) = build(config)?;
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .map_err(io::Error::other)?;
    Ok(guard)
}

impl LoggingLayer {
    fn outputs<'a>(&'a self, target: &str) -> impl Iterator<Item = &'a Output> {
        let exact = self.names.contains(target);
        self.outputs.iter().filter(move |output| {
            output.sink.is_some()
                && if exact {
                    output.config.logger == target
                } else {
                    output.config.logger.is_empty()
                }
        })
    }
}
impl<S: Subscriber> Layer<S> for LoggingLayer {
    fn enabled(&self, meta: &Metadata<'_>, _: Context<'_, S>) -> bool {
        // ERROR events may carry the daemon's terminal FATAL severity.
        self.outputs(meta.target())
            .any(|o| *meta.level() == Level::ERROR || event_severity(meta.level()) >= o.minimum)
    }
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let level = fields
            .level
            .unwrap_or_else(|| event_severity(event.metadata().level()));
        let now = Instant::now();
        for output in self
            .outputs(event.metadata().target())
            .filter(|o| level >= o.minimum)
        {
            if output.sampler.as_ref().is_some_and(|s| {
                !s.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .allow(level, &fields.message, now)
            }) {
                continue;
            }
            let line = encode(&output.config, event.metadata().target(), level, &fields);
            if let Err(error) = output
                .sink
                .as_ref()
                .unwrap()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .write(line.as_bytes())
            {
                report_error(&error.to_string());
            }
        }
    }
}

#[derive(Default)]
struct Fields {
    message: String,
    level: Option<u8>,
    values: Map<String, Value>,
}
impl Fields {
    fn insert(&mut self, key: &str, value: Value) {
        match key {
            "message" => {
                self.message = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string())
            }
            "go_level" => {
                self.level = value
                    .as_str()
                    .and_then(|v| severity(&v.to_ascii_lowercase()).ok())
            }
            // Keep user fields from replacing the structured envelope.
            "level" | "logger" | "timestamp" => {
                self.values.insert(format!("field.{key}"), value);
            }
            _ => {
                self.values.insert(key.into(), value);
            }
        }
    }
}
impl Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.insert(field.name(), json!(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.insert(field.name(), json!(value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.insert(field.name(), json!(value));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.insert(field.name(), json!(value));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.insert(field.name(), json!(value));
    }
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.insert(field.name(), json!(value));
    }
}

fn encode(config: &Config, logger: &str, level: u8, fields: &Fields) -> String {
    let timestamp = timestamp(&config.encoding_time);
    let level = ["DEBUG", "INFO", "WARN", "ERROR", "DPANIC", "PANIC", "FATAL"][usize::from(level)];
    let mut values = Map::new();
    for (key, value) in &fields.values {
        // Tracing has no duration type. Tag nanosecond fields, then remove the tag from output.
        if let Some(key) = key.strip_suffix(".duration_ns")
            && let Some(nanos) = value.as_u64()
        {
            values.insert(
                key.into(),
                match config.encoding_duration.as_str() {
                    "nanos" => json!(nanos),
                    "string" => json!(duration_string(nanos)),
                    _ => json!(nanos as f64 / 1e9),
                },
            );
        } else {
            values.insert(key.clone(), value.clone());
        }
    }
    if config.encoding == "json" {
        values.insert("timestamp".into(), timestamp);
        values.insert("level".into(), json!(level));
        if !logger.is_empty() {
            values.insert("logger".into(), json!(logger));
        }
        values.insert("message".into(), json!(fields.message));
        return format!("{}\n", Value::Object(values));
    }
    let timestamp = timestamp
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| timestamp.to_string());
    // Escape line/control characters in human-readable envelopes too (e.g. client URLs in errors).
    let clean = |s: &str| {
        let mut text = String::with_capacity(s.len());
        for c in s.chars() {
            if c.is_control() {
                text.extend(c.escape_default());
            } else {
                text.push(c);
            }
        }
        text
    };
    if config.encoding == "mixed" {
        let name = if logger.is_empty() {
            String::new()
        } else {
            format!(" [{}]", clean(logger))
        };
        format!(
            "[{timestamp}] {level}{name} {} {}\n",
            clean(&fields.message),
            Value::Object(values)
        )
    } else {
        let name = if logger.is_empty() {
            String::new()
        } else {
            format!("\t{}", clean(logger))
        };
        let context = if values.is_empty() {
            String::new()
        } else {
            format!("\t{}", Value::Object(values))
        };
        format!(
            "{timestamp}\t{level}{name}\t{}{context}\n",
            clean(&fields.message)
        )
    }
}

fn timestamp(encoding: &str) -> Value {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    match encoding {
        "epoch" => json!(since.as_secs_f64()),
        "millis" => json!(since.as_secs_f64() * 1000.0),
        "nanos" => json!(since.as_nanos().min(u64::MAX as u128) as u64),
        _ => {
            let mut text = String::new();
            // Zap's ISO8601 layout uses local time, millisecond precision and a numeric offset.
            static CLOCK: LazyLock<ChronoLocal> =
                LazyLock::new(|| ChronoLocal::new("%Y-%m-%dT%H:%M:%S%.3f%z".into()));
            if CLOCK.format_time(&mut Writer::new(&mut text)).is_err() {
                return json!(since.as_secs_f64());
            }
            if text.ends_with("+0000") {
                text.truncate(text.len() - 5);
                text.push('Z');
            }
            json!(text)
        }
    }
}

fn duration_string(nanos: u64) -> String {
    fn fraction(n: u64, scale: u64, digits: usize) -> String {
        let rest = n % scale;
        if rest == 0 {
            return (n / scale).to_string();
        }
        format!("{}.{:0digits$}", n / scale, rest)
            .trim_end_matches('0')
            .to_owned()
    }
    match nanos {
        0 => "0s".into(),
        1..1000 => format!("{nanos}ns"),
        1000..1_000_000 => format!("{}µs", fraction(nanos, 1000, 3)),
        1_000_000..1_000_000_000 => format!("{}ms", fraction(nanos, 1_000_000, 6)),
        _ => {
            let seconds = nanos / 1_000_000_000;
            let mut text = String::new();
            if seconds >= 3600 {
                text.push_str(&format!("{}h", seconds / 3600));
            }
            if seconds >= 60 {
                text.push_str(&format!("{}m", seconds / 60 % 60));
            }
            text.push_str(&format!(
                "{}s",
                fraction(nanos % 60_000_000_000, 1_000_000_000, 9)
            ));
            text
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(path: &Path, logger: &str, level: &str) -> Config {
        Config {
            file: path.display().to_string(),
            logger: logger.into(),
            level: level.into(),
            encoding: "json".into(),
            ..Config::default()
        }
    }
    fn rows(path: &Path) -> Vec<Value> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn configuration_defaults_legacy_forms_and_validation_have_no_file_side_effects() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        fs::write(&path, "").unwrap();
        let config = crate::config::Config::load(&path).unwrap();
        assert_eq!(config.logging[0].file, "stdout");
        assert_eq!(config.logging[0].encoding, "console");
        fs::write(&path, "[logging]\nlevel='DEBUG'\n").unwrap();
        let config = crate::config::Config::load(&path).unwrap();
        assert_eq!(config.logging[0].file, "stderr");
        assert_eq!(config.logging[0].encoding, "mixed");
        fs::write(
            &path,
            "[[logging]]\nfile='none'\n[common]\nlogfile='stdout'\nlog-level='warn'\n",
        )
        .unwrap();
        let config = crate::config::Config::load(&path).unwrap();
        assert_eq!(config.logging.len(), 1);
        assert_eq!(config.logging[0].level, "warn");
        assert_eq!(config.logging[0].file, "stdout");

        let log = dir.path().join("not-created/log");
        let first = output(&log, "", "info");
        assert!(validate(std::slice::from_ref(&first)).is_ok());
        assert!(!log.parent().unwrap().exists());
        for invalid in [
            "level='bogus'",
            "encoding='yaml'",
            "encoding-time='utc'",
            "encoding-duration='ms'",
            "sample-tick='1s'",
            "sample-tick='0s'\nsample-thereafter=1",
            "sample-initial=-1",
            "file='syslog://remote'",
            "file='file://remote/path'",
            "file='foo%xy'",
            "file='foo%00'",
            "file='stdout?unknown=1'",
            "levle='debug'",
        ] {
            fs::write(&path, format!("[[logging]]\n{invalid}\n")).unwrap();
            assert!(crate::config::Config::load(&path).is_err(), "{invalid}");
        }
        assert!(
            build(&[
                first,
                Config {
                    level: "bad".into(),
                    ..Config::default()
                }
            ])
            .is_err()
        );
        assert!(!log.parent().unwrap().exists());
        let (config, _) = Config { file: "file:///tmp/log%20file?encoding=json&level=debug&encoding-time=nanos&encoding-duration=string".into(), ..Config::default() }.prepare().unwrap();
        assert_eq!(config.file, "/tmp/log file");
        assert_eq!(config.encoding, "json");
        assert_eq!(config.level, "debug");
        assert_eq!(config.encoding_time, "nanos");
        assert_eq!(config.encoding_duration, "string");
    }

    #[test]
    fn named_routes_replace_default_tee_filter_and_disable_without_losing_fields() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("default");
        let errors = dir.path().join("errors");
        let tcp = dir.path().join("tcp");
        let fatal = dir.path().join("fatal");
        let (layer, guard) = build(&[
            output(&default, "", "info"),
            output(&errors, "", "error"),
            output(&tcp, "tcp", "debug"),
            output(&fatal, "terminal", "fatal"),
            Config {
                logger: "access".into(),
                file: "none".into(),
                ..Config::default()
            },
        ])
        .unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::debug!(target: "main", "filtered");
            tracing::info!(target: "main", count = 2u64, ratio = 1.5, ok = true, error = "line\n\"quoted\"", "started");
            tracing::error!(target: "main", "failed");
            tracing::debug!(target: "tcp", "parse failed");
            tracing::error!(target: "access", "disabled");
            tracing::info!(target: "tcp.child", "no prefix inheritance");
            tracing::error!(target: "terminal", "not fatal");
            tracing::error!(target: "terminal", go_level = "FATAL", "fatal error");
        });
        drop(guard);
        let default = rows(&default);
        assert_eq!(default.len(), 3);
        assert_eq!(default[0]["logger"], "main");
        assert_eq!(default[0]["level"], "INFO");
        assert_eq!(default[0]["message"], "started");
        assert_eq!(default[0]["count"], 2);
        assert_eq!(default[0]["ratio"], 1.5);
        assert_eq!(default[0]["ok"], true);
        assert_eq!(default[0]["error"], "line\n\"quoted\"");
        assert_eq!(rows(&errors).len(), 1);
        assert_eq!(rows(&tcp)[0]["level"], "DEBUG");
        assert_eq!(rows(&tcp).len(), 1);
        assert_eq!(rows(&fatal).len(), 1);
        assert_eq!(rows(&fatal)[0]["level"], "FATAL");
        assert!(rows(&fatal)[0].get("go_level").is_none());
    }

    #[test]
    fn encodings_timestamps_and_duration_units_follow_zap_conventions() {
        let iso8601 =
            regex::Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}(Z|[+-]\d{4})$").unwrap();
        let mut fields = Fields {
            message: "fetch served".into(),
            ..Fields::default()
        };
        fields.values.insert(
            "runtime_seconds.duration_ns".into(),
            json!(1_500_000_000u64),
        );
        for (unit, expected) in [
            ("seconds", json!(1.5)),
            ("nanos", json!(1_500_000_000u64)),
            ("string", json!("1.5s")),
        ] {
            let config = Config {
                encoding: "json".into(),
                encoding_duration: unit.into(),
                ..Config::default()
            };
            let row: Value = serde_json::from_str(&encode(&config, "access", 1, &fields)).unwrap();
            assert_eq!(row["runtime_seconds"], expected);
            assert!(row.get("runtime_seconds.duration_ns").is_none());
            let time = row["timestamp"].as_str().unwrap();
            assert!(iso8601.is_match(time), "{time}");
        }
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        for (unit, scale) in [("epoch", 1.0), ("millis", 1000.0), ("nanos", 1e9)] {
            assert!((timestamp(unit).as_f64().unwrap() / scale - seconds).abs() < 1.0);
        }
        let mixed = encode(&Config::default(), "access", 2, &fields);
        assert!(mixed.contains("] WARN [access] fetch served {\"runtime_seconds\":1.5}"));
        let console = encode(&Config::application_default(), "access", 1, &fields);
        assert!(console.contains("\tINFO\taccess\tfetch served\t{\"runtime_seconds\":1.5}"));
        fields.message = "one\ntwo\r\x1b".into();
        assert_eq!(
            encode(&Config::default(), "access\n", 1, &fields)
                .lines()
                .count(),
            1
        );
        assert_eq!(
            encode(
                &Config::application_default(),
                "access",
                1,
                &Fields {
                    message: "empty".into(),
                    ..Fields::default()
                }
            )
            .split('\t')
            .count(),
            4
        );
        for (nanos, expected) in [
            (0, "0s"),
            (1, "1ns"),
            (1500, "1.5µs"),
            (1_234_000, "1.234ms"),
            (60_000_000_000, "1m0s"),
            (3_600_001_000_000, "1h0m0.001s"),
        ] {
            assert_eq!(duration_string(nanos), expected);
        }
    }

    #[test]
    fn sampling_is_bounded_per_message_and_level_and_resets() {
        let now = Instant::now();
        let mut sampler = Sampler {
            tick: Duration::from_secs(1),
            first: 2,
            thereafter: 3,
            counts: vec![(now, 0); 7 * 4096],
        };
        let allowed: Vec<_> = (1..=8).filter(|_| sampler.allow(1, "same", now)).collect();
        assert_eq!(allowed, vec![1, 2, 5, 8]);
        assert!(sampler.allow(2, "same", now));
        assert!(sampler.allow(1, "different", now));
        assert!(sampler.allow(1, "same", now + Duration::from_secs(1)));
        assert_eq!(sampler.counts.len(), 7 * 4096);
        for (text, duration) in [
            ("1s", Duration::from_secs(1)),
            ("1m2.5s", Duration::from_millis(62_500)),
            ("+1us", Duration::from_micros(1)),
            (".5ms", Duration::from_micros(500)),
        ] {
            assert_eq!(sample_tick(text), Some(duration));
        }
        for text in [
            "",
            "0",
            "0s",
            "-1s",
            "NaNs",
            "1sBAD",
            "1d",
            "999999999999999999h",
        ] {
            assert_eq!(sample_tick(text), None);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let config = Config {
            sample_tick: "1h".into(),
            sample_initial: 2,
            sample_thereafter: 3,
            ..output(&path, "", "info")
        };
        let (layer, guard) = build(&[config]).unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            for n in 1..=8 {
                tracing::info!(target: "main", n, "same");
            }
        });
        drop(guard);
        assert_eq!(
            rows(&path)
                .iter()
                .map(|row| row["n"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 5, 8]
        );
    }

    #[test]
    fn shared_file_writes_are_atomic_and_external_rotation_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs/carbon.log");
        let rotated = dir.path().join("rotated");
        let (layer, guard) =
            build(&[output(&path, "", "info"), output(&path, "tcp", "debug")]).unwrap();
        assert_eq!(guard.sinks.len(), 1);
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        for n in 0..100 {
                            tracing::info!(target: "tcp", n, "concurrent");
                        }
                    })
                });
            }
        });
        assert_eq!(rows(&path).len(), 400);
        fs::rename(&path, &rotated).unwrap();
        if let Sink::File { checked, .. } = &mut *guard.sinks[0].lock().unwrap() {
            *checked = Instant::now() - Duration::from_secs(2);
        }
        tracing::dispatcher::with_default(
            &dispatch,
            || tracing::info!(target: "main", "after rotation"),
        );
        assert_eq!(rows(&path)[0]["message"], "after rotation");
        assert_eq!(rows(&rotated).len(), 400);
        fs::remove_file(&path).unwrap();
        if let Sink::File { checked, .. } = &mut *guard.sinks[0].lock().unwrap() {
            *checked = Instant::now() - Duration::from_secs(2);
        }
        tracing::dispatcher::with_default(
            &dispatch,
            || tracing::info!(target: "main", "after deletion"),
        );
        drop(guard);
        assert_eq!(rows(&path)[0]["message"], "after deletion");
    }
}
