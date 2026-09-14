use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use whisper_rs::{Aggregation, Metadata, Retention};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
#[derive(Default)]
pub struct Config {
    pub common: Common,
    pub whisper: Whisper,
    pub cache: Cache,
    pub tcp: Receiver,
    pub udp: Receiver,
    pub carbonserver: Carbonserver,
    pub dump: Dump,
    pub prometheus: Prometheus,
    pub pprof: Pprof,
    #[serde(deserialize_with = "crate::logging::deserialize")]
    pub logging: Vec<crate::logging::Config>,
    pub pickle: Disabled,
    pub carbonlink: Disabled,
    pub grpc: Disabled,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Common {
    pub graph_prefix: String,
    pub metric_endpoint: String,
    #[serde(default = "sixty_seconds", deserialize_with = "duration")]
    pub metric_interval: std::time::Duration,
    pub max_cpu: usize,
    pub log_level: Option<String>,
    pub logfile: Option<String>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Whisper {
    pub data_dir: String,
    pub schemas_file: String,
    pub aggregation_file: String,
    pub quotas_file: String,
    pub workers: usize,
    pub sparse_create: bool,
    pub flock: bool,
    pub compressed: bool,
    pub out_of_order: bool,
    pub out_of_order_compact_rate: u64,
    pub out_of_order_compact_threshold: u64,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Cache {
    pub max_size: u64,
    pub max_bytes: u64,
    #[serde(default = "noop")]
    pub write_strategy: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Receiver {
    pub enabled: bool,
    #[serde(deserialize_with = "listen_address")]
    pub listen: String,
    pub max_line_bytes: usize,
    pub max_connections: usize,
    #[serde(default = "two_minutes", deserialize_with = "duration")]
    pub read_timeout: std::time::Duration,
    #[serde(default = "thirty_seconds", deserialize_with = "duration")]
    pub shutdown_timeout: std::time::Duration,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Carbonserver {
    pub enabled: bool,
    #[serde(deserialize_with = "listen_address")]
    pub listen: String,
    pub trie_index: bool,
    pub trigram_index: bool,
    pub realtime_index: usize,
    #[serde(default = "sixty_seconds", deserialize_with = "duration")]
    pub request_timeout: std::time::Duration,
    #[serde(default = "five_minutes", deserialize_with = "duration")]
    pub scan_frequency: std::time::Duration,
    #[serde(default = "sixty_seconds", deserialize_with = "duration")]
    pub quota_usage_report_frequency: std::time::Duration,
    pub file_list_cache: String,
    pub file_list_cache_version: u8,
    pub concurrent_requests: usize,
    pub max_creates_per_second: u64,
    pub query_cache_size: usize,
    pub query_cache_size_mb: usize,
    pub query_cache_enabled: bool,
    pub find_cache_enabled: bool,
    pub max_globs: usize,
    pub max_metrics_globbed: usize,
    pub max_metrics_rendered: usize,
    pub grpc: Disabled,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Dump {
    pub enabled: bool,
    pub path: String,
    pub restore_per_second: usize,
}
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Disabled {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Prometheus {
    pub enabled: bool,
    pub endpoint: String,
    pub labels: HashMap<String, String>,
}
impl Default for Prometheus {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "/metrics".into(),
            labels: HashMap::new(),
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Pprof {
    pub enabled: bool,
    #[serde(deserialize_with = "listen_address")]
    pub listen: String,
}
impl Default for Pprof {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: "127.0.0.1:7007".into(),
        }
    }
}

impl Prometheus {
    pub fn validate(&self, carbonserver: bool) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if !self.endpoint.starts_with('/')
            || self.endpoint.split('/').any(|part| part.starts_with(':'))
            || self.endpoint.parse::<axum::http::Uri>().is_err()
            || self.endpoint.bytes().any(|b| {
                b.is_ascii_control()
                    || b.is_ascii_whitespace()
                    || matches!(b, b'{' | b'}' | b'*' | b'?' | b'#')
            })
        {
            return Err("prometheus.endpoint must be a literal absolute HTTP path".into());
        }
        for name in self.labels.keys() {
            let valid = !name.is_empty()
                && !name.starts_with("__")
                && name.bytes().enumerate().all(|(i, b)| {
                    b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
                });
            if !valid
                || matches!(name.as_str(), "le" | "version")
                || (carbonserver && matches!(name.as_str(), "code" | "handler" | "type" | "hit"))
            {
                return Err(format!("invalid or conflicting prometheus label: {name}"));
            }
        }
        Ok(())
    }
}

impl Default for Common {
    fn default() -> Self {
        Self {
            graph_prefix: "carbon.agents.{host}".into(),
            metric_endpoint: "local".into(),
            metric_interval: sixty_seconds(),
            max_cpu: 1,
            log_level: None,
            logfile: None,
        }
    }
}
impl Default for Whisper {
    fn default() -> Self {
        Self {
            data_dir: "/var/lib/graphite/whisper".into(),
            schemas_file: "/etc/go-carbon/storage-schemas.conf".into(),
            aggregation_file: String::new(),
            quotas_file: String::new(),
            workers: 1,
            sparse_create: false,
            flock: false,
            compressed: false,
            out_of_order: false,
            out_of_order_compact_rate: 5,
            out_of_order_compact_threshold: 65_536,
        }
    }
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            max_size: 1_000_000,
            max_bytes: 0,
            write_strategy: "noop".into(),
        }
    }
}
impl Default for Receiver {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: String::new(),
            max_line_bytes: 65_536,
            max_connections: 32_768,
            read_timeout: std::time::Duration::from_secs(120),
            shutdown_timeout: std::time::Duration::from_secs(30),
        }
    }
}
impl Default for Carbonserver {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: "127.0.0.1:8080".into(),
            trie_index: true,
            trigram_index: true,
            realtime_index: 65_536,
            request_timeout: sixty_seconds(),
            scan_frequency: five_minutes(),
            quota_usage_report_frequency: sixty_seconds(),
            file_list_cache: String::new(),
            file_list_cache_version: 1,
            concurrent_requests: 0,
            max_creates_per_second: 0,
            query_cache_size: 0,
            query_cache_size_mb: 0,
            query_cache_enabled: true,
            find_cache_enabled: true,
            max_globs: 100,
            max_metrics_globbed: 10_000_000,
            max_metrics_rendered: 1_000_000,
            grpc: Disabled::default(),
        }
    }
}
impl Default for Dump {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/var/lib/graphite/dump".into(),
            restore_per_second: 0,
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let mut config: Self =
            toml::from_str(&fs::read_to_string(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        if config.carbonserver.query_cache_size > 0 && config.carbonserver.query_cache_size_mb > 0 {
            return Err(
                "set only one of carbonserver.query-cache-size and query-cache-size-mb".into(),
            );
        }
        if config.dump.restore_per_second != 0 {
            return Err("dump.restore-per-second is not implemented".into());
        }
        if config.carbonserver.query_cache_size_mb > 0 {
            config.carbonserver.query_cache_size = config
                .carbonserver
                .query_cache_size_mb
                .checked_mul(1024 * 1024)
                .ok_or_else(|| "carbonserver.query-cache-size-mb is too large".to_string())?;
        }
        if config.cache.write_strategy != "noop" {
            return Err("cache.write-strategy supports only noop".into());
        }
        for (name, section) in [
            ("pickle", &config.pickle),
            ("carbonlink", &config.carbonlink),
            ("grpc", &config.grpc),
            ("carbonserver.grpc", &config.carbonserver.grpc),
        ] {
            if section.enabled {
                return Err(format!("{name} is not supported"));
            }
        }
        config.validate_diagnostics()?;
        crate::graphite::validate(&config.common)?;
        // A zero tokio timeout fires before the first poll: every read/request would fail.
        for (name, value) in [
            ("tcp.read-timeout", config.tcp.read_timeout),
            (
                "carbonserver.request-timeout",
                config.carbonserver.request_timeout,
            ),
        ] {
            if value.is_zero() {
                return Err(format!("{name} must be positive"));
            }
        }
        if config.common.log_level.is_some() || config.common.logfile.is_some() {
            let mut logging = crate::logging::Config::application_default();
            if let Some(level) = &config.common.log_level {
                logging.level = level.clone();
            }
            if let Some(file) = &config.common.logfile {
                logging.file = file.clone();
            }
            config.logging = vec![logging];
        }
        if config.logging.is_empty() {
            config
                .logging
                .push(crate::logging::Config::application_default());
        }
        crate::logging::validate(&config.logging)?;
        Ok(config)
    }

    pub fn validate_diagnostics(&self) -> Result<(), String> {
        self.prometheus.validate(self.carbonserver.enabled)?;
        if self.pprof.enabled && !crate::profiling::SUPPORTED {
            return Err("CPU profiling requires 64-bit Linux".into());
        }
        if self.pprof.enabled
            && self.prometheus.enabled
            && (self.prometheus.endpoint == "/debug/pprof"
                || self.prometheus.endpoint.starts_with("/debug/pprof/"))
        {
            return Err("prometheus.endpoint conflicts with the enabled pprof routes".into());
        }
        Ok(())
    }
}
fn listen_address<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let mut address = String::deserialize(d)?;
    // Treat Go's :port shorthand as an IPv4 wildcard, not an empty DNS hostname.
    if address
        .strip_prefix(':')
        .is_some_and(|port| !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()))
    {
        address.insert_str(0, "0.0.0.0");
    }
    Ok(address)
}

fn duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<std::time::Duration, D::Error> {
    let v = String::deserialize(d)?;
    parse_duration(&v).ok_or_else(|| serde::de::Error::custom("invalid duration"))
}
fn noop() -> String {
    "noop".into()
}
fn two_minutes() -> std::time::Duration {
    std::time::Duration::from_secs(120)
}
fn sixty_seconds() -> std::time::Duration {
    std::time::Duration::from_secs(60)
}
fn five_minutes() -> std::time::Duration {
    std::time::Duration::from_secs(300)
}
fn thirty_seconds() -> std::time::Duration {
    std::time::Duration::from_secs(30)
}
pub(crate) fn parse_duration(value: &str) -> Option<std::time::Duration> {
    use std::{sync::LazyLock, time::Duration};
    static PARTS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)").unwrap());
    let value = value.strip_prefix('+').unwrap_or(value);
    if value == "0" {
        return Some(Duration::ZERO);
    }
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
    if value.is_empty() || end != value.len() || seconds > i64::MAX as f64 / 1e9 {
        return None;
    }
    Duration::try_from_secs_f64(seconds).ok()
}

pub struct Rules {
    schemas: Vec<Schema>,
    aggregations: Vec<AggregationRule>,
}
struct Schema {
    regex: Regex,
    metadata: Metadata,
}
struct AggregationRule {
    regex: Regex,
    aggregation: Aggregation,
    xff: f32,
}
impl Rules {
    pub fn load(
        schemas: impl AsRef<Path>,
        aggregation: impl AsRef<Path>,
        defaults: &Whisper,
    ) -> Result<Self, String> {
        let schemas = parse_ini(&fs::read_to_string(schemas).map_err(|e| e.to_string())?)?
            .into_iter()
            .map(|(_, v)| {
                let regex = Regex::new(required(&v, "pattern")?).map_err(|e| e.to_string())?;
                let compressed = v
                    .get("compressed")
                    .map(|x| x.parse())
                    .transpose()
                    .map_err(|_| "invalid compressed".to_string())?
                    .unwrap_or(defaults.compressed);
                let mut metadata = Metadata {
                    aggregation: Aggregation::Average,
                    x_files_factor: 0.5,
                    retentions: retentions(required(&v, "retentions")?)?,
                    compressed,
                };
                metadata.validate().map_err(|e| e.to_string())?;
                Ok(Schema { regex, metadata })
            })
            .collect::<Result<_, String>>()?;
        let aggregations = if aggregation.as_ref().as_os_str().is_empty() {
            Vec::new()
        } else {
            parse_ini(&fs::read_to_string(aggregation).map_err(|e| e.to_string())?)?
                .into_iter()
                .map(|(_, v)| {
                    let xff: f32 = required(&v, "xfilesfactor")?
                        .parse()
                        .map_err(|_| "invalid xfilesfactor")?;
                    if !xff.is_finite() || !(0.0..=1.0).contains(&xff) {
                        return Err("invalid xfilesfactor".to_string());
                    }
                    Ok(AggregationRule {
                        regex: Regex::new(required(&v, "pattern")?).map_err(|e| e.to_string())?,
                        aggregation: aggregation_method(required(&v, "aggregationmethod")?)?,
                        xff,
                    })
                })
                .collect::<Result<_, String>>()?
        };
        Ok(Self {
            schemas,
            aggregations,
        })
    }
    pub fn metadata(&self, metric: &str) -> Option<Metadata> {
        let mut m = self
            .schemas
            .iter()
            .find(|s| s.regex.is_match(metric))?
            .metadata
            .clone();
        if let Some(a) = self.aggregations.iter().find(|a| a.regex.is_match(metric)) {
            m.aggregation = a.aggregation;
            m.x_files_factor = a.xff;
        }
        if !m.x_files_factor.is_finite() || !(0.0..=1.0).contains(&m.x_files_factor) {
            return None;
        }
        Some(m)
    }
}
fn aggregation_method(s: &str) -> Result<Aggregation, String> {
    match s {
        "average" | "avg" => Ok(Aggregation::Average),
        "sum" => Ok(Aggregation::Sum),
        "last" => Ok(Aggregation::Last),
        "max" => Ok(Aggregation::Max),
        "min" => Ok(Aggregation::Min),
        "first" => Ok(Aggregation::First),
        _ => Err(format!("unknown aggregation method {s}")),
    }
}
fn retentions(s: &str) -> Result<Vec<Retention>, String> {
    let values = s.split(',').collect::<Vec<_>>();
    if values.is_empty() || values.len() > 1_000_000 {
        return Err("bad retention count".into());
    }
    values
        .into_iter()
        .map(|x| {
            let (step, count) = x
                .trim()
                .split_once(':')
                .ok_or_else(|| "bad retention".to_string())?;
            let step = units(step)?;
            let count = if count.bytes().all(|c| c.is_ascii_digit()) {
                count.parse().map_err(|_| "bad retention".to_string())?
            } else {
                units(count)?
                    .checked_div(step)
                    .ok_or_else(|| "bad retention".to_string())?
            };
            if step == 0 || count == 0 {
                return Err("bad retention".into());
            }
            Ok(Retention {
                seconds_per_point: step,
                points: count,
            })
        })
        .collect()
}
fn units(s: &str) -> Result<u32, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: u32 = n.parse().map_err(|_| "bad retention".to_string())?;
    n.checked_mul(match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        "w" => 604_800,
        "y" => 31_536_000,
        _ => return Err("bad retention".into()),
    })
    .ok_or_else(|| "bad retention".into())
}
type IniSections = Vec<(String, std::collections::HashMap<String, String>)>;
fn parse_ini(input: &str) -> Result<IniSections, String> {
    let mut out = Vec::new();
    let mut current = None;
    for line in input.lines().enumerate() {
        let text = line.1.trim();
        if text.is_empty() || text.starts_with('#') || text.starts_with(';') {
            continue;
        }
        if text.starts_with('[') && text.ends_with(']') {
            if let Some(v) = current.take() {
                out.push(v)
            };
            current = Some((
                text[1..text.len() - 1].trim().to_owned(),
                std::collections::HashMap::new(),
            ));
        } else {
            let (key, value) = text
                .split_once('=')
                .ok_or_else(|| format!("line {}: expected key = value", line.0 + 1))?;
            let Some((_, values)) = current.as_mut() else {
                return Err(format!("line {}: config section not found", line.0 + 1));
            };
            values.insert(
                key.trim().to_ascii_lowercase(),
                value.trim().trim_matches(['\"', '\'']).to_owned(),
            );
        }
    }
    if let Some(v) = current {
        out.push(v)
    };
    Ok(out)
}
fn required<'a>(
    v: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str, String> {
    v.get(key)
        .filter(|v| !v.is_empty())
        .map(String::as_str)
        .ok_or_else(|| format!("missing {key}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_rate_config_defaults_to_unlimited_and_rejects_invalid_limits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        fs::write(&path, "").unwrap();
        assert_eq!(
            Config::load(&path)
                .unwrap()
                .carbonserver
                .max_creates_per_second,
            0
        );
        for value in ["0", "500", "-1", "1.5", "'500'"] {
            fs::write(
                &path,
                format!("[carbonserver]\nmax-creates-per-second = {value}\n"),
            )
            .unwrap();
            let result = Config::load(&path);
            if let Ok(expected) = value.parse::<u64>() {
                assert_eq!(
                    result.unwrap().carbonserver.max_creates_per_second,
                    expected
                );
            } else {
                assert!(result.is_err(), "{value}");
            }
        }
    }

    #[test]
    fn go_style_listeners_normalize_only_port_only_addresses() {
        for (input, expected) in [
            (":2003", "0.0.0.0:2003"),
            (":8080", "0.0.0.0:8080"),
            (":7007", "0.0.0.0:7007"),
            (":0", "0.0.0.0:0"),
            (":65535", "0.0.0.0:65535"),
            ("127.0.0.1:2003", "127.0.0.1:2003"),
            ("0.0.0.0:2003", "0.0.0.0:2003"),
            ("localhost:2003", "localhost:2003"),
            ("[::]:2003", "[::]:2003"),
            ("[::1]:2003", "[::1]:2003"),
            ("::1", "::1"),
            (":", ":"),
            (":bad", ":bad"),
            ("", ""),
        ] {
            let mut text = "[common]\nmetric-endpoint='tcp://localhost:3002'\n".to_owned();
            for section in ["tcp", "udp", "carbonserver", "pprof"] {
                text.push_str(&format!("[{section}]\nlisten='{input}'\n"));
            }
            let config: Config = toml::from_str(&text).unwrap();
            for listen in [
                &config.tcp.listen,
                &config.udp.listen,
                &config.carbonserver.listen,
                &config.pprof.listen,
            ] {
                assert_eq!(listen, expected, "input: {input}");
            }
            assert_eq!(config.common.metric_endpoint, "tcp://localhost:3002");
        }
    }

    #[test]
    fn pprof_defaults_and_conflicting_diagnostics_paths() {
        let mut config = Config::default();
        assert!(!config.pprof.enabled);
        assert_eq!(config.pprof.listen, "127.0.0.1:7007");
        config.prometheus.enabled = true;
        for endpoint in ["/debug/pprof", "/debug/pprof/", "/debug/pprof/profile"] {
            config.prometheus.endpoint = endpoint.into();
            config.pprof.enabled = false;
            assert!(config.validate_diagnostics().is_ok());
            config.pprof.enabled = true;
            assert!(config.validate_diagnostics().is_err());
        }
        let config: Config =
            toml::from_str("[pprof]\nenabled=true\nlisten='127.0.0.1:7100'\n").unwrap();
        assert!(config.pprof.enabled);
        assert_eq!(config.pprof.listen, "127.0.0.1:7100");
        assert_eq!(
            config.validate_diagnostics().is_ok(),
            crate::profiling::SUPPORTED
        );
    }
    #[test]
    fn rejects_invalid_storage_settings_before_admission() {
        let dir = tempfile::tempdir().unwrap();
        let schemas = dir.path().join("schemas");
        let aggregation = dir.path().join("aggregation");
        for retention in ["2:60,7:600", "1:60,10:5", "2:4294967295"] {
            fs::write(
                &schemas,
                format!("[all]\npattern = .*\nretentions = {retention}\n"),
            )
            .unwrap();
            assert!(Rules::load(&schemas, "", &Whisper::default()).is_err());
        }
        fs::write(&schemas, "[all]\npattern = .*\nretentions = 10:600,1:60\n").unwrap();
        let rules = Rules::load(&schemas, "", &Whisper::default()).unwrap();
        assert_eq!(
            rules.metadata("a").unwrap().retentions[0].seconds_per_point,
            1
        );
        for xff in ["NaN", "-1", "2"] {
            fs::write(
                &aggregation,
                format!("[all]\npattern = .*\naggregationMethod = average\nxFilesFactor = {xff}\n"),
            )
            .unwrap();
            assert!(Rules::load(&schemas, &aggregation, &Whisper::default()).is_err());
        }
    }
    #[test]
    fn parses_rules_and_rejects_excluded_features() {
        let dir = tempfile::tempdir().unwrap();
        let schemas = dir.path().join("s");
        let aggr = dir.path().join("a");
        fs::write(&schemas, "[x]\npattern = ^a\\.\nretentions = 60:10\n").unwrap();
        fs::write(
            &aggr,
            "[x]\npattern = ^a\\.\nxFilesFactor = 0.7\naggregationMethod = sum\n",
        )
        .unwrap();
        let rules = Rules::load(&schemas, &aggr, &Whisper::default()).unwrap();
        assert_eq!(rules.metadata("a.b").unwrap().aggregation, Aggregation::Sum);
        let conf = dir.path().join("c");
        fs::write(&conf, "[cache]\nwrite-strategy='max'\n").unwrap();
        assert!(Config::load(conf).is_err());
    }
    #[test]
    fn rejects_zero_timeouts() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join("c");
        for toml in [
            "[tcp]\nread-timeout = \"0s\"\n",
            "[carbonserver]\nrequest-timeout = \"0\"\n",
        ] {
            fs::write(&conf, toml).unwrap();
            let error = Config::load(&conf).unwrap_err();
            assert!(error.ends_with("must be positive"), "{toml}: {error}");
        }
        fs::write(&conf, "[carbonserver]\nscan-frequency = \"0\"\n").unwrap();
        assert!(
            Config::load(&conf)
                .unwrap()
                .carbonserver
                .scan_frequency
                .is_zero()
        );
    }
    #[test]
    fn converts_existing_query_cache_megabytes() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join("c");
        fs::write(&conf, "[carbonserver]\nquery-cache-size-mb = 2\n").unwrap();
        assert_eq!(
            Config::load(conf).unwrap().carbonserver.query_cache_size,
            2 * 1024 * 1024
        );
    }
}
