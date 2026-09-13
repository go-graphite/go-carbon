use regex::Regex;
use serde::Deserialize;
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
    pub pickle: Disabled,
    pub carbonlink: Disabled,
    pub grpc: Disabled,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Common {
    pub graph_prefix: String,
    pub max_cpu: usize,
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

impl Default for Common {
    fn default() -> Self {
        Self {
            graph_prefix: "carbon.agents.{host}".into(),
            max_cpu: 1,
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
        Ok(config)
    }
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
fn parse_duration(v: &str) -> Option<std::time::Duration> {
    let (n, u) = v
        .trim()
        .split_at(v.trim().find(|c: char| !c.is_ascii_digit())?);
    let n = n.parse::<u64>().ok()?;
    Some(std::time::Duration::from_secs(n.checked_mul(match u {
        "ms" => return Some(std::time::Duration::from_millis(n)),
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        _ => return None,
    })?))
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
