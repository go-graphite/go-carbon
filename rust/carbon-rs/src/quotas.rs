use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::index::Glob;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Costs {
    pub metrics: i64,
    pub namespaces: i64,
    pub data_points: i64,
    pub logical_size: i64,
    pub physical_size: i64,
}
impl Costs {
    pub fn metric(data_points: i64, logical_size: i64, physical_size: i64) -> Self {
        Self {
            metrics: 1,
            data_points,
            logical_size,
            physical_size,
            namespaces: 0,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DroppingPolicy {
    New,
    None,
}
#[derive(Debug, Clone, Serialize)]
pub struct Rule {
    pub pattern: String,
    pub namespaces: i64,
    pub metrics: i64,
    pub data_points: i64,
    pub logical_size: i64,
    pub physical_size: i64,
    pub throughput: i64,
    pub dropping_policy: DroppingPolicy,
    pub stat_metric_prefix: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    Throughput,
    Namespaces,
    Metrics,
    DataPoints,
    LogicalSize,
    PhysicalSize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reservation {
    id: u64,
    pub new_metric: bool,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Usage {
    pub namespaces: i64,
    pub metrics: i64,
    pub data_points: i64,
    pub logical_size: i64,
    pub physical_size: i64,
    pub throughput: i64,
    pub throttled: i64,
}
#[derive(Debug, Clone, Serialize)]
pub struct NamespaceReport {
    pub namespace: String,
    pub rule: Option<Rule>,
    pub usage: Usage,
}
struct Compiled {
    rule: Rule,
    glob: Option<Glob>,
}
struct Metric {
    costs: Costs,
    handles: Vec<String>,
}
struct Pending {
    metric: String,
    costs: Costs,
    handles: Vec<String>,
    namespaces: Vec<(String, String)>, // (namespace, parent) charged by this reservation
}
struct State {
    metrics: HashMap<String, Metric>,
    namespaces: HashSet<String>,
    usage: HashMap<String, Usage>,
    pending: HashMap<u64, Pending>,
    window_at: Instant,
}

/// Quota state is one mutex because admissions must reserve every parent atomically.
/// ponytail: global admission lock; shard by root namespace if profiling shows contention.
pub struct Engine {
    rules: Vec<Compiled>,
    window: Duration,
    state: Mutex<State>,
    next: Mutex<u64>,
}
pub type QuotaEngine = Engine;

impl Engine {
    pub fn load(path: impl AsRef<Path>, window: Duration) -> Result<Self, String> {
        let rules = parse_ini(&fs::read_to_string(path).map_err(|e| e.to_string())?)?
            .into_iter()
            .map(|(pattern, values)| {
                let glob = if pattern == "/" {
                    None
                } else {
                    Some(Glob::new(pattern.as_str()).map_err(|e| e.to_string())?)
                };
                Ok(Compiled {
                    rule: Rule {
                        pattern,
                        namespaces: number(&values, "namespaces")?,
                        metrics: number(&values, "metrics")?,
                        data_points: number(&values, "data-points")?,
                        logical_size: number(&values, "logical-size")?,
                        physical_size: number(&values, "physical-size")?,
                        throughput: throughput(number(&values, "throughput")?, window),
                        dropping_policy: match values
                            .get("dropping-policy")
                            .map(String::as_str)
                            .unwrap_or("new")
                        {
                            "" | "new" => DroppingPolicy::New,
                            "none" => DroppingPolicy::None,
                            v => return Err(format!("unknown dropping-policy {v}")),
                        },
                        stat_metric_prefix: values
                            .get("stat-metric-prefix")
                            .map(|x| x.trim_start_matches('.').to_owned())
                            .unwrap_or_default(),
                    },
                    glob,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            rules,
            window,
            state: Mutex::new(State {
                metrics: HashMap::new(),
                namespaces: HashSet::new(),
                usage: HashMap::new(),
                pending: HashMap::new(),
                window_at: Instant::now(),
            }),
            next: Mutex::new(1),
        })
    }

    /// Checks throughput for every metric; resource limits only for a new metric.
    /// An accepted new metric remains reserved until `commit` or `release`.
    pub fn admit(&self, metric: &str, points: i64, costs: Costs) -> Result<Reservation, Rejection> {
        let mut state = self.state.lock().unwrap();
        self.reset_if_due(&mut state);
        let handles = self.handles(metric);
        for handle in &handles {
            if let Some(rule) = self.rule_for(handle) {
                let usage = state.usage.entry(handle.clone()).or_default();
                if rule.throughput > 0 && usage.throughput.saturating_add(points) > rule.throughput
                {
                    usage.throttled = usage.throttled.saturating_add(points);
                    if rule.dropping_policy == DroppingPolicy::New {
                        return Err(Rejection::Throughput);
                    }
                }
            }
        }
        for handle in &handles {
            let usage = state.usage.entry(handle.clone()).or_default();
            usage.throughput = usage.throughput.saturating_add(points);
        }
        if state.metrics.contains_key(metric)
            || state
                .pending
                .values()
                .any(|pending| pending.metric == metric)
        {
            return Ok(Reservation {
                id: 0,
                new_metric: false,
            });
        }
        let mut namespace_adds = Vec::new();
        let path = metric.rsplit_once('.').map(|x| x.0).unwrap_or("");
        let parts: Vec<_> = if path.is_empty() {
            Vec::new()
        } else {
            path.split('.').collect()
        };
        for i in 1..=parts.len() {
            let ns = parts[..i].join(".");
            if state.namespaces.contains(&ns) || pending_namespace_exists(&state, &ns) {
                continue;
            }
            let parent = if i == 1 {
                "/".to_owned()
            } else {
                parts[..i - 1].join(".")
            };
            namespace_adds.push((ns, parent));
        }
        for handle in &handles {
            let add = Costs {
                namespaces: namespace_adds
                    .iter()
                    .filter(|(_, parent)| parent == handle)
                    .count() as i64,
                ..costs
            };
            let usage = *state.usage.entry(handle.clone()).or_default();
            if let Some((which, policy)) = self.exceeded(handle, usage, add) {
                state.usage.get_mut(handle).unwrap().throttled =
                    state.usage[handle].throttled.saturating_add(points);
                if policy == DroppingPolicy::New {
                    return Err(which);
                }
            }
        }
        for handle in &handles {
            let add = Costs {
                namespaces: namespace_adds
                    .iter()
                    .filter(|(_, parent)| parent == handle)
                    .count() as i64,
                ..costs
            };
            add_usage(state.usage.entry(handle.clone()).or_default(), add);
        }
        let mut next = self.next.lock().unwrap();
        let id = *next;
        *next += 1;
        state.pending.insert(
            id,
            Pending {
                metric: metric.to_owned(),
                costs,
                handles,
                namespaces: namespace_adds,
            },
        );
        Ok(Reservation {
            id,
            new_metric: true,
        })
    }
    pub fn commit(&self, reservation: Reservation) -> bool {
        if reservation.id == 0 {
            return true;
        }
        let mut state = self.state.lock().unwrap();
        let Some(p) = state.pending.remove(&reservation.id) else {
            return false;
        };
        let parent_path = p.metric.rsplit_once('.').map(|x| x.0).unwrap_or("");
        for i in 1..=parent_path.split('.').filter(|x| !x.is_empty()).count() {
            state
                .namespaces
                .insert(parent_path.split('.').take(i).collect::<Vec<_>>().join("."));
        }
        state.metrics.insert(
            p.metric,
            Metric {
                costs: p.costs,
                handles: p.handles,
            },
        );
        true
    }
    pub fn release(&self, reservation: Reservation) -> bool {
        if reservation.id == 0 {
            return true;
        }
        let mut state = self.state.lock().unwrap();
        let Some(p) = state.pending.remove(&reservation.id) else {
            return false;
        };
        // A pending sibling under the same namespace inherits its charge instead of a refund.
        let mut refund = Vec::new();
        for (ns, parent) in p.namespaces {
            match state
                .pending
                .values_mut()
                .find(|q| in_namespace(&q.metric, &ns))
            {
                Some(heir) => heir.namespaces.push((ns, parent)),
                None => refund.push((ns, parent)),
            }
        }
        for handle in p.handles {
            let namespaces = refund
                .iter()
                .filter(|(_, parent)| parent == &handle)
                .count() as i64;
            subtract_usage(
                state.usage.entry(handle).or_default(),
                Costs {
                    namespaces,
                    ..p.costs
                },
            );
        }
        true
    }
    /// Reconciles a known file after create, compaction, or a filesystem scan; `.ooo` bytes belong in `costs`.
    pub fn sync_metric(&self, metric: &str, costs: Costs) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some((old, handles)) = state.metrics.get_mut(metric).map(|current| {
            let old = current.costs;
            current.costs = costs;
            (old, current.handles.clone())
        }) else {
            return false;
        };
        for handle in handles {
            let u = state.usage.entry(handle).or_default();
            add_usage(u, difference(costs, old));
        }
        true
    }
    pub fn usage(&self, namespace: &str) -> Usage {
        self.state
            .lock()
            .unwrap()
            .usage
            .get(namespace)
            .copied()
            .unwrap_or_default()
    }
    pub fn report(&self) -> Vec<NamespaceReport> {
        let state = self.state.lock().unwrap();
        let mut report = BTreeMap::new();
        for (namespace, usage) in &state.usage {
            report.insert(
                namespace.clone(),
                NamespaceReport {
                    namespace: namespace.clone(),
                    rule: self.rule_for(namespace).cloned(),
                    usage: *usage,
                },
            );
        }
        report.into_values().collect()
    }
    pub fn snapshot(&self) -> Vec<NamespaceReport> {
        self.report()
    }
    /// Inserts files discovered at startup without charging throughput or enforcing limits.
    pub fn register_existing(&self, metric: &str, costs: Costs) {
        let mut state = self.state.lock().unwrap();
        self.register_locked(&mut state, metric, costs);
    }
    fn register_locked(&self, state: &mut State, metric: &str, costs: Costs) {
        if let Some(current) = state.metrics.get_mut(metric) {
            let delta = difference(costs, current.costs);
            current.costs = costs;
            for handle in &current.handles {
                add_usage(state.usage.entry(handle.clone()).or_default(), delta);
            }
            return;
        }
        let handles = self.handles(metric);
        for handle in &handles {
            add_usage(state.usage.entry(handle.clone()).or_default(), costs);
        }
        let parent = metric.rsplit_once('.').map(|x| x.0).unwrap_or("");
        let parts: Vec<_> = parent.split('.').filter(|x| !x.is_empty()).collect();
        for i in 1..=parts.len() {
            let namespace = parts[..i].join(".");
            if state.namespaces.insert(namespace) {
                let parent = if i == 1 {
                    "/".to_owned()
                } else {
                    parts[..i - 1].join(".")
                };
                state.usage.entry(parent).or_default().namespaces += 1;
            }
        }
        state
            .metrics
            .insert(metric.to_owned(), Metric { costs, handles });
    }
    /// Rebuilds catalog resource usage from a completed filesystem scan, preserving throughput counters.
    pub fn reconcile(&self, metrics: Vec<(String, Costs)>) {
        let mut state = self.state.lock().unwrap();
        // The daemon serializes scans with admissions. Standalone callers may
        // defer reconciliation until outstanding reservations have resolved.
        if !state.pending.is_empty() {
            return;
        }
        state.metrics.clear();
        state.namespaces.clear();
        for usage in state.usage.values_mut() {
            usage.namespaces = 0;
            usage.metrics = 0;
            usage.data_points = 0;
            usage.logical_size = 0;
            usage.physical_size = 0;
        }
        for (metric, costs) in metrics {
            self.register_locked(&mut state, &metric, costs);
        }
    }
    pub fn remove_metric(&self, metric: &str) -> bool {
        let metrics = {
            let mut state = self.state.lock().unwrap();
            if state.metrics.remove(metric).is_none() {
                return false;
            }
            state
                .metrics
                .iter()
                .map(|(metric, value)| (metric.clone(), value.costs))
                .collect()
        };
        self.reconcile(metrics);
        true
    }
    fn reset_if_due(&self, state: &mut State) {
        if self.window.is_zero() || state.window_at.elapsed() < self.window {
            return;
        }
        let elapsed = state.window_at.elapsed().as_secs_f64() / self.window.as_secs_f64();
        for (name, usage) in &mut state.usage {
            if let Some(rule) = self.rule_for(name)
                && rule.throughput > 0
                && (usage.throughput as f64) > rule.throughput as f64 * elapsed
            {
                continue;
            }
            usage.throughput = 0;
        }
        state.window_at = Instant::now();
    }
    fn handles(&self, metric: &str) -> Vec<String> {
        let mut out = vec!["/".to_owned()];
        let parent = metric.rsplit_once('.').map(|x| x.0).unwrap_or("");
        for (i, b) in parent.bytes().enumerate() {
            if b == b'.' {
                out.push(parent[..i].to_owned());
            }
        }
        if !parent.is_empty() {
            out.push(parent.to_owned());
        }
        out
    }
    fn rule_for(&self, namespace: &str) -> Option<&Rule> {
        self.rules
            .iter()
            .rev()
            .find(|compiled| {
                if compiled.rule.pattern == "/" {
                    namespace == "/"
                } else {
                    compiled
                        .glob
                        .as_ref()
                        .is_some_and(|glob| glob.matches(namespace))
                }
            })
            .map(|x| &x.rule)
    }
    fn exceeded(
        &self,
        namespace: &str,
        usage: Usage,
        add: Costs,
    ) -> Option<(Rejection, DroppingPolicy)> {
        let rule = self.rule_for(namespace)?;
        for (limit, value, reason) in [
            (
                rule.namespaces,
                usage.namespaces.saturating_add(add.namespaces),
                Rejection::Namespaces,
            ),
            (
                rule.metrics,
                usage.metrics.saturating_add(add.metrics),
                Rejection::Metrics,
            ),
            (
                rule.data_points,
                usage.data_points.saturating_add(add.data_points),
                Rejection::DataPoints,
            ),
            (
                rule.logical_size,
                usage.logical_size.saturating_add(add.logical_size),
                Rejection::LogicalSize,
            ),
            (
                rule.physical_size,
                usage.physical_size.saturating_add(add.physical_size),
                Rejection::PhysicalSize,
            ),
        ] {
            if limit > 0 && value > limit {
                return Some((reason, rule.dropping_policy));
            }
        }
        None
    }
}
fn add_usage(usage: &mut Usage, costs: Costs) {
    usage.namespaces += costs.namespaces;
    usage.metrics += costs.metrics;
    usage.data_points += costs.data_points;
    usage.logical_size += costs.logical_size;
    usage.physical_size += costs.physical_size;
}
fn subtract_usage(usage: &mut Usage, costs: Costs) {
    usage.namespaces -= costs.namespaces;
    usage.metrics -= costs.metrics;
    usage.data_points -= costs.data_points;
    usage.logical_size -= costs.logical_size;
    usage.physical_size -= costs.physical_size;
}
fn difference(a: Costs, b: Costs) -> Costs {
    Costs {
        namespaces: a.namespaces - b.namespaces,
        metrics: a.metrics - b.metrics,
        data_points: a.data_points - b.data_points,
        logical_size: a.logical_size - b.logical_size,
        physical_size: a.physical_size - b.physical_size,
    }
}
fn number(values: &HashMap<String, String>, key: &str) -> Result<i64, String> {
    match values.get(key).map(String::as_str).unwrap_or("") {
        "" => Ok(0),
        "max" | "maximum" => Ok(i64::MAX),
        v => {
            let parsed: i64 = v
                .replace(',', "")
                .parse()
                .map_err(|_| format!("invalid {key}"))?;
            if parsed < 0 {
                Err(format!("invalid {key}"))
            } else {
                Ok(parsed)
            }
        }
    }
}
// Throughput is configured per minute and scaled to the usage-report window.
// Float math keeps sub-minute windows enforceable; `as` saturates, so `max` stays unlimited.
fn throughput(per_minute: i64, window: Duration) -> i64 {
    (per_minute as f64 * window.as_secs_f64() / 60.0) as i64
}

fn in_namespace(metric: &str, namespace: &str) -> bool {
    let parent = metric.rsplit_once('.').map(|x| x.0).unwrap_or("");
    parent == namespace || parent.starts_with(&(namespace.to_owned() + "."))
}
fn pending_namespace_exists(state: &State, namespace: &str) -> bool {
    state
        .pending
        .values()
        .any(|pending| in_namespace(&pending.metric, namespace))
}
type IniSections = Vec<(String, HashMap<String, String>)>;
fn parse_ini(input: &str) -> Result<IniSections, String> {
    let mut out = Vec::new();
    let mut current = None;
    for (line, raw) in input.lines().enumerate() {
        let text = raw.trim();
        if text.is_empty() || text.starts_with('#') || text.starts_with(';') {
            continue;
        }
        if text.starts_with('[') && text.ends_with(']') {
            if let Some(v) = current.take() {
                out.push(v)
            };
            let p = text[1..text.len() - 1].trim();
            if p.is_empty() {
                return Err(format!("line {}: empty section", line + 1));
            }
            current = Some((p.to_owned(), HashMap::new()));
        } else {
            let (key, value) = text
                .split_once('=')
                .ok_or_else(|| format!("line {}: expected key = value", line + 1))?;
            let Some((_, fields)) = current.as_mut() else {
                return Err(format!("line {}: config section not found", line + 1));
            };
            fields.insert(
                key.trim().to_ascii_lowercase(),
                value.trim().trim_matches(['\"', '\'']).to_owned(),
            );
        }
    }
    if let Some(v) = current {
        out.push(v)
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn new_metric_reservation_and_last_matching_rule() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("q");
        fs::write(&file, "[/]\nmetrics = 1\n[sys.*]\nmetrics = 2\n").unwrap();
        let q = Engine::load(file, Duration::from_secs(60)).unwrap();
        let a = q.admit("sys.app.one", 1, Costs::metric(1, 1, 1)).unwrap();
        assert!(a.new_metric);
        assert!(q.commit(a));
        let b = q.admit("sys.app.two", 1, Costs::metric(1, 1, 1));
        assert_eq!(b, Err(Rejection::Metrics));
    }
    #[test]
    fn release_does_not_charge_retry_as_new() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("q");
        fs::write(&file, "[/]\nmetrics=1\n").unwrap();
        let q = Engine::load(file, Duration::from_secs(60)).unwrap();
        let r = q.admit("a", 1, Costs::metric(1, 1, 1)).unwrap();
        assert!(q.release(r));
        assert!(q.admit("a", 1, Costs::metric(1, 1, 1)).is_ok());
    }
    #[test]
    fn sibling_reservations_share_their_pending_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("q");
        fs::write(&file, "[/]\nnamespaces=1\nmetrics=2\n").unwrap();
        let q = Engine::load(file, Duration::from_secs(60)).unwrap();
        let first = q.admit("a.first", 1, Costs::metric(1, 1, 1)).unwrap();
        let second = q.admit("a.second", 1, Costs::metric(1, 1, 1)).unwrap();
        assert!(q.commit(first));
        assert!(q.commit(second));
        assert_eq!(q.usage("/").namespaces, 1);
    }
    #[test]
    fn released_sibling_hands_namespace_charge_to_pending_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("q");
        fs::write(&file, "[/]\nnamespaces=1\nmetrics=2\n").unwrap();
        let q = Engine::load(file, Duration::from_secs(60)).unwrap();
        let first = q.admit("a.first", 1, Costs::metric(1, 1, 1)).unwrap();
        let second = q.admit("a.second", 1, Costs::metric(1, 1, 1)).unwrap();
        assert!(q.release(first));
        assert!(q.commit(second));
        assert_eq!(q.usage("/").namespaces, 1);
    }
    #[test]
    fn throughput_scales_to_report_window() {
        assert_eq!(throughput(100, Duration::from_secs(30)), 50);
        assert_eq!(throughput(i64::MAX, Duration::from_secs(300)), i64::MAX);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("q");
        fs::write(&file, "[/]\nthroughput=2\n").unwrap();
        let q = Engine::load(file, Duration::from_secs(120)).unwrap();
        assert!(q.admit("a", 4, Costs::metric(1, 1, 1)).is_ok());
        assert_eq!(
            q.admit("b", 1, Costs::metric(1, 1, 1)),
            Err(Rejection::Throughput)
        );
    }
}
