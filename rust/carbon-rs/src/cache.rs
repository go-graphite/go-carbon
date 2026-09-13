use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use whisper_rs::Point;

const SHARDS: usize = 1024;
const POINT_BYTES: u64 = std::mem::size_of::<Point>() as u64;

#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    pub id: u64,
    pub metric: String,
    pub points: Arc<Vec<Point>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct Stats {
    pub pending_points: u64,
    pub in_flight_points: u64,
    pub bytes: u64,
    pub dropped_points: u64,
    pub high_water_points: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddError {
    Full,
}

struct Entry {
    pending: Vec<Point>,
    in_flight: Option<Batch>,
    scheduled: bool,
}

struct Shard {
    entries: HashMap<String, Entry>,
}

/// A no-priority write buffer. A metric is queued once until a worker takes it.
pub struct Cache {
    shards: Vec<Mutex<Shard>>,
    ready: Mutex<VecDeque<String>>,
    active: Mutex<HashMap<u64, String>>,
    admission: Mutex<()>,
    max_points: u64,
    max_bytes: u64,
    pending: AtomicU64,
    total: AtomicU64,
    in_flight: AtomicU64,
    bytes: AtomicU64,
    dropped: AtomicU64,
    high_water: AtomicU64,
    next_batch: AtomicU64,
}

impl Cache {
    pub fn new(max_points: u64, max_bytes: u64) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                    })
                })
                .collect(),
            ready: Mutex::new(VecDeque::new()),
            active: Mutex::new(HashMap::new()),
            admission: Mutex::new(()),
            max_points,
            max_bytes,
            pending: AtomicU64::new(0),
            total: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            high_water: AtomicU64::new(0),
            next_batch: AtomicU64::new(1),
        }
    }

    pub fn add(&self, metric: String, point: Point) -> Result<(), AddError> {
        let _admission = self.admission.lock().unwrap();
        let entry_bytes = POINT_BYTES
            + if self.entry_exists(&metric) {
                0
            } else {
                metric.len() as u64
            };
        if (self.max_points > 0 && self.total_points() >= self.max_points)
            || (self.max_bytes > 0
                && self
                    .bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(entry_bytes)
                    > self.max_bytes)
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(AddError::Full);
        }
        self.insert(metric, point, entry_bytes);
        Ok(())
    }

    /// Restore and retry bypass fresh-admission limits: those points were accepted already.
    pub fn restore(&self, metric: String, points: impl IntoIterator<Item = Point>) {
        let _admission = self.admission.lock().unwrap();
        for point in points {
            self.insert(
                metric.clone(),
                point,
                POINT_BYTES
                    + if self.entry_exists(&metric) {
                        0
                    } else {
                        metric.len() as u64
                    },
            );
        }
    }

    pub fn take(&self) -> Option<Batch> {
        loop {
            let metric = self.ready.lock().unwrap().pop_front()?;
            let mut shard = self.shards[self.shard(&metric)].lock().unwrap();
            let Some(entry) = shard.entries.get_mut(&metric) else {
                continue;
            };
            // Dequeue ownership is consumed even when another worker is writing it.
            // `confirm` will enqueue remaining pending points if needed.
            entry.scheduled = false;
            if entry.pending.is_empty() || entry.in_flight.is_some() {
                continue;
            }
            let points = Arc::new(std::mem::take(&mut entry.pending));
            let batch = Batch {
                id: self.next_batch.fetch_add(1, Ordering::Relaxed),
                metric: metric.clone(),
                points,
            };
            entry.in_flight = Some(batch.clone());
            let count = batch.points.len() as u64;
            self.pending.fetch_sub(count, Ordering::Relaxed);
            self.in_flight.fetch_add(count, Ordering::Relaxed);
            drop(shard);
            self.active.lock().unwrap().insert(batch.id, metric);
            return Some(batch);
        }
    }

    /// Confirms only the exact active batch. Repeated or stale confirmations are harmless.
    pub fn confirm(&self, id: u64) -> bool {
        self.finish(id, false)
    }
    pub fn retry(&self, id: u64) -> bool {
        self.finish(id, true)
    }

    pub fn get(&self, metric: &str) -> Vec<Point> {
        let shard = self.shards[self.shard(metric)].lock().unwrap();
        let Some(entry) = shard.entries.get(metric) else {
            return Vec::new();
        };
        let mut result = entry
            .in_flight
            .as_ref()
            .map_or_else(Vec::new, |b| (*b.points).clone());
        result.extend_from_slice(&entry.pending);
        result
    }

    pub fn dump(&self) -> Vec<(String, Vec<Point>)> {
        self.shards
            .iter()
            .flat_map(|shard| {
                let shard = shard.lock().unwrap();
                shard
                    .entries
                    .iter()
                    .map(|(name, entry)| {
                        let mut points = entry
                            .in_flight
                            .as_ref()
                            .map_or_else(Vec::new, |b| (*b.points).clone());
                        points.extend_from_slice(&entry.pending);
                        (name.clone(), points)
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|(_, points)| !points.is_empty())
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.total_points() == 0
    }
    pub fn stats(&self) -> Stats {
        Stats {
            pending_points: self.pending.load(Ordering::Relaxed),
            in_flight_points: self.in_flight.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            dropped_points: self.dropped.load(Ordering::Relaxed),
            high_water_points: self.high_water.load(Ordering::Relaxed),
        }
    }

    fn finish(&self, id: u64, retry: bool) -> bool {
        let _admission = self.admission.lock().unwrap();
        let Some(name) = self.active.lock().unwrap().get(&id).cloned() else {
            return false;
        };
        let mut enqueue = false;
        let mut shard = self.shards[self.shard(&name)].lock().unwrap();
        let Some(entry) = shard.entries.get_mut(&name) else {
            return false;
        };
        let Some(batch) = entry
            .in_flight
            .as_ref()
            .filter(|batch| batch.id == id)
            .cloned()
        else {
            return false;
        };
        entry.in_flight = None;
        let count = batch.points.len() as u64;
        self.in_flight.fetch_sub(count, Ordering::Relaxed);
        if retry {
            let mut recovered = (*batch.points).clone();
            recovered.append(&mut entry.pending);
            entry.pending = recovered;
            self.pending.fetch_add(count, Ordering::Relaxed);
        } else {
            self.total.fetch_sub(count, Ordering::Relaxed);
            self.bytes
                .fetch_sub(POINT_BYTES.saturating_mul(count), Ordering::Relaxed);
        }
        if !entry.pending.is_empty() && !entry.scheduled {
            entry.scheduled = true;
            enqueue = true;
        }
        if !retry && entry.pending.is_empty() {
            self.bytes
                .fetch_sub(entry_name_bytes(&name), Ordering::Relaxed);
            shard.entries.remove(&name);
        }
        drop(shard);
        self.active.lock().unwrap().remove(&id);
        if enqueue {
            self.ready.lock().unwrap().push_back(name);
        }
        true
    }

    fn insert(&self, metric: String, point: Point, bytes: u64) {
        let mut enqueue = false;
        let mut shard = self.shards[self.shard(&metric)].lock().unwrap();
        let entry = shard
            .entries
            .entry(metric.clone())
            .or_insert_with(|| Entry {
                pending: Vec::new(),
                in_flight: None,
                scheduled: false,
            });
        entry.pending.push(point);
        if !entry.scheduled {
            entry.scheduled = true;
            enqueue = true;
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
        self.high_water
            .fetch_max(self.total_points(), Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        drop(shard);
        if enqueue {
            self.ready.lock().unwrap().push_back(metric);
        }
    }

    fn entry_exists(&self, metric: &str) -> bool {
        self.shards[self.shard(metric)]
            .lock()
            .unwrap()
            .entries
            .contains_key(metric)
    }
    fn total_points(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
    fn shard(&self, metric: &str) -> usize {
        metric
            .bytes()
            .fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619)) as usize
            % SHARDS
    }
}

fn entry_name_bytes(name: &str) -> u64 {
    name.len() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_does_not_starve_healthy_metrics_or_free_capacity() {
        let cache = Cache::new(2, 0);
        cache.add("broken".into(), p(1)).unwrap();
        cache.add("healthy".into(), p(1)).unwrap();
        let broken = cache.take().unwrap();
        assert_eq!(cache.total_points(), 2);
        assert!(cache.add("extra".into(), p(1)).is_err());
        cache.retry(broken.id);
        let healthy = cache.take().unwrap();
        assert_eq!(healthy.metric, "healthy");
        cache.confirm(healthy.id);
        assert_eq!(cache.total_points(), 1);
    }
    fn p(n: i64) -> Point {
        Point {
            timestamp: n,
            value: n as f64,
        }
    }
    #[test]
    fn retry_keeps_old_points_before_new_and_confirm_is_exact() {
        let c = Cache::new(10, 0);
        c.add("a".into(), p(1)).unwrap();
        let b = c.take().unwrap();
        c.add("a".into(), p(2)).unwrap();
        assert!(c.retry(b.id));
        assert_eq!(c.get("a"), vec![p(1), p(2)]);
        assert!(!c.confirm(b.id));
        let b = c.take().unwrap();
        assert_eq!(&*b.points, &[p(1), p(2)]);
        assert!(c.confirm(b.id));
        assert!(c.is_empty());
    }
    #[test]
    fn get_and_dump_include_in_flight() {
        let c = Cache::new(2, 0);
        c.add("a".into(), p(1)).unwrap();
        let b = c.take().unwrap();
        c.add("a".into(), p(2)).unwrap();
        assert_eq!(c.get("a"), vec![p(1), p(2)]);
        assert_eq!(c.dump(), vec![("a".into(), vec![p(1), p(2)])]);
        assert!(c.confirm(b.id));
    }
    #[test]
    fn consumed_ready_entry_is_restored_after_confirmation() {
        let c = Cache::new(3, 0);
        c.add("a".into(), p(1)).unwrap();
        let first = c.take().unwrap();
        c.add("a".into(), p(2)).unwrap();
        assert!(c.take().is_none()); // another worker saw the queued metric while first was active
        assert!(c.confirm(first.id));
        let second = c.take().unwrap();
        assert_eq!(&*second.points, &[p(2)]);
        assert!(c.confirm(second.id));
        assert!(c.is_empty());
    }
    #[test]
    fn fresh_add_is_bounded_restore_is_not() {
        let c = Cache::new(1, 0);
        c.add("a".into(), p(1)).unwrap();
        assert_eq!(c.add("a".into(), p(2)), Err(AddError::Full));
        c.restore("a".into(), [p(2)]);
        assert_eq!(c.get("a"), vec![p(1), p(2)]);
    }
    #[test]
    fn concurrent_adds_are_drained_once() {
        let cache = Arc::new(Cache::new(1_000, 0));
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for point in 0..10 {
                        cache.add(format!("m{worker}"), p(point)).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let mut seen = 0;
        while let Some(batch) = cache.take() {
            seen += batch.points.len();
            assert!(cache.confirm(batch.id));
        }
        assert_eq!(seen, 80);
        assert!(cache.is_empty());
    }
}
