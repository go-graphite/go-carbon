package carbonserver

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// V2QuotaMetric describes logical storage from the stored policy, including
// classic-only names. Physical quotas are unsupported for shared immutable parts.
type V2QuotaMetric struct {
	Name                    string
	LogicalSize, DataPoints int64
}

type v2QuotaName struct {
	metric    V2QuotaMetric
	accepted  atomic.Uint64
	confirmed uint64 // publication owner, under mu
}

// V2Quotas keeps one host-wide classic quota authority. Existing-name throughput
// admission uses atomics only. New-name reservations and catalog replacement
// serialize metadata, never producer buffers or core writes. Full trie rebuilds
// are correctness scaffolding; large-catalog cost remains unqualified.
type V2Quotas struct {
	mu        sync.Mutex
	index     atomic.Pointer[trieIndex]
	known     sync.Map // name -> *v2QuotaName
	names     map[string]*v2QuotaName
	rules     []*Quota
	frequency time.Duration
	estimate  func(string) (int64, int64, int64)
	sequence  atomic.Uint64
	rejected  atomic.Uint64
}

func NewV2Quotas(rules []*Quota, frequency time.Duration, estimate func(string) (int64, int64, int64)) (*V2Quotas, error) {
	if frequency <= 0 || estimate == nil {
		return nil, errors.New("v2 quotas require frequency and logical size estimator")
	}
	q := &V2Quotas{names: map[string]*v2QuotaName{}, frequency: frequency, estimate: estimate}
	for _, rule := range rules {
		if rule == nil || (rule.PhysicalSize > 0 && rule.PhysicalSize != int64(^uint64(0)>>1)) {
			return nil, errors.New("v2 physical quotas unsupported")
		}
		r := *rule
		r.PhysicalSize = 0
		q.rules = append(q.rules, &r)
	}
	// Validate before exposing any authority. No admissions until Publish.
	t := newTrie(".wsp", 0, estimate)
	if _, err := t.applyQuotas(frequency, q.rules...); err != nil {
		return nil, err
	}
	return q, nil
}

// Fence must be sampled BEFORE collecting the reader's immutable catalog image.
// It prevents that publication from consuming later pending name reservations.
func (q *V2Quotas) Fence() uint64 { return q.sequence.Load() }

// Rejected counts points/applications refused by the quota gate.
func (q *V2Quotas) Rejected() uint64 { return q.rejected.Load() }

const v2QuotaRetired uint64 = 1 << 63

func (n *v2QuotaName) admit(sequence uint64) bool {
	for {
		old := n.accepted.Load()
		if old&v2QuotaRetired != 0 {
			return false
		}
		if old >= sequence || n.accepted.CompareAndSwap(old, sequence) {
			return true
		}
	}
}

// Publish refreshes all host storage usage, retaining unobserved reservations and
// unchanged throughput windows. Repeated catalog refreshes never rotate usage.
func (q *V2Quotas) Publish(metrics []V2QuotaMetric, fence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if fence > q.sequence.Load() {
		return errors.New("v2 quota fence exceeds admission frontier")
	}
	present := make(map[string]V2QuotaMetric, len(metrics))
	for _, m := range metrics {
		if m.Name == "" || m.LogicalSize < 0 || m.DataPoints < 0 {
			return errors.New("invalid v2 quota metric")
		}
		present[m.Name] = m
	}
	next := make(map[string]*v2QuotaName, len(metrics))
	for name, m := range present {
		n := q.names[name]
		if n == nil {
			n = &v2QuotaName{}
		}
		n.metric = m
		n.confirmed = max(n.confirmed, fence)
		next[name] = n
	}
	for name, n := range q.names {
		if _, ok := next[name]; ok {
			continue
		}
		for {
			prior := n.accepted.Load()
			if prior&v2QuotaRetired != 0 {
				break
			}
			if prior > n.confirmed {
				next[name] = n
				break
			}
			// Fence admission atomically before dropping membership. A receiver that
			// lost this race re-enters the new-name reservation path after publication.
			if n.accepted.CompareAndSwap(prior, prior|v2QuotaRetired) {
				break
			}
		}
	}
	t := newTrie(".wsp", 0, q.estimate)
	for name, n := range next {
		m := n.metric
		if _, err := t.insert(metricStoreVirtualPath(name), m.LogicalSize, 0, m.DataPoints, 0); err != nil {
			return err
		}
	}
	if old := q.index.Load(); old != nil {
		t.throughputs = old.throughputs
	}
	if err := q.apply(t); err != nil {
		return err
	}
	q.updateUsage(t)
	q.names = next
	for name, n := range next {
		q.known.Store(name, n)
	}
	q.known.Range(func(key, value any) bool {
		name := key.(string)
		if _, exists := next[name]; !exists {
			q.known.CompareAndDelete(key, value)
		}
		return true
	})
	q.index.Store(t)
	return nil
}

// ShouldThrottleMetric reserves new-name logical storage before accepted points
// enter any producer queue. inCache has classic's meaning: throughput still
// applies, while existing storage bypasses creation checks.
func (q *V2Quotas) ShouldThrottleMetric(ps *points.Points, inCache bool) (throttled bool) {
	defer func() {
		if throttled {
			q.rejected.Add(1)
		}
	}()
	t := q.index.Load()
	if t == nil || ps == nil {
		return true
	}
	if t.throughputThrottle(ps) {
		return true
	}
	seq := q.sequence.Add(1)
	if value, ok := q.known.Load(ps.Metric); ok {
		n := value.(*v2QuotaName)
		if n.admit(seq) {
			if current, exists := q.known.Load(ps.Metric); exists && current == value {
				return false
			}
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	t = q.index.Load()
	if n := q.names[ps.Metric]; n != nil {
		n.admit(seq)
		return false
	}
	if !inCache && t.throttleMetric(ps) {
		return true
	}
	logical, _, count := q.estimate(ps.Metric)
	m := V2QuotaMetric{Name: ps.Metric, LogicalSize: logical, DataPoints: count}
	if _, err := t.insert(metricStoreVirtualPath(m.Name), logical, 0, count, 0); err != nil {
		return true
	}
	// Newly materialized namespaces must receive matching rules before another
	// new name can reserve their capacity. Existing throughput recorders survive.
	if err := q.apply(t); err != nil {
		return true
	}
	q.updateUsage(t)
	n := &v2QuotaName{metric: m}
	n.accepted.Store(seq)
	q.names[m.Name] = n
	q.known.Store(m.Name, n)
	return false
}

func (q *V2Quotas) updateUsage(t *trieIndex) {
	assignments, _, _ := t.resolveQuotas(q.rules)
	for path, meta := range t.quotaNodes {
		usage := quotaStorageUsage(assignments[path].node)
		atomic.StoreInt64(&meta.usage.Metrics, usage.Metrics)
		atomic.StoreInt64(&meta.usage.Namespaces, usage.Namespaces)
		atomic.StoreInt64(&meta.usage.LogicalSize, usage.LogicalSize)
		atomic.StoreInt64(&meta.usage.DataPoints, usage.DataPoints)
	}
}

// An expired namespace can be recreated within the same throughput interval.
// Keep its recorder even when no storage node currently materializes its rule.
func (q *V2Quotas) apply(t *trieIndex) error {
	saved := map[string]*throughputUsagePerNamespace{}
	depth := t.throughputs.getDepth()
	t.throughputs.entries.Range(func(k, v any) bool { saved[k.(string)] = v.(*throughputUsagePerNamespace); return true })
	manager, err := t.applyQuotas(q.frequency, q.rules...)
	if err != nil {
		return err
	}
	for path, old := range saved {
		if manager.load(path) == nil {
			manager.entries.Store(path, old)
		}
	}
	depth = max(depth, manager.getDepth())
	atomic.StoreInt64(&manager.depth, int64(depth))
	manager.publishRoutes(depth)
	return nil
}
