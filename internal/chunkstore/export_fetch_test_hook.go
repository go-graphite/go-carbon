package chunkstore

import (
	"context"
	"sync/atomic"
)

// SetFetchSharedIteratorForTest switches the shared fetch iterator for tests
// and benchmarks in other packages and returns a function restoring it.
func (s *Store) SetFetchSharedIteratorForTest(on bool) func() {
	previous := s.fetchSharedIterator
	s.fetchSharedIterator = on
	return func() { s.fetchSharedIterator = previous }
}

// DisableMetadataCacheForTest makes the store read every metadata key from the
// database until the returned function restores the cache. Use it only while
// no other goroutine uses the store.
func (s *Store) DisableMetadataCacheForTest() func() {
	previous := s.stateCache
	s.stateCache = nil
	return func() { s.stateCache = previous }
}

// MetadataCacheEnabledForTest reports whether the store caches metadata.
func (s *Store) MetadataCacheEnabledForTest() bool { return s.stateCache != nil }

// countStoreReads enables storeReads, the LSM point lookups and walks that
// fetches and continuation updates issue, for tests and benchmarks.
var (
	countStoreReads atomic.Bool
	storeReads      atomic.Uint64
)

// CountStoreReadsForTest starts counting LSM reads and returns a function
// that stops counting and reports how many were made.
func CountStoreReadsForTest() func() uint64 {
	storeReads.Store(0)
	countStoreReads.Store(true)
	return func() uint64 {
		countStoreReads.Store(false)
		return storeReads.Load()
	}
}

func countStoreRead() {
	if countStoreReads.Load() {
		storeReads.Add(1)
	}
}

// prewarmBeforeFill, when set by a test, runs after a prewarm read a name's
// state and before it fills the cache with it.
var prewarmBeforeFill func(name string)

// SetPrewarmBeforeFillForTest makes PrewarmMetadata call hook after reading
// each name's state and before filling the cache with it, until the returned
// function restores the previous hook. Use it only while no prewarm runs.
func SetPrewarmBeforeFillForTest(hook func(name string)) func() {
	previous := prewarmBeforeFill
	prewarmBeforeFill = hook
	return func() { prewarmBeforeFill = previous }
}

// ForgetMetadataCacheForTest drops every cached state that no mutation holds
// pending, as eviction would.
func (s *Store) ForgetMetadataCacheForTest() {
	c := s.stateCache
	if c == nil {
		return
	}
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for s := sh.lruTail; s >= 0; {
			prev := sh.slots[s].prev
			if sh.entries[string(recordKey(sh.record(s)))] == nil {
				c.remove(sh, sh.position(s), s)
				c.entries.Add(-1)
			}
			s = prev
		}
		c.compact(sh)
		sh.mu.Unlock()
	}
}

// SameMetricStripeForTest reports whether two metrics share a lock stripe and
// metadata cache shard.
func (s *Store) SameMetricStripeForTest(a, b string) bool {
	return s.metricStripe(a) == s.metricStripe(b)
}

// PlantCatalogNameForTest records name in the catalog without the creation
// name check, like a name admitted before MaxMetricNameBytes was lowered.
func (s *Store) PlantCatalogNameForTest(ctx context.Context, name string, config MetricConfig) error {
	config.Name = "plant.for.test"
	m, err := s.Create(ctx, config)
	if err != nil {
		return err
	}
	m.Name, m.ID = name, m.ID+1<<32
	value, err := s.encodeCatalog(m)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(catalogKey(name), value, nil); err != nil {
		return err
	}
	if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return err
	}
	if err := b.Commit(nil); err != nil {
		return err
	}
	return s.Delete(ctx, "plant.for.test")
}
