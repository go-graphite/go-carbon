package carbonserver

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// renderRangeCache owns immutable, unoverlaid values from the finest archive.
// A metric revision validates ALL cached slots, including late corrections.
// Coarser archives need an explicit archive reader to keep tail resolution fixed;
// they retain the full-fetch path. The budget covers retained entries, not active
// readers holding an evicted entry or the ordinary response arrays.
type renderRangeCache struct {
	mu          sync.Mutex
	entries     map[string]*list.Element
	lru         list.List
	used, limit int64
	mutable     time.Duration
	now         func() time.Time
}

type renderRangeEntry struct {
	series     store.Series
	epoch      uint64
	bytes      int64
	retryAfter int64 // revision-changing series bypass admission briefly
}

// SetQueryRangeCache configures the optional per-series historical-prefix cache.
// Reconfiguration drops the old namespace; in-flight readers may finish using it.
func (listener *CarbonserverListener) SetQueryRangeCache(enabled bool, sizeMB int, mutable time.Duration) error {
	if !enabled {
		listener.queryRangeCache.Store(nil)
		return nil
	}
	if sizeMB <= 0 || int64(sizeMB) > math.MaxInt64/(1<<20) || mutable < time.Second {
		return fmt.Errorf("query range cache requires a positive size and mutable window >= 1s")
	}
	listener.queryRangeCache.Store(&renderRangeCache{
		entries: make(map[string]*list.Element), limit: int64(sizeMB) * (1 << 20), mutable: mutable, now: time.Now,
	})
	return nil
}

func (c *renderRangeCache) get(name string) *renderRangeEntry {
	if !c.mu.TryLock() {
		return nil
	}
	defer c.mu.Unlock()
	if e := c.entries[name]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(*renderRangeEntry)
	}
	return nil
}

func finestRenderRange(m store.Metadata, from, until int, now int64) bool {
	return len(m.Retentions) > 0 && from >= 0 && until > from && int64(until) <= now && int64(from) <= now &&
		now-int64(from) <= int64(m.Retentions[0].MaxRetention())
}

func (c *renderRangeCache) put(s *store.Series, epoch uint64, now int64) {
	if s == nil || s.Step <= 0 || len(s.Metadata.Name) > 4096 || len(s.Metadata.Retentions) > 64 || len(s.Metadata.Retentions) == 0 || s.Step != s.Metadata.Retentions[0].Step {
		return
	}
	// Round down: no cached slot may overlap the recent-data exclusion period.
	cutoff := now - int64(c.mutable/time.Second)
	end := min(int64(s.UntilTime), cutoff-cutoff%int64(s.Step))
	n := (end - int64(s.FromTime)) / int64(s.Step)
	if n <= 0 || n > int64(len(s.Values)) || n*3 < int64(len(s.Values)) {
		return
	}
	size := int64(512+len(s.Metadata.Name)+16*len(s.Metadata.Retentions)) + n*8
	if size > c.limit {
		return
	}
	// Cap per-request copying and eviction work independently of the 64MiB
	// retained-entry budget. Contention is a cache miss, never a queued render.
	if size > 1<<20 {
		return
	}
	covered := func() bool {
		if old := c.entries[s.Metadata.Name]; old != nil {
			p := old.Value.(*renderRangeEntry)
			return p.epoch == epoch && p.series.Metadata.SameReadRevision(s.Metadata) &&
				p.series.FromTime <= s.FromTime && p.series.UntilTime >= int(end)
		}
		return false
	}
	if !c.mu.TryLock() {
		return
	}
	_, _, fits := c.spaceFor(s.Metadata.Name, size)
	skip := covered() || !fits
	c.mu.Unlock()
	if skip {
		return
	}
	// The caller overlays live ingestion points after return. Own the retained
	// data and metadata; no allocation or copying happens under the cache lock.
	entry := &renderRangeEntry{series: *s, epoch: epoch, bytes: size}
	entry.series.Metadata.Name = strings.Clone(s.Metadata.Name)
	entry.series.Metadata.Retentions = slices.Clone(s.Metadata.Retentions)
	entry.series.Values = slices.Clone(s.Values[:n])
	entry.series.UntilTime = int(end)
	if !c.mu.TryLock() {
		return
	}
	defer c.mu.Unlock()
	if covered() {
		return
	}
	name := entry.series.Metadata.Name
	old := c.entries[name]
	victims, victimCount, fits := c.spaceFor(name, size)
	if !fits {
		return
	}
	remove := func(e *list.Element) {
		previous := e.Value.(*renderRangeEntry)
		delete(c.entries, previous.series.Metadata.Name)
		c.used -= previous.bytes
		c.lru.Remove(e)
	}
	if old != nil {
		remove(old)
	}
	for _, e := range victims[:victimCount] {
		remove(e)
	}
	c.entries[name] = c.lru.PushFront(entry)
	c.used += size
}

// spaceFor is called under mu; it never changes the cache. Fixed victim storage
// keeps admission allocation-free until bounded space has been found.
func (c *renderRangeCache) spaceFor(name string, size int64) (victims [4]*list.Element, n int, ok bool) {
	available := c.limit - c.used
	old := c.entries[name]
	if old != nil {
		available += old.Value.(*renderRangeEntry).bytes
	}
	for e := c.lru.Back(); available < size && e != nil && n < len(victims); e = e.Prev() {
		if e == old {
			continue
		}
		victims[n] = e
		n++
		available += e.Value.(*renderRangeEntry).bytes
	}
	return victims, n, available >= size
}

// backoff replaces invalid values with a small, bounded LRU marker. Readers
// holding the immutable old entry can finish. A stable series may be admitted
// again after at most one minute; recent writeout bursts use ordinary Fetch.
func (c *renderRangeCache) backoff(name string, old *renderRangeEntry, metadata store.Metadata, now int64) {
	metadata.Name = old.series.Metadata.Name
	metadata.Retentions = slices.Clone(metadata.Retentions)
	marker := &renderRangeEntry{series: store.Series{Metadata: metadata}, epoch: old.epoch,
		bytes:      int64(512 + len(metadata.Name) + 16*len(metadata.Retentions)),
		retryAfter: now + min(60, int64(c.mutable/time.Second))}
	if !c.mu.TryLock() {
		return
	}
	defer c.mu.Unlock()
	e := c.entries[name]
	if e == nil || e.Value.(*renderRangeEntry) != old {
		return
	}
	c.used += marker.bytes - old.bytes
	e.Value = marker
}

func (c *renderRangeCache) fetch(ctx context.Context, db *store.Store, name string, from, until int) (*store.Series, int, error) {
	now := c.now().Unix()
	epoch := db.ReadCacheEpoch()
	old := c.get(name)
	if old != nil && old.epoch == epoch && old.retryAfter > now {
		s, err := db.Fetch(ctx, name, from, until)
		return s, 0, err
	}
	var prefix *store.Series
	if old != nil && old.epoch == epoch && old.series.Step > 0 && finestRenderRange(old.series.Metadata, from, until, now) {
		// Clip a local header, never the retained immutable values. Validation and
		// the tail read happen in one store read view; misses cost one Fetch.
		candidate := old.series
		cutoff := now - int64(c.mutable/time.Second)
		candidate.UntilTime = min(candidate.UntilTime, int(cutoff-cutoff%int64(candidate.Step)))
		step := candidate.Step
		start := from - from%step + step
		end := until - until%step + step
		// Tiny prefixes pay nearly the full tail-fetch cost plus assembly. Keep
		// the ordinary path when less than a third of the result can be reused.
		if min(candidate.UntilTime, end)-start < (end-start)/3 {
			s, err := db.Fetch(ctx, name, from, until)
			return s, 0, err
		}
		prefix = &candidate
	}
	var s *store.Series
	var reused int
	var err error
	if prefix == nil {
		s, err = db.Fetch(ctx, name, from, until)
	} else {
		s, reused, err = db.FetchWithPrefix(ctx, name, from, until, prefix, epoch)
	}
	current := c.now().Unix()
	if err != nil || s == nil {
		return s, 0, err
	}
	if reused > 0 && (current < now || !finestRenderRange(s.Metadata, from, until, current) || db.ReadCacheEpoch() != epoch) {
		s, err = db.Fetch(ctx, name, from, until)
		return s, 0, err
	}
	if finestRenderRange(s.Metadata, from, until, current) && db.ReadCacheEpoch() == epoch {
		if old != nil && old.epoch == epoch && !s.Metadata.SameReadRevision(old.series.Metadata) {
			// Whole-series revisions cannot distinguish tail writes from corrections.
			// Do not repeatedly copy large prefixes of actively written series.
			c.backoff(name, old, s.Metadata, current)
		} else {
			c.put(s, epoch, current)
		}
	}
	return s, reused, nil
}

func (listener *CarbonserverListener) fetchMetricStoreSeries(ctx context.Context, db *store.Store, name string, from, until int) (*store.Series, error) {
	if listener.capacityReadLatency != nil {
		started := time.Now()
		defer func() { listener.capacityReadLatency.Observe(time.Since(started)) }()
	}
	c := listener.queryRangeCache.Load()
	if c == nil {
		return db.Fetch(ctx, name, from, until)
	}
	s, reused, err := c.fetch(ctx, db, name, from, until)
	if err == nil && s != nil {
		listener.prometheus.cacheRequest("render_range", reused > 0)
		if reused > 0 {
			atomic.AddUint64(&listener.metrics.RangeCacheHit, 1)
			atomic.AddUint64(&listener.metrics.RangeCacheValues, uint64(reused))
		} else {
			atomic.AddUint64(&listener.metrics.RangeCacheMiss, 1)
		}
	}
	return s, err
}
