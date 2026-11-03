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
	series store.Series
	epoch  uint64
	bytes  int64
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[name]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(*renderRangeEntry)
	}
	return nil
}

func sameRenderRevision(a, b store.Metadata) bool {
	return a.ID == b.ID && a.Generation == b.Generation && a.Revision == b.Revision &&
		a.Continuation == b.Continuation && a.AggregationMethod == b.AggregationMethod &&
		math.Float32bits(a.XFilesFactor) == math.Float32bits(b.XFilesFactor) && slices.Equal(a.Retentions, b.Retentions)
}

func finestRenderRange(m store.Metadata, from, until int, now int64) bool {
	return len(m.Retentions) > 0 && from >= 0 && until > from && int64(until) <= now && int64(from) <= now &&
		now-int64(from) <= int64(m.Retentions[0].MaxRetention())
}

func (c *renderRangeCache) put(s *store.Series, epoch uint64, now int64) {
	if s == nil || s.Step <= 0 || len(s.Metadata.Retentions) == 0 || s.Step != s.Metadata.Retentions[0].Step {
		return
	}
	// Round down: no cached slot may overlap the recent-data exclusion period.
	cutoff := now - int64(c.mutable/time.Second)
	end := min(int64(s.UntilTime), cutoff-cutoff%int64(s.Step))
	n := (end - int64(s.FromTime)) / int64(s.Step)
	if n <= 0 || n > int64(len(s.Values)) {
		return
	}
	size := int64(512+len(s.Metadata.Name)+16*len(s.Metadata.Retentions)) + n*8
	if size > c.limit {
		return
	}
	// The caller later overlays the live ingestion cache into s.Values. Detach
	// all retained data and strings before returning it to that caller.
	entry := &renderRangeEntry{series: *s, epoch: epoch, bytes: size}
	entry.series.Metadata.Name = strings.Clone(s.Metadata.Name)
	entry.series.Metadata.Retentions = slices.Clone(s.Metadata.Retentions)
	entry.series.Values = make([]float64, n)
	copy(entry.series.Values, s.Values[:n])
	entry.series.UntilTime = int(end)
	c.mu.Lock()
	defer c.mu.Unlock()
	name := entry.series.Metadata.Name
	if old := c.entries[name]; old != nil {
		previous := old.Value.(*renderRangeEntry)
		if previous.epoch == epoch && sameRenderRevision(previous.series.Metadata, s.Metadata) &&
			previous.series.FromTime <= s.FromTime && previous.series.UntilTime >= int(end) {
			return
		}
		c.used -= previous.bytes
		c.lru.Remove(old)
		delete(c.entries, name)
	}
	for c.used+size > c.limit {
		old := c.lru.Back()
		previous := old.Value.(*renderRangeEntry)
		delete(c.entries, previous.series.Metadata.Name)
		c.used -= previous.bytes
		c.lru.Remove(old)
	}
	c.entries[name] = c.lru.PushFront(entry)
	c.used += size
}

func (c *renderRangeCache) fetch(ctx context.Context, db *store.Store, name string, from, until int) (*store.Series, int, error) {
	now := c.now().Unix()
	epoch := db.ReadCacheEpoch()
	full := func() (*store.Series, int, error) {
		s, err := db.Fetch(ctx, name, from, until)
		current := c.now().Unix()
		if err == nil && s != nil && finestRenderRange(s.Metadata, from, until, current) && db.ReadCacheEpoch() == epoch {
			c.put(s, epoch, current)
		}
		return s, 0, err
	}
	old := c.get(name)
	if old == nil || old.epoch != epoch {
		return full()
	}
	m, err := db.Metadata(ctx, name)
	if err != nil {
		return full()
	}
	if !sameRenderRevision(m, old.series.Metadata) || !finestRenderRange(m, from, until, now) {
		return full()
	}
	step := old.series.Step
	start := from - from%step + step
	end := until - until%step + step
	cutoff := now - int64(c.mutable/time.Second)
	prefixEnd := min(old.series.UntilTime, end, int(cutoff-cutoff%int64(step)))
	if start < old.series.FromTime || start >= prefixEnd {
		return full()
	}
	first := (start - old.series.FromTime) / step
	count := (prefixEnd - start) / step
	if first < 0 || first+count > len(old.series.Values) {
		return full()
	}
	var result *store.Series
	if prefixEnd == end {
		result = &store.Series{Metadata: m, FromTime: start, UntilTime: end, Step: step, Values: make([]float64, count)}
	} else {
		// Whisper's exclusive-from interval rounds up by one step.
		tail, e := db.Fetch(ctx, name, prefixEnd-step, until)
		if e != nil {
			return nil, 0, e
		}
		if tail == nil || tail.Step != step || tail.FromTime != prefixEnd || tail.UntilTime != end ||
			!sameRenderRevision(m, tail.Metadata) || db.ReadCacheEpoch() != epoch {
			return full()
		}
		result = &store.Series{Metadata: tail.Metadata, FromTime: start, UntilTime: end, Step: step, Values: make([]float64, count+len(tail.Values))}
		copy(result.Values[count:], tail.Values)
	}
	copy(result.Values, old.series.Values[first:first+count])
	// A slow fetch can cross a retention boundary. Recheck the original
	// range, since the shorter tail would still select the finest archive.
	current := c.now().Unix()
	if current < now || !finestRenderRange(m, from, until, current) || db.ReadCacheEpoch() != epoch {
		return full()
	}
	cutoff = current - int64(c.mutable/time.Second)
	if old.series.UntilTime < min(result.UntilTime, int(cutoff-cutoff%int64(step))) {
		c.put(result, epoch, current)
	}
	return result, count, nil
}

func (listener *CarbonserverListener) fetchMetricStoreSeries(ctx context.Context, db *store.Store, name string, from, until int) (*store.Series, error) {
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
