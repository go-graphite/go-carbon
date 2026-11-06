package cache

import (
	"sync/atomic"
	"unsafe"
	"weak"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// A series identity outlives queued batches without retaining any removed batch.
// Wire-id slices hold these shared identities; only cache insertion/replacement
// refreshes the atomic hint. The per-point path takes only the usual cache lock.
type StreamSeries struct {
	name  string
	cache *Cache
	hash  uint64
	hint  atomic.Uint64
}
type seriesSlot struct {
	hash uint64
	ref  weak.Pointer[StreamSeries]
}

// Keep this an independently bounded identity cache, rather than sizing it from
// live cache items: dictionaries may outlive those items, and distinct dead names
// must not cause unbounded growth. At cap, replacement needs no liveness scan.
const maxStreamSeriesSlots = 8192 // 128 MiB across 1024 shards, allocated lazily

// StreamName is read-only and remains valid while this series is referenced.
func (s *StreamSeries) StreamName() []byte {
	return unsafe.Slice(unsafe.StringData(s.name), len(s.name))
}

// ResolveStreamName shares existing cache/index name storage. It is called only
// for validated definitions, never per point. Weak slots do not keep names or
// batches alive after the last connection/cache reference disappears.
func (c *Cache) ResolveStreamName(name []byte) any {
	hash := xxh3.Hash(name)
	sh := c.data[hash&(shardCount-1)]
	// Ingestion never takes seriesMu. Weak-pointer promotion and index growth
	// must not hold mu: a cold weak reference can fault in heap span metadata.
	sh.seriesMu.Lock()
	defer sh.seriesMu.Unlock()
	size := len(sh.streamSeries)
	if size == 0 {
		size = 16
	} else if sh.streamSeriesUsed >= size/2 && size < maxStreamSeriesSlots {
		size *= 2
	}
	if size != len(sh.streamSeries) {
		old := sh.streamSeries
		sh.streamSeries = make([]seriesSlot, size)
		sh.streamSeriesUsed = 0
		for _, slot := range old {
			if slot.ref != (weak.Pointer[StreamSeries]{}) {
				sh.putStreamSeries(slot)
			}
		}
		sh.streamSeriesBytes.Store(int64(size) * int64(unsafe.Sizeof(seriesSlot{})))
	}
	mask := len(sh.streamSeries) - 1
	free := -1
	for step := 0; step < min(64, len(sh.streamSeries)); step++ {
		i := (int(hash>>shardBits) + step) & mask
		slot := &sh.streamSeries[i]
		if slot.ref == (weak.Pointer[StreamSeries]{}) {
			free = i
			break
		}
		// Most probes have a different hash. Do not promote those weak
		// references, even to look for dead slots. Exact names still resolve
		// collisions, including a zero hash.
		if slot.hash == hash {
			existing := slot.ref.Value()
			if existing != nil && existing.name == string(name) {
				return existing
			}
			if existing == nil && free < 0 {
				free = i
			}
		}
	}
	// Only borrowing the canonical name needs the cache lock. No weak-pointer
	// work or index allocation happens in this critical section. Lock order is
	// seriesMu -> mu; the cache never takes seriesMu while holding mu.
	sh.mu.RLock()
	metric := ""
	if it := sh.items.getBytesHashed(hash, name); it != nil {
		metric = it.Metric
	}
	sh.mu.RUnlock()
	if metric == "" {
		metric = string(name)
	}
	series := &StreamSeries{name: metric, cache: c, hash: hash}
	if free < 0 {
		free = int(hash>>shardBits) & mask
	}
	if sh.streamSeries[free].ref == (weak.Pointer[StreamSeries]{}) {
		sh.streamSeriesUsed++
	}
	sh.streamSeries[free] = seriesSlot{hash, weak.Make(series)}
	return series
}

// putStreamSeries bounds all probes, including growth. At capacity this index
// is an identity cache, not an ownership map: replacing a live or dead weak slot
// may forgo sharing, but cannot change point admission or retain a name. Growing
// copies hashes and weak handles without touching any referent's heap metadata.
func (sh *Shard) putStreamSeries(slot seriesSlot) {
	mask := len(sh.streamSeries) - 1
	home := int(slot.hash>>shardBits) & mask
	for step := 0; step < min(64, len(sh.streamSeries)); step++ {
		i := (home + step) & mask
		if sh.streamSeries[i].ref == (weak.Pointer[StreamSeries]{}) {
			sh.streamSeries[i] = slot
			sh.streamSeriesUsed++
			return
		}
		if sh.streamSeries[i].hash == slot.hash {
			// Also reuses expired entries without a separate liveness probe.
			sh.streamSeries[i] = slot
			return
		}
	}
	sh.streamSeries[home] = slot
}

func (c *Cache) addStreamSeries(series *StreamSeries, value float64, ts int64) {
	settings := c.settings.Load().(*cacheSettings)
	if settings.xlog != nil || settings.tagsEnabled || series.cache != c {
		c.AddPoint(series.StreamName(), value, ts)
		return
	}
	sh := c.data[series.hash&(shardCount-1)]
	if hint := series.hint.Load(); hint != 0 {
		slot := uint32(hint&0xffffff) - 1
		generation := hint >> 24
		sh.mu.Lock()
		if int(slot) < len(sh.items.slots) {
			it := sh.items.slots[slot].item
			if it != nil && it.streamGeneration == generation && c.settings.Load().(*cacheSettings).xlog == nil {
				c.addPointLocked(sh, settings, it, series.hash, value, ts)
				sh.mu.Unlock()
				return
			}
		}
		sh.mu.Unlock()
	}

	// The ordinary insertion path preserves quota, recovery, tags, throttling and
	// graceful diversion, while passing the existing immutable name allocation.
	c.addWithRestore(points.OnePoint(series.name, value, ts), false, false, false, series, nil)
}

// StreamSeriesIndexBytes reports the fixed-width weak index capacity. Series
// identities are held by decoder slots, and their names alias cache storage.
func (c *Cache) StreamSeriesIndexBytes() int {
	n := 0
	for _, sh := range c.data {
		n += int(sh.streamSeriesBytes.Load())
	}
	return n
}

// streamIdentity is protected by the shard lock. A number never identifies two
// batches, so a stale slot cannot pin or alias a removed/reallocated item.
func (sh *Shard) streamIdentity(it *item) uint64 {
	if it.streamGeneration == 0 && sh.streamGeneration != ^uint64(0) {
		sh.streamGeneration++
		it.streamGeneration = sh.streamGeneration
	}
	return it.streamGeneration
}

// bindStreamSeries publishes an insertion/lookup result while the owning shard
// is still locked. A rehash or eviction later invalidates the generation check.
func (sh *Shard) bindStreamSeries(series *StreamSeries, it *item, slot int) {
	generation := sh.streamIdentity(it)
	if generation != 0 && generation < 1<<40 && slot < 0xffffff {
		series.hint.Store(generation<<24 | uint64(slot+1))
	} else {
		series.hint.Store(0)
	}
}
