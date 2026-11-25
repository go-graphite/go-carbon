package cache

import (
	"sync/atomic"
	"unsafe"
	"weak"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// Wire-id slices own shared series identities. A direct item hint is valid only
// under the shard lock and is cleared before removal, replacement or rehash.
// The item holds a weak reverse edge, so neither removed batches nor abandoned
// identities live longer because of this shortcut.
type StreamSeries struct {
	name  string
	cache *Cache
	hash  uint64
	hint  atomic.Uint64
	item  *item // protected by the owning cache shard lock
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
	if series.cache != c {
		c.AddPoint(series.StreamName(), value, ts)
		return
	}
	if !c.tryStreamPoint(series, value, ts) {
		c.refreshStreamPoint(series, value, ts)
	}
}

// Dictionary points enter one warm path. Resolution owns the
// canonical name; a point reads its direct item hint under the shard lock.
// Non-owning identities retain the numeric generation check. True means
// the point was handled (including quota refusal); false needs cold admission.
func (c *Cache) tryStreamPoint(series *StreamSeries, value float64, ts int64) bool {
	h := series.hint.Load()
	if h == 0 {
		return false
	}
	sh := c.data[series.hash&(shardCount-1)]
	sh.mu.Lock()
	it := series.item
	if it == nil {
		slot := uint32(h&0xffffff) - 1
		if h != 0 && int(slot) < len(sh.items.slots) {
			candidate := sh.items.slots[slot].item
			if candidate != nil && candidate.streamGeneration == h>>24 {
				it = candidate
				if it.streamSeries == (weak.Pointer[StreamSeries]{}) {
					bindStreamItem(series, it)
				}
			}
		}
	}
	if it != nil {
		s := c.settings.Load().(*cacheSettings)
		if s.xlog == nil && !s.tagsEnabled {
			handled := c.addPointLocked(sh, s, it, series.hash, value, ts)
			sh.mu.Unlock()
			return handled
		}
	}
	sh.mu.Unlock()
	return false
}

// A cache item tracks at most one direct stream identity. Other identities keep
// the generation-checked numeric hint. Removal clears both links under mu,
// before the item can be decoded or handed to persistence. Rehash clears the
// shortcut too, preserving the numeric hint's original refresh boundaries.
func bindStreamItem(series *StreamSeries, it *item) {
	if series.hint.Load() == 0 {
		return
	}
	// Make reuses the weak handle already registered by ResolveStreamName.
	// The reverse edge must remain weak: queued items must not extend a
	// dictionary identity's lifetime after its final connection releases it.
	ref := weak.Make(series)
	if it.streamSeries == ref || it.streamSeries.Value() == nil {
		it.streamSeries = ref
		series.item = it
	}
}

func (it *item) unbindStreamItem() {
	if s := it.streamSeries.Value(); s != nil {
		s.item = nil
	}
	it.streamSeries = weak.Pointer[StreamSeries]{}
}

// Cold admission publishes hints only for the exact canonical name/hash,
// excluding normalized tags and preserving ordinary recovery/admission behavior.
type streamPointHint struct {
	name   string
	hash   uint64
	hint   *atomic.Uint64
	series *StreamSeries
}

// Keep cold admission outside the warm frame. Ordinary insertion claims recovery
// and publishes the hint before unlocking, including for an existing rejected item.
func (c *Cache) refreshStreamPoint(series *StreamSeries, value float64, ts int64) {
	binding := streamPointHint{name: series.name, hash: series.hash, hint: &series.hint, series: series}
	c.addWaiting(points.OnePoint(series.name, value, ts), false, false, &binding)
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

// bindStreamHint publishes an insertion/lookup result while the owning shard
// is still locked. A rehash or eviction later invalidates the generation check.
func (sh *Shard) bindStreamHint(hint *atomic.Uint64, it *item, slot int) {
	generation := sh.streamIdentity(it)
	if generation != 0 && generation < 1<<40 && slot < 0xffffff {
		hint.Store(generation<<24 | uint64(slot+1))
	} else {
		hint.Store(0)
	}
}
