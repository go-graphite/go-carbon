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
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.streamSeries) == 0 {
		sh.streamSeries = make([]seriesSlot, 16)
	}
	if sh.streamSeriesNew >= len(sh.streamSeries)/2 {
		old := sh.streamSeries
		live := 0
		for _, slot := range old {
			if slot.ref.Value() != nil {
				live++
			}
		}
		size := len(old)
		if live*2 >= size {
			size = min(maxStreamSeriesSlots, size*2)
		}
		sh.streamSeries = make([]seriesSlot, size)
		sh.streamSeriesNew = 0
		for _, slot := range old {
			if slot.ref.Value() == nil {
				continue
			}
			mask := size - 1
			for step := 0; step < min(64, size); step++ {
				i := (int(slot.hash>>shardBits) + step) & mask
				if sh.streamSeries[i].hash == 0 || step == min(64, size)-1 {
					sh.streamSeries[i] = slot
					break
				}
			}
		}
	}
	mask := len(sh.streamSeries) - 1
	free := -1
	for step := 0; step < min(64, len(sh.streamSeries)); step++ {
		i := (int(hash>>shardBits) + step) & mask
		slot := &sh.streamSeries[i]
		existing := slot.ref.Value()
		if existing != nil && slot.hash == hash && existing.name == string(name) {
			return existing
		}
		if existing == nil && free < 0 {
			free = i
		}
		if slot.hash == 0 {
			break
		}
	}
	if free < 0 {
		if len(sh.streamSeries) < maxStreamSeriesSlots {
			old := sh.streamSeries
			sh.streamSeries = make([]seriesSlot, len(old)*2)
			mask = len(sh.streamSeries) - 1
			for _, slot := range old {
				if slot.ref.Value() == nil {
					continue
				}
				for i := int(slot.hash>>shardBits) & mask; ; i = (i + 1) & mask {
					if sh.streamSeries[i].hash == 0 {
						sh.streamSeries[i] = slot
						break
					}
				}
			}
			for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
				if sh.streamSeries[i].hash == 0 {
					free = i
					break
				}
			}
		} else {
			free = int(hash>>shardBits) & mask
		}
	}
	metric := ""
	if it := sh.items.getBytesHashed(hash, name); it != nil {
		metric = it.Metric
	}
	if metric == "" {
		metric = string(name)
	}
	sh.streamSeriesNew++
	series := &StreamSeries{name: metric, cache: c, hash: hash}
	sh.streamSeries[free] = seriesSlot{hash, weak.Make(series)}
	return series
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
	c.add(points.OnePoint(series.name, value, ts), false, false)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it := sh.items.getBytesHashed(series.hash, series.StreamName())
	if it == nil {
		series.hint.Store(0)
		return
	}
	mask := len(sh.items.slots) - 1
	for slot := int(series.hash>>shardBits) & mask; ; slot = (slot + 1) & mask {
		if sh.items.slots[slot].item == it {
			generation := sh.streamIdentity(it)
			if generation != 0 && generation < 1<<40 && slot < 0xffffff {
				series.hint.Store(generation<<24 | uint64(slot+1))
			} else {
				series.hint.Store(0)
			}
			return
		}
	}
}

// StreamSeriesIndexBytes reports the fixed-width weak index capacity. Series
// identities are held by decoder slots, and their names alias cache storage.
func (c *Cache) StreamSeriesIndexBytes() int {
	n := 0
	for _, sh := range c.data {
		sh.mu.RLock()
		n += cap(sh.streamSeries) * 16
		sh.mu.RUnlock()
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
