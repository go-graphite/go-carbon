package cache

import "github.com/zeebo/xxh3"

// PointRef is a connection-owned hint for adding points to one metric. Its zero
// value is ready to use. A ref must not be used concurrently. Reset it to
// PointRef{} before assigning its connection ID to a different name, or when
// releasing that ID: the hint does not retain removed batches or their names.
// Cache removals and replacements do not require the caller to reset it.
type PointRef struct {
	cache      *Cache
	generation uint64
	shard      uint32
	slot       uint32
}

// AddPointRef adds a point with the same admission and bookkeeping as AddPoint.
// name must identify the same metric for the lifetime of ref (until it is reset).
// It may alias a reused input buffer; the cache never retains that buffer.
// A nil ref behaves as AddPoint. Tags and graceful diversion use AddPoint too.
func (c *Cache) AddPointRef(ref *PointRef, name []byte, value float64, timestamp int64) {
	if ref == nil {
		c.AddPoint(name, value, timestamp)
		return
	}
	s := c.settings.Load().(*cacheSettings)
	if s.xlog != nil || s.tagsEnabled {
		*ref = PointRef{}
		c.AddPoint(name, value, timestamp)
		return
	}
	if ref.cache == c && ref.generation != 0 {
		shard := c.data[ref.shard]
		shard.mu.Lock()
		if uint(ref.slot) < uint(len(shard.items.slots)) {
			values := shard.items.slots[ref.slot].item
			if values != nil && values.streamGeneration == ref.generation && c.settings.Load().(*cacheSettings).xlog == nil {
				handled := c.addPointLocked(shard, s, values, values.hash, value, timestamp)
				shard.mu.Unlock()
				if !handled {
					c.AddPoint(name, value, timestamp)
				}
				return
			}
		}
		shard.mu.Unlock()
	}

	// This is deliberately the ordinary path: in particular, a cache miss
	// must claim pending recovery before it applies live quotas or inserts.
	*ref = PointRef{}
	c.AddPoint(name, value, timestamp)
	hash := xxh3.Hash(name)
	index := uint32(hash & (shardCount - 1))
	shard := c.data[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	values := shard.items.getBytesHashed(hash, name)
	if values == nil {
		return
	}
	// Resolve the slot only when refreshing. A warm hit reads it directly,
	// without hashing, probing or comparing the name. The lock also covers
	// any table growth between AddPoint and this refresh.
	mask := len(shard.items.slots) - 1
	for slot := int(hash>>shardBits) & mask; ; slot = (slot + 1) & mask {
		if shard.items.slots[slot].item == values {
			*ref = PointRef{cache: c, generation: shard.streamIdentity(values), shard: index, slot: uint32(slot)}
			return
		}
	}
}

// AddStreamPoint binds a stream dictionary slot to this cache. Dictionary resets
// and inherited stream snapshots supply empty slots, so references never cross
// name assignments or cache lifetimes. The normal AddPointRef fallback retains
// admission, tag handling and graceful input diversion.
func (c *Cache) AddStreamPoint(name []byte, value float64, timestamp int64, slot *any) {
	if slot != nil {
		if series, ok := (*slot).(*StreamSeries); ok {
			c.addStreamSeries(series, value, timestamp)
			return
		}
	}
	if slot == nil {
		c.AddPoint(name, value, timestamp)
		return
	}
	ref, ok := (*slot).(*PointRef)
	if !ok {
		ref = new(PointRef)
		*slot = ref
	}
	c.AddPointRef(ref, name, value, timestamp)
}
