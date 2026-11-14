package cache

import (
	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// RecoveryMetric owns a name and its hash, never a cache item or slot. It stays
// valid across eviction/rehash while a bounded replay batch waits for admission.
type RecoveryMetric struct {
	name     string
	hash     uint64
	overhead int64
}

func (m RecoveryMetric) Name() string         { return m.name }
func (m RecoveryMetric) OverheadBytes() int64 { return m.overhead }

// ResolveRecoveryMetric combines conservative active-map pricing and canonical
// name resolution in one lookup. Admission must reserve the returned overhead
// before AddRecoveryMetric; the usual headroom covers concurrent eviction.
func (c *Cache) ResolveRecoveryMetric(name []byte) RecoveryMetric {
	hash := xxh3.Hash(name)
	sh := c.data[hash&(shardCount-1)]
	sh.mu.RLock()
	it := sh.items.getBytesHashed(hash, name)
	if it != nil {
		canonical := it.Metric
		sh.mu.RUnlock()
		return RecoveryMetric{name: canonical, hash: hash}
	}
	sh.mu.RUnlock()
	return RecoveryMetric{name: string(name), hash: hash, overhead: int64(len(name)) + 1024}
}

// AddRecoveryMetric applies an admitted group through ordinary recovery/source
// tracking. Tags still normalize before routing; a precomputed hash is used only
// when normalization leaves the resolved name unchanged.
func (c *Cache) AddRecoveryMetric(m RecoveryMetric, data []points.Point, source *RecoverySource, live bool) {
	p := points.Points{Metric: m.name, Data: data}
	c.addWithRestoreHashed(&p, !live, true, false, nil, source, &m)
}
