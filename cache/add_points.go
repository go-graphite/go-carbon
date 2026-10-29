package cache

import (
	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// AddPoints amortizes lookup and locking for consecutive points of one metric.
// It borrows name and data. Admission, logging and wake-ups remain per point.
func (c *Cache) AddPoints(name []byte, data []points.Point) {
	for len(data) > 0 {
		if len(data) == 1 {
			c.AddPoint(name, data[0].Value, data[0].Timestamp)
			return
		}
		s := c.settings.Load().(*cacheSettings)
		if s.xlog != nil || s.tagsEnabled {
			for _, p := range data {
				c.AddPoint(name, p.Value, p.Timestamp)
			}
			return
		}
		hash := xxh3.Hash(name)
		shard := c.data[hash&(shardCount-1)]
		shard.mu.Lock()
		values := shard.items.getBytesHashed(hash, name)
		if values == nil {
			shard.mu.Unlock()
			c.AddPoint(name, data[0].Value, data[0].Timestamp)
			data = data[1:]
			continue
		}
		for i, p := range data {
			s = c.settings.Load().(*cacheSettings)
			if s.xlog != nil || s.tagsEnabled {
				shard.mu.Unlock()
				for _, p := range data[i:] {
					c.AddPoint(name, p.Value, p.Timestamp)
				}
				return
			}
			c.addPointLocked(shard, s, values, p.Value, p.Timestamp)
		}
		shard.mu.Unlock()
		return
	}
}
