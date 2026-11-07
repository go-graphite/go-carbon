package cache

import "github.com/zeebo/xxh3"

// RecoveryMetricName returns an owned name for a borrowed recovery record.
// Reuse the active item's allocation when it exists; the returned string stays
// valid even if that item is concurrently popped or evicted before insertion.
func (c *Cache) RecoveryMetricName(name []byte) string {
	hash := xxh3.Hash(name)
	shard := c.data[hash&(shardCount-1)]
	shard.mu.RLock()
	item := shard.items.getBytesHashed(hash, name)
	if item != nil {
		canonical := item.Metric
		shard.mu.RUnlock()
		return canonical
	}
	shard.mu.RUnlock()
	return string(name)
}
