package cache

import (
	"iter"
	"slices"
)

// TrackRestorePrefix snapshots queued and in-flight history after every older
// source has been admitted and before live input opens. The persister serializes
// each metric from pop through confirmation, so later live batches cannot settle
// this prefix ahead of history. Failed writes requeue without confirming.
// The startup owner calls this once per cache; it must finish before readers
// or live writers can observe RestorePending.
// The returned, repeatable names include in-flight batches and remain available
// after confirmation. Index preparation unions them with a later store catalog,
// avoiding a gap when a metric leaves the cache during catalog loading.
func (c *Cache) TrackRestorePrefix() iter.Seq[string] {
	var names []string
	for _, shard := range c.data {
		shard.mu.Lock()
		prefix := make(map[string]int64)
		for _, slot := range shard.items.slots {
			if it := slot.item; it != nil {
				prefix[it.Metric] += int64(it.count())
			}
		}
		for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
			if p != nil {
				prefix[p.Metric] += int64(len(p.Data))
			}
		}
		for name, n := range prefix {
			names = append(names, name)
			c.restoreOutstanding.Add(n)
		}
		if len(prefix) != 0 {
			shard.restorePrefix = prefix
		}
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
	return slices.Values(names)
}

func (c *Cache) confirmRestoreLocked(shard *Shard, name string, count int) {
	if remaining := shard.restorePrefix[name]; remaining > 0 {
		n := min(remaining, int64(count))
		if remaining == n {
			delete(shard.restorePrefix, name)
			if len(shard.restorePrefix) == 0 {
				shard.restorePrefix = nil
			}
		} else {
			shard.restorePrefix[name] -= n
		}
		c.restoreOutstanding.Add(-n)
	}
}

// RestorePending is independent of newer live traffic and includes in-flight
// history. A read caller must check it before reading the store, not afterwards.
func (c *Cache) RestorePending(name string) bool {
	if c.restoreOutstanding.Load() == 0 {
		return false
	}
	shard := c.GetShard(name)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.restorePrefix[name] > 0
}

func (c *Cache) RestoreOutstanding() int64 { return c.restoreOutstanding.Load() }
