package cache

import "github.com/go-graphite/go-carbon/points"

// SnapshotLogShard streams unpersisted queued and in-flight points directly to
// the journal, bypassing the bounded live staging buffer. Persistence must be
// stopped. Input on this shard waits until its snapshot is encoded and synced;
// later points then belong to gen and follow that snapshot in the journal.
func (c *Cache) SnapshotLogShard(index int, gen uint64, emit func(*points.Points) error) error {
	shard := c.data[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	var scratch points.Points
	if err := shard.visitLocked(&scratch, emit); err != nil {
		return err
	}
	shard.log.entries.Reset()
	shard.log.gen = gen
	shard.log.items = make(map[uint64]int32)
	// Existing batches were written by the preceding snapshot generation;
	// only input accepted after this shard unlocks belongs to gen.
	first := gen - 1
	shard.log.oldest = first
	for _, it := range shard.items.all {
		it.logFirst = first
		it.setFlag(itemLogTracked, true)
		shard.log.items[first]++
	}
	for batch := range shard.log.first {
		shard.log.first[batch] = first
		shard.log.items[first]++
	}
	shard.log.wakeRestore()
	c.syncMemoryLocked(shard)
	return nil
}
