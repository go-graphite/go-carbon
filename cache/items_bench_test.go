package cache

import (
	"testing"

	"github.com/zeebo/xxh3"
)

// The received-name case models a receiver's reused buffer. The interned case
// shows the upper bound from already owning the cache's canonical string;
// this benchmark deliberately excludes shard-lock and quota-accounting costs.
func BenchmarkItemMapResident(b *testing.B) {
	c, names, n := largeIngestCache(b)
	var probes, slots, nameBytes int
	for _, shard := range c.data {
		slots += len(shard.items.slots)
		mask := len(shard.items.slots) - 1
		for i, slot := range shard.items.slots {
			if slot.item != nil {
				probes += ((i - (int(slot.hash>>shardBits) & mask)) & mask) + 1
				nameBytes += len(slot.item.Metric)
			}
		}
	}
	for _, interned := range []bool{false, true} {
		name := "received"
		if interned {
			name = "interned"
		}
		b.Run(name, func(b *testing.B) {
			var buf [160]byte
			state := uint64(1)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				state = state*6364136223846793005 + 1
				metric := names[(state>>32)%uint64(n)]
				var it *item
				if interned {
					hash := xxh3.HashString(metric)
					it = c.data[hash&(shardCount-1)].items.getHashed(hash, metric)
				} else {
					copy(buf[:], metric)
					key := buf[:len(metric)]
					hash := xxh3.Hash(key)
					it = c.data[hash&(shardCount-1)].items.getBytesHashed(hash, key)
				}
				if it == nil {
					b.Fatal("resident metric missing")
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(probes)/float64(n), "probes/lookup")
			b.ReportMetric(float64(n)/float64(slots), "load")
			b.ReportMetric(float64(nameBytes)/float64(n), "name-B")
		})
	}
}
