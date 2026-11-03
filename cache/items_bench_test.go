package cache

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"unsafe"

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
				probes += ((i - (int(slot.item.hash>>shardBits) & mask)) & mask) + 1
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
			cpuStart := compactCPUTime()
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
			b.ReportMetric(float64(compactCPUTime()-cpuStart)/float64(b.N), "cpu-ns/lookup")
			b.ReportMetric(float64(probes)/float64(n), "probes/lookup")
			b.ReportMetric(float64(n)/float64(slots), "load")
			b.ReportMetric(float64(nameBytes)/float64(n), "name-B")
		})
	}
}

// This is an exact capacity plan for insert-only itemMap tables, not a heap
// benchmark or lookup timing result. Keep the same names as the 5M bookkeeping
// fixture so the actual per-shard occupancy determines growth thresholds.
func BenchmarkItemMapCapacityPlan(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("CACHE_ITEM_MAP_PLAN"))
	if n == 0 {
		b.Skip("set CACHE_ITEM_MAP_PLAN=5000000 or 8070000")
	}
	for range b.N {
		peak, _ := strconv.Atoi(os.Getenv("CACHE_ITEM_MAP_PEAK"))
		if peak < n {
			peak = n
		}
		var counts [shardCount]int
		prefix := strings.Repeat("namespace.component.", 8)
		for i := 0; i < peak; i++ {
			name := fmt.Sprintf("%smetric.%07d", prefix, i)
			counts[xxh3.HashString(name)&(shardCount-1)]++
		}
		var half, dense, smaller int
		for _, count := range counts {
			a, z := 8, 8
			for count*2 > a {
				a *= 2
			}
			for count*8 > z*5 {
				z *= 2
			}
			half += a
			dense += z
			if z < a {
				smaller++
			}
		}
		bytes := float64(unsafe.Sizeof(itemSlot{})) + 1 // pointer slot plus control
		b.ReportMetric(float64(half)*bytes/float64(n), "half-B/metric")
		b.ReportMetric(float64(dense)*bytes/float64(n), "dense-B/metric")
		b.ReportMetric(float64(half-dense)*bytes/float64(n), "saved-B/metric")
		b.ReportMetric(float64(smaller), "smaller-shards")
		b.ReportMetric(float64(half-dense)*bytes/1e9, "saved-GB")
	}
}
