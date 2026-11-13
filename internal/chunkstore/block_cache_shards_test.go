package chunkstore

import "testing"

func TestExactBlockCacheShardsAvoidSmallBudgetCliff(t *testing.T) {
	for _, test := range []struct {
		bytes       int64
		procs, want int
	}{
		{1 << 20, 192, 4},
		{56 << 20, 192, 14},
		{224 << 20, 192, 56},
		{1792 << 20, 192, 7},
		{1792 << 20, 4, 7},
		{1792 << 20, 1, 4},
		{89 << 30, 192, 356},
		{89 << 30, 16, 64},
	} {
		got := exactBlockCacheShards(test.bytes, test.procs)
		if got != test.want {
			t.Fatalf("cache bytes=%d procs=%d: shards=%d want %d", test.bytes, test.procs, got, test.want)
		}
		if got > 4 && test.bytes/int64(got) < 4<<20 {
			t.Fatal("partitioning made a shard smaller than the cache's minimum block budget")
		}
	}
}

func TestLargeBlockCacheShardFitsIngestIndex(t *testing.T) {
	// A 90 GiB combined cache leaves 89 GiB in the primary cache. The old
	// 768-way split at 192 CPUs rejected a 158 MiB index on every lookup.
	const primaryBytes = int64(89 << 30)
	const indexBytes = int64(158 << 20)
	for _, procs := range []int{16, 96, 128, 192, 256} {
		shards := exactBlockCacheShards(primaryBytes, procs)
		if primaryBytes/int64(shards) < indexBytes {
			t.Fatalf("%d CPUs: %d shards cannot retain the ingest index", procs, shards)
		}
	}
}
