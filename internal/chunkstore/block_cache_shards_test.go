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
		{1792 << 20, 192, 448},
		{1792 << 20, 4, 16},
		{1792 << 20, 1, 4},
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
