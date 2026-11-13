package carbon

import "testing"

// Zero store-compaction-workers scales with the CPU count: 32 on 192 CPUs,
// 10 on 64, never below 4 or above Pebble's validated 64. Explicit values win.
func TestStoreCompactionWorkersDefault(t *testing.T) {
	for _, tc := range []struct{ configured, cpus, want int }{
		{0, 1, 4}, {0, 8, 4}, {0, 32, 5}, {0, 64, 10}, {0, 192, 32}, {0, 384, 64}, {0, 1024, 64},
		{1, 192, 1}, {16, 192, 16}, {64, 8, 64},
	} {
		if got := storeCompactionWorkers(tc.configured, tc.cpus); got != tc.want {
			t.Errorf("storeCompactionWorkers(%d, %d) = %d, want %d", tc.configured, tc.cpus, got, tc.want)
		}
	}
}
