package v2index

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

func burstNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("svc.host%05d.metric%03d.value%02d", i%50000, (i/50000)%1000, i%97)
	}
	rand.New(rand.NewSource(7)).Shuffle(n, func(i, j int) { names[i], names[j] = names[j], names[i] })
	return names
}

// TestCatalogBurstReport drives the burst a fresh store produces. BURST_NAMES
// sets the size; BURST_CHUNK the names fed to one Apply (0 = whole backlog).
func TestCatalogBurstReport(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("BURST_NAMES"))
	if n == 0 {
		t.Skip("set BURST_NAMES")
	}
	chunk, _ := strconv.Atoi(os.Getenv("BURST_CHUNK"))
	if chunk == 0 {
		chunk = n
	}
	names := burstNames(n)
	var c Catalog
	var longest, total time.Duration
	rev := uint64(0)
	start := time.Now()
	for i := 0; i < n; i += chunk {
		rev++
		s := time.Now()
		if err := c.Apply(rev, names[i:min(i+chunk, n)], nil); err != nil {
			t.Fatal(err)
		}
		d := time.Since(s)
		total += d
		longest = max(longest, d)
	}
	t.Logf("names=%d chunk=%d applies=%d longest=%v total=%v wall=%v levels=%d len=%d", n, chunk, rev, longest, total, time.Since(start), c.Snapshot().Levels(), c.Snapshot().Len())
}

// A large chunked burst never makes one publication long, and every name
// of the capped backlog is visible and identical to a full rebuild.
func TestCatalogChunkedBurstBoundedAndComplete(t *testing.T) {
	const n, chunk = 1_000_000, 64 << 10
	names := burstNames(n)
	var c Catalog
	var longest time.Duration
	for i, rev := 0, uint64(1); i < n; i, rev = i+chunk, rev+1 {
		s := time.Now()
		if err := c.Apply(rev, names[i:min(i+chunk, n)], nil); err != nil {
			t.Fatal(err)
		}
		longest = max(longest, time.Since(s))
	}
	if longest > 2*time.Second {
		t.Fatalf("longest publication %v", longest)
	}
	want := slices.Clone(names)
	slices.Sort(want)
	if s := c.Snapshot(); s.Len() != n || !s.EqualNames(want) {
		t.Fatal("capped backlog not fully visible")
	}
	checkLevels(t, c.Snapshot())
}
