package carbonserver

import (
	"fmt"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func admissionSeries(name string, n int) *store.Series {
	return &store.Series{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 400000}}}},
		FromTime: 1699800000, UntilTime: 1699800000 + n, Step: 1, Values: make([]float64, n)}
}

func TestRenderRangeAdmissionBounded(t *testing.T) {
	l := NewCarbonserverListener(nil)
	if err := l.SetQueryRangeCache(true, 64, time.Second); err != nil {
		t.Fatal(err)
	}
	c := l.queryRangeCache.Load()
	const now = 1700100000
	// Exercise the real 64MiB retained budget with more history than fits.
	for i := range 200 {
		c.put(admissionSeries(fmt.Sprintf("budget.%d", i), 65536), 1, now)
		if c.used > c.limit {
			t.Fatal("exceeded 64MiB budget")
		}
	}
	if c.used < c.limit-(1<<20) {
		t.Fatalf("did not pressure budget: %d", c.used)
	}
	oversize := admissionSeries("oversize", 131072)
	c.put(oversize, 1, now)
	if c.get("oversize") != nil {
		t.Fatal("admitted entry above 1MiB")
	}
	// Covering entries must be rejected before cloning their arrays.
	same := admissionSeries("budget.199", 65536)
	if got := testing.AllocsPerRun(20, func() { c.put(same, 1, now) }); got != 0 {
		t.Fatalf("covered prefix allocated: %v", got)
	}
	// A large admission may not evict an unbounded number of tiny entries.
	if err := l.SetQueryRangeCache(true, 1, time.Second); err != nil {
		t.Fatal(err)
	}
	c = l.queryRangeCache.Load()
	c.limit = 16 << 10
	for i := range 100 {
		c.put(admissionSeries(fmt.Sprintf("small.%d", i), 1), 1, now)
	}
	before, used := len(c.entries), c.used
	large := admissionSeries("large", 1024)
	if got := testing.AllocsPerRun(20, func() { c.put(large, 1, now) }); got != 0 {
		t.Fatalf("rejected admission allocated: %v", got)
	}
	if c.get("large") != nil || len(c.entries) != before || c.used != used {
		t.Fatal("expensive admission partially evicted entries")
	}
}

func TestRenderRangeContentionBypassesCache(t *testing.T) {
	l := NewCarbonserverListener(nil)
	if err := l.SetQueryRangeCache(true, 64, time.Second); err != nil {
		t.Fatal(err)
	}
	c := l.queryRangeCache.Load()
	s := admissionSeries("contended", 4096)
	c.mu.Lock()
	defer c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if c.get(s.Metadata.Name) != nil {
			t.Error("unexpected entry")
		}
		c.put(s, 1, 1700100000)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cache operation queued behind namespace lock")
	}
}
