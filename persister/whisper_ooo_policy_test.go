package persister

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

func TestStoreOutOfOrderPointPolicy(t *testing.T) {
	dir := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, dir, cache)
	p.outOfOrder.ticker.Stop()
	p.outOfOrder.ticker = &helper.ThrottleTicker{C: make(chan bool, 10)}
	p.SetOutOfOrderCompactionPolicy(2, 2*time.Hour, time.Minute)
	p.outOfOrder.threshold = 1 // physical allocation must not force a merge
	const metric = "test.batched"
	path := filepath.Join(dir, "test", "batched.wsp")
	base := time.Now().Unix() - 3600
	for _, offset := range []int64{0, 2, 4} {
		cache.add(metric, base+offset, 1)
	}
	p.store(metric)
	cache.add(metric, base+1, 7)
	// No rate token: do not even scan the sidecar.
	p.store(metric)
	if p.oooCompactionChecks != 0 || p.oooCompactions != 0 {
		t.Fatal("checked or compacted without rate budget")
	}
	p.outOfOrder.ticker.C <- true
	cache.add(metric, base+6, 1)
	p.store(metric)
	if p.oooCompactionChecks != 1 || p.oooCompactionDeferred != 1 || p.oooCompactions != 0 {
		t.Fatalf("one pending record: checks=%d deferred=%d merges=%d", p.oooCompactionChecks, p.oooCompactionDeferred, p.oooCompactions)
	}
	if got := fetchValue(t, path, int(base)+1); got != 7 {
		t.Fatalf("pending value = %v", got)
	}
	// A duplicate timestamp across separate opens is still one live record.
	p.outOfOrder.ticker.C <- true
	cache.add(metric, base+1, 8)
	p.store(metric)
	if p.oooCompactions != 0 {
		t.Fatal("duplicate inflated pending count")
	}
	// Volume must trigger independently of the old physical threshold.
	p.outOfOrder.threshold = 1 << 40
	p.outOfOrder.ticker.C <- true
	cache.add(metric, base+3, 9)
	p.store(metric)
	if p.oooCompactions != 1 || p.oooCompactErrors != 0 {
		t.Fatalf("merges=%d errors=%d", p.oooCompactions, p.oooCompactErrors)
	}
	if _, err := os.Stat(path + ".ooo"); !os.IsNotExist(err) {
		t.Fatalf("sidecar after merge: %v", err)
	}
	if got := fetchValue(t, path, int(base)+1); got != 8 {
		t.Fatalf("merged duplicate value = %v", got)
	}
	if got := fetchValue(t, path, int(base)+3); got != 9 {
		t.Fatalf("merged new value = %v", got)
	}
	stats := map[string]float64{}
	p.Stat(func(name string, value float64) { stats[name] = value })
	if stats["oooCompactionChecks"] != 3 || stats["oooCompactionDeferred"] != 2 {
		t.Fatalf("stats: %v", stats)
	}
	p.Stat(func(name string, value float64) { stats[name] = value })
	if stats["oooCompactionChecks"] != 0 || stats["oooCompactionDeferred"] != 0 {
		t.Fatal("interval counters did not reset")
	}
}

func TestStoreOutOfOrderPointAge(t *testing.T) {
	for _, urgent := range []bool{false, true} {
		t.Run(map[bool]string{false: "sample age", true: "retention margin"}[urgent], func(t *testing.T) {
			dir := t.TempDir()
			cache := &fakeCache{}
			p := newOOOTestPersister(t, dir, cache)
			p.outOfOrder.ticker.Stop()
			p.outOfOrder.ticker = &helper.ThrottleTicker{C: make(chan bool, 1)}
			p.SetOutOfOrderCompactionPolicy(100, time.Minute, time.Minute)
			if urgent {
				p.SetOutOfOrderCompactionPolicy(100, 2*time.Hour, 2*time.Hour)
			}
			base := time.Now().Unix() - 3600
			cache.add("age.test", base, 1)
			cache.add("age.test", base+2, 1)
			p.store("age.test")
			p.outOfOrder.ticker.C <- true
			cache.add("age.test", base+1, 7)
			p.store("age.test")
			if p.oooCompactions != 1 {
				t.Fatalf("merges=%d", p.oooCompactions)
			}
		})
	}
}
