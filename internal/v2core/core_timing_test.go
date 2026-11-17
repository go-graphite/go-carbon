package v2core

import (
	"sync"
	"testing"
	"time"
)

func TestCoreSealTimingPreservesPublication(t *testing.T) {
	var mu sync.Mutex
	phases := map[string]time.Duration{}
	e, err := openCoreWithHooks(t.TempDir(), coreConfig{Shards: 1, Producers: 1, MaxRows: 16, PartitionSeconds: 3600}, coreHooks{SealTiming: func(phase string, d time.Duration) { mu.Lock(); defer mu.Unlock(); phases[phase] += d }})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, []row{{ID: 1, Time: 1, Bits: 42, Seq: 1}}, 1, true); err != nil {
		t.Fatal(err)
	}
	if e.durableFrontiers()[0][0] != 1 {
		t.Fatal("timing observer changed publication frontier")
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if phases["seal_total"] <= 0 || phases["publication"] <= 0 || phases["seal_total"] < phases["rollup_plan"]+phases["publication"] {
		t.Fatalf("invalid phase durations: %v", phases)
	}
}
