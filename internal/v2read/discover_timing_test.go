package v2read

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"runtime/metrics"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestDiscoverRestartTiming times the startup name sweep over a reopened
// store: V2_DISCOVER_NAMES names, V2_DISCOVER_SEALS sealed rounds, 16 owners.
func TestDiscoverRestartTiming(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("V2_DISCOVER_NAMES"))
	seals, _ := strconv.Atoi(os.Getenv("V2_DISCOVER_SEALS"))
	if n == 0 || seals == 0 {
		t.Skip("set V2_DISCOVER_NAMES and V2_DISCOVER_SEALS")
	}
	const owners = 16
	// V2_DISCOVER_DIR keeps the store, so both builds time the same files.
	dir := os.Getenv("V2_DISCOVER_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	_, statErr := os.Stat(dir + "/built")
	built := statErr == nil
	cfg := testConfig()
	cfg.Shards, cfg.Owners = owners, owners
	open := func() (*Reader, *v2core.Engine) {
		r, err := New(owners, 256<<20, 16<<20)
		if err != nil {
			t.Fatal(err)
		}
		r.Identities = &v2core.PolicyCatalog{}
		e, err := v2core.Open(dir, cfg, v2core.Hooks{Identities: r.Identities, PublishBatchesWithInventory: r.PublishWithInventory})
		if err != nil {
			t.Fatal(err)
		}
		return r, e
	}
	r, e := open()
	if built {
		r.Close()
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	policies := make([][]byte, n)
	for i := range n {
		var err error
		policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(fmt.Sprintf("restart.host%04d.metric.%07d", i%1000, i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	order := uint64(0)
	for s := range seals * btoi(!built) {
		batches := make([]v2core.Batch, 0, n)
		for i := range n {
			order++
			batches = append(batches, v2core.Batch{ID: uint64(i + 1), Order: order, Clock: int64(100000 + 10*s), Policy: policies[i], Points: []v2core.Row{{ID: uint64(i + 1), Time: int64(100000 + 10*s), Bits: order, Seq: order}}})
		}
		if err := e.AppendBatches(0, batches, uint64(s+1), true); err != nil {
			t.Fatal(err)
		}
	}
	if !built {
		r.Close()
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dir+"/built", nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	r, e = open()
	opened := time.Since(start)
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Allocation and GC CPU of discovery alone, the costs that scale with names.
	sample := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}, {Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(sample)
	alloc0, gc0 := sample[0].Value.Uint64(), sample[1].Value.Float64()
	start = time.Now()
	names, masks, err := r.NamesWithMasks(context.Background())
	took := time.Since(start)
	metrics.Read(sample)
	allocPerName, gcCPU := (sample[0].Value.Uint64()-alloc0)/uint64(max(n, 1)), sample[1].Value.Float64()-gc0
	if err != nil || len(names) != n {
		t.Fatalf("names=%d err=%v", len(names), err)
	}
	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s %v\n", name, masks[name])
	}
	t.Logf("DISCOVER names=%d seals=%d owners=%d open=%s discover=%s alloc=%dB/name gc_cpu=%.2fs digest=%x", n, seals, owners, opened.Round(time.Millisecond), took.Round(time.Millisecond), allocPerName, gcCPU, h.Sum(nil)[:6])
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
