package v2read

import (
	"fmt"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestSharedBlockCacheServesRepeatReads checks that every owner opens its
// sealed parts on the Reader's shared block cache, and that a repeated cold
// read of one name is served from it instead of decompressing blocks again.
// A per-owner cache sized from the read budget (85 KiB at 96 owners) held no
// 256 KiB dense block, so each render re-read one block per live part.
func TestSharedBlockCacheServesRepeatReads(t *testing.T) {
	const owners = 2
	r, err := New(owners, 1<<20, 64<<10) // per-owner cache limit 16 KiB, as at production scale
	if err != nil {
		t.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	r.BlockCacheBytes = 64 << 20
	cfg := testConfig()
	cfg.Shards, cfg.Owners = owners, owners
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, PublishBatchesWithInventory: r.PublishWithInventory})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	const n = 4000
	names := make([]string, n)
	policies := make([][]byte, n)
	for i := range names {
		names[i] = fmt.Sprintf("cache.host%02d.metric.%05d", i%64, i)
		policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 4000}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	order, cursor := uint64(0), uint64(0)
	for round := int64(0); round < 3; round++ {
		var batches []v2core.Batch
		for i := range names {
			id := uint64(i + 1)
			order++
			ts := 1000 + round*10 + int64(i%7)
			batches = append(batches, v2core.Batch{ID: id, Order: order, Clock: 2000, Policy: policies[i], Points: []v2core.Row{{ID: id, Time: ts, Bits: math.Float64bits(float64(i) + float64(ts)), Seq: order}}})
		}
		cursor++
		if err := e.AppendBatches(0, batches, cursor, true); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		got, err := fetchAll(r, []string{names[0], names[n-1]}, 900, 2000, 2000)
		return err == nil && len(got) == 2 && len(got[names[n-1]].bits) > 0
	})
	if r.blocks == nil {
		t.Fatal("owners did not open parts on the shared block cache")
	}
	name := names[n/2]
	first, err := fetchAll(r, []string{name}, 900, 2000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	m1 := r.blocks.Metrics()
	second, err := fetchAll(r, []string{name}, 900, 2000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	m2 := r.blocks.Metrics()
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("repeat read differs: %v vs %v", first, second)
	}
	if m1.Size == 0 || m2.Hits <= m1.Hits || m2.Misses != m1.Misses {
		t.Fatalf("repeat read not served by the shared cache: size %d, hits %d->%d, misses %d->%d", m1.Size, m1.Hits, m2.Hits, m1.Misses, m2.Misses)
	}
	t.Logf("shared cache %d bytes, repeat read hits +%d, misses +%d", m2.Size, m2.Hits-m1.Hits, m2.Misses-m1.Misses)
}
