package v2read

import (
	"fmt"
	"math"
	"runtime"
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
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, PublishPending: r.PublishPending})
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

// TestCloseRacesPartsPublication closes Readers while a publication opens
// fresh parts on the shared block cache. Close drops the Reader's cache
// reference; a publication that had already passed its first closed check
// then took a reference on the freed cache and panicked with "pebble:
// inconsistent reference count" when a Reader closed before its engine.
func TestCloseRacesPartsPublication(t *testing.T) {
	r, err := New(1, 1<<20, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	e, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: r.PublishPending})
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
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "close.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AppendBatches(0, []v2core.Batch{{ID: 1, Policy: policy, Order: 1, Clock: 1000, Points: []v2core.Row{{ID: 1, Time: 990, Bits: math.Float64bits(1), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	g := r.owners[0].Load()
	if g == nil || g.inventory == nil {
		t.Fatal("no sealed publication")
	}
	number, inventory, db := g.number, g.inventory, e.DBs()[0]
	for i := range 300 {
		c, err := New(1, 1<<20, 1<<16)
		if err != nil {
			t.Fatal(err)
		}
		c.BlockCacheBytes = 1 << 20
		// The installed generation's parts hold the cache's only other reference.
		if err := c.PublishPending(0, db.NewSnapshot(), v2core.PendingBatches(nil), number, inventory); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			// Another number cannot share the installed parts: this opens its own.
			done <- c.PublishPending(0, db.NewSnapshot(), v2core.PendingBatches(nil), number+1, inventory)
		}()
		for range i % 32 {
			runtime.Gosched()
		}
		c.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
