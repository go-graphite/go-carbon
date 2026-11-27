package v2read

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A head-only publication shares the current sealed generation, so the core
// pins no Pebble snapshot for it; readers still see every new pending point.
// A seal moves the generation and takes exactly one new pin.
func TestHeadOnlyPublishPinsNothing(t *testing.T) {
	t.Parallel()
	clock := int64(1794700800)
	cfg := testConfig()
	cfg.SealInterval = time.Hour
	reader, err := New(1, 1<<30, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	var pins, unpinned, published atomic.Int64
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{
		Now: func() time.Time { return time.Unix(clock, 0) },
		PublishBatchesWithInventory: func(owner int, snap *pebble.Snapshot, pending []v2core.Batch, number uint64, inventory *v2core.RollupInventory) error {
			if snap == nil {
				unpinned.Add(1)
			} else {
				pins.Add(1)
			}
			err := reader.PublishWithInventory(owner, snap, pending, number, inventory)
			published.Store(int64(len(pending)))
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	const name = "pin.metric"
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
	if err != nil {
		t.Fatal(err)
	}
	check := func(order uint64) {
		t.Helper()
		err := reader.FetchMany(context.Background(), []string{name}, clock-60, clock, clock, func(_ string, s *Series) error {
			for o := uint64(1); o <= order; o++ {
				if i := (clock - 30 + int64(o) - s.From) / s.Step; s.Values[i] != float64(o) {
					t.Fatalf("order %d: slot %d = %v, want %d", order, i, s.Values[i], o)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	const heads = 6
	for order := uint64(1); order <= heads; order++ {
		b := v2core.Batch{ID: 1, Order: order, Clock: clock, Policy: policy, Points: []v2core.Row{{ID: 1, Time: clock - 30 + int64(order), Bits: math.Float64bits(float64(order)), Seq: order}}}
		if err := engine.AppendBatches(0, []v2core.Batch{b}, order, false); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return published.Load() == int64(order) })
		check(order)
	}
	if pins.Load() != 1 || unpinned.Load() < heads {
		t.Fatalf("head-only publications took %d pins (want the initial one), %d unpinned", pins.Load(), unpinned.Load())
	}
	// Each head-only publication holds its own part references and no Pebble
	// snapshot: the sealed publication closed its snapshot once parts were open.
	if g := reader.owners[0].Load(); g == nil || g.snapshot != nil || g.base != nil || g.parts == nil {
		t.Fatalf("head-only publication holds a snapshot or no parts: %+v", g)
	}
	b := v2core.Batch{ID: 1, Order: heads + 1, Clock: clock, Policy: policy, Points: []v2core.Row{{ID: 1, Time: clock - 30 + heads + 1, Bits: math.Float64bits(heads + 1), Seq: heads + 1}}}
	if err := engine.AppendBatches(0, []v2core.Batch{b}, heads+1, true); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return pins.Load() >= 2 && published.Load() == 0 })
	check(heads + 1)
	if g := reader.owners[0].Load(); g == nil || g.snapshot != nil {
		t.Fatal("sealed publication kept its snapshot after opening parts")
	}
	// A head-only publication over sealed parts reads them through its own
	// registry references after the sealed publication is released.
	b = v2core.Batch{ID: 1, Order: heads + 2, Clock: clock, Policy: policy, Points: []v2core.Row{{ID: 1, Time: clock - 30 + heads + 2, Bits: math.Float64bits(heads + 2), Seq: heads + 2}}}
	if err := engine.AppendBatches(0, []v2core.Batch{b}, heads+2, false); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return published.Load() == 1 })
	check(heads + 2)
}
