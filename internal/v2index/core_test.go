package v2index

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestCoreCatalogArrivalExpiryAndRecovery(t *testing.T) {
	root := t.TempDir()
	cfg := v2core.Config{Producers: 1, Shards: 1, Owners: 1, MaxRows: 100, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1", Rollups: true}
	engine, err := v2core.Open(root, cfg, v2core.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = engine.Close()
		}
	}()
	appendName := func(id, order uint64, name string) {
		t.Helper()
		p, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 60}}, Method: sealrollup.Average, XFilesFactor: .5}})
		if err != nil {
			t.Fatal(err)
		}
		b := v2core.Batch{ID: id, Order: order, Clock: 100, Policy: p, Points: []v2core.Row{{ID: id, Time: 100, Bits: math.Float64bits(1), Seq: order}}}
		if err := engine.AppendBatches(0, []v2core.Batch{b}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	c := &Catalog{}
	refresh := func(rev uint64, live func(string) bool) {
		t.Helper()
		var snapshots []*pebble.Snapshot
		for _, db := range engine.DBs() {
			snapshot := db.NewSnapshot()
			defer snapshot.Close()
			snapshots = append(snapshots, snapshot)
		}
		if err := c.PublishCore(context.Background(), rev, snapshots, live); err != nil {
			t.Fatal(err)
		}
	}
	appendName(1, 1, "service.old")
	refresh(1, nil)
	pinned := c.Snapshot()
	appendName(2, 2, "service.new")
	refresh(2, nil)
	if got := slices.Collect(c.Snapshot().Names()); !slices.Equal(got, []string{"service.new", "service.old"}) {
		t.Fatal(got)
	}
	refresh(3, func(name string) bool { return name != "service.old" })
	if got := slices.Collect(c.Snapshot().Names()); !slices.Equal(got, []string{"service.new"}) {
		t.Fatal(got)
	}
	if pinned.Len() != 1 {
		t.Fatal("pinned view changed")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	engine, err = v2core.Open(root, cfg, v2core.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	refresh(4, func(name string) bool { return name != "service.old" })
	if c.Snapshot().Len() != 1 {
		t.Fatal("recovery ignored mask")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.PublishCore(ctx, 5, nil, nil); err == nil {
		t.Fatal("cancel ignored")
	}
	if c.Snapshot().Revision() != 4 {
		t.Fatal("failure replaced live image")
	}
}
