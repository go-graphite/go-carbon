package v2read

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestExpiryResetReopenResume(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "core")
	const clock = int64(1794700800)
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "expiry.metric", Group: "local", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 60}, {Step: 10, Count: 60}}}})
	if err != nil {
		t.Fatal(err)
	}
	var expire atomic.Bool
	cfg := v2core.Config{Rollups: true, JournalIdentity: "expiry-test", Producers: 1, Shards: 1, Owners: 1, MaxRows: 32, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}
	var r *Reader
	var e *v2core.Engine
	open := func() {
		var err error
		r, err = New(1, 1<<20, 1<<16)
		if err != nil {
			t.Fatal(err)
		}
		r.Now = func() time.Time { return time.Unix(clock, 0) }
		e, err = v2core.Open(root, cfg, v2core.Hooks{PublishPending: r.PublishPending, DenseRollups: true, Expire: func(_ v2core.RollupPolicy, _ v2core.RollupMetadata, _ time.Time) bool { return expire.Load() },
			ExpiryTTL: func(v2core.RollupPolicy, time.Time) (time.Duration, time.Time) { return time.Nanosecond, time.Time{} }})
		if err != nil {
			t.Fatal(err)
		}
	}
	closeStore := func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	open()
	appendBatch := func(order, cursor uint64, stamp int64, bits uint64) {
		t.Helper()
		err := e.AppendBatches(0, []v2core.Batch{{ID: 7, Order: order, Clock: clock, Policy: policy, Points: []v2core.Row{{ID: 7, Time: stamp, Bits: bits, Seq: order}}}}, cursor, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	appendBatch(1, 1, clock-3, math.Float64bits(3))
	// Keep an old generation pinned across the reset publication.
	old := r.owners[0].Load()
	if old == nil || !old.retain() {
		t.Fatal("missing publication")
	}
	expire.Store(true)
	if err := e.AppendBatches(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Fetch(ctx, "expiry.metric", int(clock-10), int(clock)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired fetch: %v", err)
	}
	names, err := r.Names(ctx)
	if err != nil || len(names) != 0 {
		t.Fatalf("expired names %v %v", names, err)
	}
	if masked, err := r.Masked(ctx, "expiry.metric"); err != nil || !masked {
		t.Fatalf("mask %v %v", masked, err)
	}
	snapshot := e.DBs()[0].NewSnapshot()
	err = v2core.ScanRollups(snapshot, func(_ v2core.RollupPolicy, metadata v2core.RollupMetadata, _ []sealrollup.Record) error {
		// Version 1 readers predate expiry and ignore Expired/ResetOrder. Reset
		// metadata must carry a version they reject (2, or 3 with closure
		// state); only readers that honour expiry accept those versions.
		if metadata.Version < 2 {
			t.Error("reset metadata is readable by an older reader that ignores expiry authority")
		}
		if metadata.Version != 4 || !metadata.Expired || metadata.ResetOrder != 2 {
			t.Errorf("reset metadata lost expiry authority: %+v", metadata)
		}
		return nil
	})
	if closeErr := snapshot.Close(); err != nil || closeErr != nil {
		t.Fatalf("reset metadata %v %v", err, closeErr)
	}
	old.load()
	m := old.lookup("expiry.metric")
	if old.err != nil || m == nil || m.meta.Expired {
		t.Fatal("old snapshot changed")
	}
	old.release()
	closeStore()
	// Reopen keeps the reset even when expiration configuration is disabled.
	expire.Store(false)
	open()
	if masked, err := r.Masked(ctx, "expiry.metric"); err != nil || !masked {
		t.Fatalf("reopened mask %v %v", masked, err)
	}
	// Replayed old cursor cannot restore the previous incarnation.
	appendBatch(1, 1, clock-3, math.Float64bits(99))
	appendBatch(2, 3, clock-2, 1<<63)
	out, err := r.Fetch(ctx, "expiry.metric", int(clock-10), int(clock))
	if err != nil || out == nil {
		t.Fatalf("resume %v", err)
	}
	for i, v := range out.Values() {
		stamp := int64(out.FromTime() + i*out.Step())
		if stamp == clock-3 && !math.IsNaN(v) {
			t.Fatalf("old value resurrected %v", v)
		}
		if stamp == clock-2 && math.Float64bits(v) != 1<<63 {
			t.Fatalf("resume bits %x", math.Float64bits(v))
		}
	}
	if masked, err := r.Masked(ctx, "expiry.metric"); err != nil || !masked {
		t.Fatalf("resume classic mask %v %v", masked, err)
	}
	closeStore()
	open()
	defer closeStore()
	out, err = r.Fetch(ctx, "expiry.metric", int(clock-10), int(clock))
	if err != nil || out == nil {
		t.Fatalf("resume reopen %v", err)
	}
}
