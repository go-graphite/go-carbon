package v2read

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestOpenedRequestsCancelAndReleasePins(t *testing.T) {
	r, _ := New(1, 1<<28, 1<<20)
	e, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: r.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer r.Close()
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "cancel.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.AppendBatches(0, []v2core.Batch{{ID: 1, Policy: policy, Order: 1, Clock: 1000, Points: []v2core.Row{{ID: 1, Time: 990, Bits: math.Float64bits(42), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	g := r.owners[0].Load()
	// Occupy only discovery's incremental loader. Point lookups and renders
	// must finish while it is busy, rather than wait for its whole-name walk.
	<-r.caches[0].loadGate
	if _, err := r.Metadata(context.Background(), "cancel.metric"); err != nil {
		t.Fatal(err)
	}
	if err := r.FetchMany(context.Background(), []string{"cancel.metric"}, 989, 990, 1000, func(_ string, s *Series) error {
		if len(s.Values) != 1 || math.Float64bits(s.Values[0]) != math.Float64bits(42) {
			t.Error("render bits changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := r.Names(ctx); done <- err }()
	await(t, func() bool { return g.refs.Load() > 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled discovery kept its pin")
	}
	if g.refs.Load() != 1 {
		t.Fatalf("cancel leaked generation: %d", g.refs.Load())
	}
	r.caches[0].loadGate <- struct{}{}
	// Every cancelled point request must leave just the publication reference.
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	for range 100 {
		if _, err := r.Metadata(ctx, "cancel.metric"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := r.FetchMany(ctx, []string{"cancel.metric"}, 989, 990, 1000, func(string, *Series) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if g.refs.Load() != 1 {
		t.Fatalf("point cancellation leaked: %d", g.refs.Load())
	}
}
