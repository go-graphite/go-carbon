package v2read

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// TestPendingRowsSelectByID feeds an owner registered row applications for a
// growing name set through threshold (background) and forced seals, and after
// every step compares the by-ID selection on each published row view with the
// linear oracle over the decoded head. Requests mix sealed names (resolved
// through metadata), names first seen since the last seal (resolved through
// NewNames) and unknown names.
func TestPendingRowsSelectByID(t *testing.T) {
	rng := rand.New(rand.NewSource(1609))
	cfg := testConfig()
	cfg.PublishInterval = time.Nanosecond
	cfg.SealMaxRows = 64
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: reader.Identities, PublishPending: reader.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	const total = 120
	names := make([]string, total)
	policies := make([][]byte, total)
	for i := range names {
		names[i] = fmt.Sprintf("byid.n%d", i)
		policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	now := int64(1794700800)
	order, cursor := uint64(0), uint64(0)
	rowViews, fresh, sealedHits := 0, 0, 0
	for step := range 200 {
		known := min(total, 4+step/2) // new names keep arriving
		var batches []v2core.Batch
		for k := 1 + rng.Intn(12); k > 0; k-- {
			i := rng.Intn(known)
			order++
			id := uint64(i + 1)
			batches = append(batches, v2core.Batch{ID: id, Order: order, Clock: now + int64(step), Policy: policies[i], Points: []v2core.Row{{ID: id, Time: now + int64(step), Bits: math.Float64bits(float64(step)), Seq: order}}})
		}
		cursor++
		if err := engine.AppendBatches(0, batches, cursor, rng.Intn(15) == 0); err != nil {
			t.Fatal(err)
		}
		// Each step checks whatever the owner has published: seal completions
		// publish at once, the rest on the owner's publish interval. Every 25th step
		// waits for its own batches so timed publications are covered too.
		last := batches[len(batches)-1]
		await(t, func() bool {
			g := reader.owners[0].Load()
			if g == nil || step%25 != 24 || g.view.Len() == 0 {
				return g != nil // a seal may have taken the whole head
			}
			b, _, _ := g.view.Application(g.view.Len() - 1)
			return b.Order >= last.Order
		})
		g := pinOwner(t, reader, 0)
		g.view.IDs(0, g.view.Len(), func(_ int, _ uint64, cold *v2core.Batch) bool {
			if cold == nil {
				rowViews++
				return false
			}
			return true
		})
		g.view.NewNames(func(uint64, uint64) { fresh++ })
		for q := 0; q < 6; q++ {
			var want []string
			for k := 1 + rng.Intn(3); k > 0; k-- {
				want = append(want, fmt.Sprintf("byid.n%d", rng.Intn(total+10)))
			}
			index, err := g.parts.SnapshotForNames(context.Background(), g.snapshot, want, g.cache.ranges)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range want {
				if _, _, ok := index.LookupMetadata(n); ok {
					sealedHits++
				}
			}
			got, err := selectPending(context.Background(), index, g.view, want)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := linearPending(g.view.Batches(), want)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(ref) {
				t.Fatalf("step %d %v: got %d applications, want %d", step, want, len(got), len(ref))
			}
			for i := range got {
				if got[i].ID != ref[i].ID || got[i].Order != ref[i].Order || len(got[i].Points) != len(ref[i].Points) {
					t.Fatalf("step %d %v: application %d differs", step, want, i)
				}
			}
		}
		g.release()
	}
	if rowViews == 0 || fresh == 0 || sealedHits == 0 {
		t.Fatalf("coverage: %d row views, %d new names, %d sealed hits", rowViews, fresh, sealedHits)
	}
}

// TestPendingByIDSecondIDFailsLoudly breaks the by-ID reader's invariant: it
// feeds a sealed name's rows under a second ID. The owner never installs that
// ID (its seal plan refuses a second ID for a name), so the ID stays on the
// published new-name list, the reader selects its rows, and both sides fail
// loudly instead of dropping the points.
func TestPendingByIDSecondIDFailsLoudly(t *testing.T) {
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{Identities: reader.Identities, PublishPending: reader.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	const name = "byid.twice"
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
	if err != nil {
		t.Fatal(err)
	}
	now := int64(1794700800)
	batch := func(id, order uint64) []v2core.Batch {
		return []v2core.Batch{{ID: id, Order: order, Clock: now, Policy: policy, Points: []v2core.Row{{ID: id, Time: now + int64(order), Bits: math.Float64bits(1), Seq: order}}}}
	}
	if err := engine.AppendBatches(0, batch(1, 1), 1, true); err != nil {
		t.Fatal(err)
	}
	if err := engine.AppendBatches(0, batch(2, 2), 2, false); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		g := reader.owners[0].Load()
		return g != nil && g.view.Len() > 0
	})
	g := pinOwner(t, reader, 0)
	_, err = g.forNames(context.Background(), []string{name}, &reader.stages)
	g.release()
	if err == nil || err.Error() != "pending identity/policy mismatch" {
		t.Fatalf("read of a name pending under a second ID: %v, want the identity mismatch", err)
	}
	err = engine.AppendBatches(0, nil, 3, true)
	if closeErr := engine.Close(); err == nil {
		err = closeErr
	}
	if err == nil || !strings.Contains(err.Error(), "multiple producer IDs") {
		t.Fatalf("seal of a name under a second ID: %v, want the owner to refuse it", err)
	}
}

// pinOwner retains the owner's current generation as Reader.FetchMany does.
// A retain fails only when a publication swapped the loaded generation out and
// released it, so the next load sees its successor; a plain load-then-retain
// fataled under load. The retry bound only turns a broken swap into a failure.
func pinOwner(t *testing.T, r *Reader, owner int) *generation {
	t.Helper()
	for range 10000 {
		if g := r.owners[owner].Load(); g != nil && g.retain() {
			return g
		}
		runtime.Gosched()
	}
	t.Fatal("no retainable generation")
	return nil
}
