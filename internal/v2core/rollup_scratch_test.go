package v2core

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealTimeOnlyFixture models a time-only seal: every name arrives in perName
// applications of one point (one per relay packet), round after round, so a
// seal holds perName rows per name. Orders rise across rounds.
func sealTimeOnlyFixture(tb testing.TB, names, perName int, seed int64) (*coreRollups, []Batch) {
	r := newCoreRollups()
	r.parallelism = 1
	rng := rand.New(rand.NewSource(seed))
	metrics := make([]*rollupMetric, names)
	for i := range metrics {
		id := uint64(i + 1)
		name := fmt.Sprintf("containers.k8s.prod.cluster-%02d.namespace-%03d.pod-%06x.owner-%d.container.cpu_usage_seconds_total", i%7, (i/7)%311, i, seed)
		p := RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}}
		raw, err := EncodeRollupPolicy(p)
		if err != nil {
			tb.Fatal(err)
		}
		m, err := r.metric(id, raw)
		if err != nil {
			tb.Fatal(err)
		}
		r.metrics[id], r.names[p.Name] = m, id
		metrics[i] = m
	}
	batches := make([]Batch, 0, names*perName)
	for round := range perName {
		start := len(batches)
		for i, m := range metrics {
			t := int64(100000 + round)
			batches = append(batches, Batch{ID: uint64(i + 1), Registered: m.PolicyIdentity, Clock: t, Points: []Row{{Time: t, Bits: uint64(rng.Int63()), Seq: 1}}})
		}
		rng.Shuffle(len(batches)-start, func(i, j int) { batches[start+i], batches[start+j] = batches[start+j], batches[start+i] })
	}
	for i := range batches {
		batches[i].Order = uint64(i + 1)
	}
	return r, batches
}

// TestSealPlanScratchBound pins the planner memory bound for time-only seals:
// stages are sized by names, not rows, and a planner keeps no working set
// after Commit or Abort. The working set goes back to the shared pool, so
// retained planner heap follows the seals in flight, not the owner count.
func TestSealPlanScratchBound(t *testing.T) {
	const names, perName = 3000, 6
	r, batches := sealTimeOnlyFixture(t, names, perName, 1)
	for seal := range 3 {
		// Empty the shared pool so the capacities below are this seal's own.
		runtime.GC()
		runtime.GC()
		plan, err := r.planParts(SealInput{Batches: batches})
		if err != nil {
			t.Fatal(err)
		}
		if plan.Names != names {
			t.Fatalf("seal %d planned %d names, want %d", seal, plan.Names, names)
		}
		if got := cap(r.stages); got > 2*names {
			t.Fatalf("seal %d: stage capacity %d for %d names (%d rows); stages must be sized by names", seal, got, names, len(batches))
		}
		if r.scratch == nil || r.retainedScratchBytes() == 0 {
			t.Fatalf("seal %d: plan holds no working set", seal)
		}
		if seal == 2 {
			plan.Commit()
		} else {
			plan.Abort()
		}
		if n := r.retainedScratchBytes(); n != 0 || r.scratch != nil || r.pending {
			t.Fatalf("seal %d: planner retains %d B of working set after release (scratch %v, pending %v)", seal, n, r.scratch != nil, r.pending)
		}
	}
}

// BenchmarkSealPlanRetained reports planner heap per frozen row for time-only
// seals (6 applications per name): inflight-B/row is one plan's working set,
// retained-B/row is what `owners` planners (and the shared pool) keep between
// seals after each has planned one seal, one seal in flight at a time. The
// retained figure is the heap freed by dropping every planner's seal buffers
// and emptying the pool.
func BenchmarkSealPlanRetained(b *testing.B) {
	const owners, names, perName = 8, 20000, 6
	planners := make([]*coreRollups, owners)
	inputs := make([][]Batch, owners)
	for i := range planners {
		planners[i], inputs[i] = sealTimeOnlyFixture(b, names, perName, int64(i+1))
	}
	rows := names * perName
	inflight := sealPlanRetainedHeap(b, planners[0], inputs[0])
	b.ResetTimer()
	for b.Loop() {
		for i, r := range planners {
			plan, err := r.planParts(SealInput{Batches: inputs[i]})
			if err != nil {
				b.Fatal(err)
			}
			plan.Abort()
		}
	}
	b.StopTimer()
	elapsed := b.Elapsed()
	var held, dropped runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&held)
	for _, r := range planners {
		r.stages, r.batchPositions, r.samples, r.batches, r.arena = nil, nil, nil, nil, nil
		r.files, r.coverage, r.positions, r.pendingNames, r.lanes = nil, nil, nil, nil, nil
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&dropped)
	runtime.KeepAlive(planners)
	runtime.KeepAlive(inputs)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*owners*rows), "ns/row")
	b.ReportMetric(float64(inflight)/float64(rows), "inflight-B/row")
	b.ReportMetric(float64(held.HeapAlloc-min(held.HeapAlloc, dropped.HeapAlloc))/float64(owners*rows), "retained-B/row")
}
