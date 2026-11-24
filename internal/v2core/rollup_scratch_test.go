package v2core

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
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
	metrics := make([]*PolicyIdentity, names)
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
		r.install(id, m, metricMetadata{})
		metrics[i] = m
	}
	batches := make([]Batch, 0, names*perName)
	for round := range perName {
		start := len(batches)
		for i, m := range metrics {
			t := int64(100000 + round)
			batches = append(batches, Batch{ID: uint64(i + 1), Registered: m, Clock: t, Points: []Row{{Time: t, Bits: uint64(rng.Int63()), Seq: 1}}})
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
	defer drainRollupScratchPool()
	const names, perName = 3000, 6
	r, batches := sealTimeOnlyFixture(t, names, perName, 1)
	for seal := range 3 {
		// The cache survives GC; explicitly discard earlier working sets so
		// the capacities below are this seal's own.
		drainRollupScratchPool()
		plan, err := r.planParts(coreSealInput{Batches: batches})
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

func drainRollupScratchPool() {
	for {
		select {
		case <-rollupScratchPool:
		default:
			return
		}
	}
}

func TestSealScratchReuseAcrossGC(t *testing.T) {
	drainRollupScratchPool()
	defer drainRollupScratchPool()
	r := newCoreRollups()
	r.takeScratch()
	s := r.scratch
	r.stages = make([]rollupStage, 0, 1024)
	r.release()
	runtime.GC()
	runtime.GC()
	r.takeScratch()
	defer r.release()
	if r.scratch != s || cap(r.stages) != 1024 {
		t.Fatal("GC discarded reusable seal buffers")
	}
}

func TestSealScratchIdleBound(t *testing.T) {
	drainRollupScratchPool()
	defer drainRollupScratchPool()
	planners := make([]*coreRollups, cap(rollupScratchPool))
	for i := range planners {
		planners[i] = newCoreRollups()
		planners[i].takeScratch()
	}
	for _, r := range planners {
		r.release()
	}
	if len(rollupScratchPool) != cap(rollupScratchPool) {
		t.Fatalf("idle working sets = %d, want %d", len(rollupScratchPool), cap(rollupScratchPool))
	}
}

// A plan owns its workspace until Commit/Abort. Slow dictionary writes must
// stop the next planner before it allocates another seal's stages and outputs.
func TestSealScratchWaitsForResolution(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			drainRollupScratchPool()
			defer drainRollupScratchPool()
			owners := make([]*coreRollups, cap(rollupScratchPool))
			plans := make([]coreSealPlan, len(owners))
			defer func() {
				for i, r := range owners {
					if r != nil && r.pending {
						plans[i].Abort()
					}
				}
			}()
			for i := range owners {
				r, batches := sealTimeOnlyFixture(t, 1, 1, int64(i))
				plan, err := r.planParts(coreSealInput{Batches: batches})
				if err != nil {
					t.Fatal(err)
				}
				owners[i], plans[i] = r, plan
			}
			r, batches := sealTimeOnlyFixture(t, 1, 1, 99)
			type result struct {
				plan coreSealPlan
				err  error
			}
			done := make(chan result, 1)
			go func() {
				plan, err := r.planParts(coreSealInput{Batches: batches})
				done <- result{plan, err}
			}()
			select {
			case got := <-done:
				if got.err == nil {
					got.plan.Abort()
				}
				t.Fatal("planner exceeded the active workspace bound")
			case <-time.After(100 * time.Millisecond):
			}
			if commit {
				plans[0].Commit()
			} else {
				plans[0].Abort()
			}
			select {
			case got := <-done:
				if got.err != nil {
					t.Fatal(got.err)
				}
				got.plan.Abort()
				if r.scratch != nil || r.retainedScratchBytes() != 0 {
					t.Fatal("resolved plan retained its workspace")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("resolved plan did not release a workspace")
			}
		})
	}
}

// BenchmarkSealPlanRetained reports planner heap per frozen row for time-only
// seals (6 applications per name): inflight-B/row is one plan's working set,
// retained-B/row is what `owners` planners (and the shared pool) keep between
// seals after each has planned one seal, one seal in flight at a time. The
// retained figure is the heap freed by dropping every planner's seal buffers
// and emptying the pool.
func BenchmarkSealPlanRetained(b *testing.B) {
	drainRollupScratchPool()
	defer drainRollupScratchPool()
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
			plan, err := r.planParts(coreSealInput{Batches: inputs[i]})
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
		r.stages, r.stageMetrics, r.samples, r.batches, r.arena = nil, nil, nil, nil, nil
		r.files, r.coverage, r.lanes = nil, nil, nil
	}
	drainRollupScratchPool()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&dropped)
	runtime.KeepAlive(planners)
	runtime.KeepAlive(inputs)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*owners*rows), "ns/row")
	b.ReportMetric(float64(inflight)/float64(rows), "inflight-B/row")
	b.ReportMetric(float64(held.HeapAlloc-min(held.HeapAlloc, dropped.HeapAlloc))/float64(owners*rows), "retained-B/row")
}

// retainedScratchBytes is the planner working-set capacity this planner keeps
// between seals; with pooling it is zero once a seal has committed or aborted.
func (r *coreRollups) retainedScratchBytes() int {
	n := (cap(r.sweepKeys) + cap(r.sweepSpare) + cap(r.seen)) * 8
	// A nonzero table retains storage; its private backing capacity is not exposed.
	if !reflect.ValueOf(r.pendingNames).IsZero() {
		n += int(unsafe.Sizeof(r.pendingNames))
	}
	n += cap(r.stages)*int(unsafe.Sizeof(rollupStage{})) + cap(r.stageMetrics)*int(unsafe.Sizeof((*PolicyIdentity)(nil))) + cap(r.samples)*int(unsafe.Sizeof(sealrollup.Sample{})) + cap(r.batches)*int(unsafe.Sizeof(sealrollup.Batch{})) + cap(r.arena)
	for i := range r.lanes {
		lane := &r.lanes[i]
		n += (cap(lane.ranked)+cap(lane.spare))*int(unsafe.Sizeof(rollupRankRef{})) + cap(lane.counts)*8 + cap(lane.records)*int(unsafe.Sizeof(sealrollup.Record{})) + cap(lane.output.arena)
		for _, file := range lane.output.files {
			n += cap(*file) * int(unsafe.Sizeof(plannedKV{}))
		}
	}
	return n
}

// Two owners may hold plans simultaneously. A third seal reuses only a
// resolved plan's working set; neither Commit nor Abort may leave aliases.
func TestSealScratchOutstandingPlans(t *testing.T) {
	for _, commitFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(commitFirst), func(t *testing.T) {
			drainRollupScratchPool()
			defer drainRollupScratchPool()
			_, input := sealTimeOnlyFixture(t, 300, 2, 42)
			for i := range input {
				input[i].Policy = input[i].Registered.definitionBytes()
				input[i].Registered = nil
			}
			first, second := newCoreRollups(), newCoreRollups()
			first.parallelism, second.parallelism = 1, 1
			a, err := first.planParts(coreSealInput{Batches: input})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if first.pending {
					a.Abort()
				}
			}()
			snapshot := func(p coreSealPlan) string {
				var buf bytes.Buffer
				for _, part := range p.Parts {
					for _, segment := range part.Segments {
						for _, kv := range segment {
							fmt.Fprintf(&buf, "%s:%x;", kv.name, kv.value)
							if kv.record != nil {
								fmt.Fprintf(&buf, "%+v;", *kv.record)
							}
						}
					}
				}
				return buf.String()
			}
			want := snapshot(a)
			keys, spare, seen := slices.Clone(first.sweepKeys), slices.Clone(first.sweepSpare), slices.Clone(first.seen)
			held := first.scratch
			check := func() {
				t.Helper()
				if first.scratch != held || held == nil || !first.pending {
					t.Fatal("outstanding plan lost scratch ownership")
				}
				if got := snapshot(a); got != want {
					t.Fatal("outstanding plan records or names changed during reuse")
				}
				if !slices.Equal(first.sweepKeys, keys) || !slices.Equal(first.sweepSpare, spare) || !slices.Equal(first.seen, seen) {
					t.Fatal("outstanding plan keys changed")
				}
				for i, m := range first.stageMetrics {
					if n, ok := first.pendingNames.Get(m.fingerprint()); !ok || n != uint32(i) {
						t.Fatal("outstanding plan lost fresh-name checks")
					}
				}
			}
			check()
			for round := range 3 {
				secondInput := slices.Clone(input)
				for i := range secondInput {
					secondInput[i].Order += uint64(round) * 1000
				}
				b, err := second.planParts(coreSealInput{Batches: secondInput})
				if err != nil {
					t.Fatal(err)
				}
				if first.scratch == second.scratch {
					t.Fatal("outstanding plans share scratch")
				}
				check()
				if round == 0 {
					b.Abort()
				} else {
					b.Commit()
				}
				if second.retainedScratchBytes() != 0 {
					t.Fatal("resolved owner retains scratch")
				}
				check()
			}
			if commitFirst {
				a.Commit()
			} else {
				a.Abort()
			}
			if first.retainedScratchBytes() != 0 {
				t.Fatal("first owner retains released scratch")
			}
			// Reuse a returned table with exactly the same fresh names. Old entries
			// must be cleared, while two IDs in a new plan must still be rejected.
			third := newCoreRollups()
			c, err := third.planParts(coreSealInput{Batches: input})
			if err != nil {
				t.Fatal(err)
			}
			c.Abort()
			duplicate := append([]Batch(nil), input[:1]...)
			dup := input[0]
			dup.ID, dup.Order = 1<<40, dup.Order+1
			duplicate = append(duplicate, dup)
			if p, err := third.planParts(coreSealInput{Batches: duplicate}); err == nil {
				p.Abort()
				t.Fatal("accepted two fresh IDs for one name")
			} else if !strings.Contains(err.Error(), "multiple pending IDs") {
				t.Fatal(err)
			}
			if third.retainedScratchBytes() != 0 {
				t.Fatal("failed plan retains scratch")
			}
		})
	}
}

func TestSealScratchExpirySeen(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		for _, offset := range []uint64{0, 1 << 20, 1 << 40} {
			t.Run(fmt.Sprintf("expiry=%v/offset=%d", expiry, offset), func(t *testing.T) {
				drainRollupScratchPool()
				defer drainRollupScratchPool()
				_, input := sealTimeOnlyFixture(t, 16, 1, 11)
				for i := range input {
					input[i].Policy = input[i].Registered.definitionBytes()
					input[i].Registered = nil
				}
				r := newCoreRollups() // name-keyed compatibility path, with a durable DB
				r.parallelism = 1
				db, err := pebble.Open(t.TempDir(), &pebble.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				r.db = db
				if expiry {
					r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return false }
					r.setExpiryTTL(func(RollupPolicy, time.Time) (time.Duration, time.Time) { return time.Hour, time.Time{} })
				}
				for i := range input {
					input[i].ID += offset
				}
				p, err := r.planParts(coreSealInput{Batches: input})
				if err != nil {
					t.Fatal(err)
				}
				if expiry && offset == 0 && len(r.seen) == 0 {
					t.Fatal("dense expiry plan lacks idle-ID bitset")
				}
				if !expiry && cap(r.seen) != 0 {
					t.Fatal("expiry-off plan allocated idle-ID bitset")
				}
				p.Commit()
				if r.retainedScratchBytes() != 0 {
					t.Fatal("expiry plan retains scratch after commit")
				}
			})
		}
	}
}
