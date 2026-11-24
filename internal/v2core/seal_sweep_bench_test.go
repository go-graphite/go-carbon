package v2core

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// BenchmarkSealSweep compares seal planners on one fixture: N names, a quarter
// touched per seal with k points each (k = 1.1 and 5.7), rows in shuffled
// arrival order, single-archive 10s:3d. Arms:
//   - planParts: the production planner through the sealPlanner seam (ranked
//     names, plan + Abort, no commit or part write);
//   - model-A: a skeleton of today's planner (maps, 80-B batch descriptors,
//     pointerful stages, a 4-object pointer chase, radix by rank, commit);
//   - model-B1/B2: the seal sweep (16-B rows, stable radix by rank, one run
//     sweep over per-name SoA state indexed by local ID (B1) or by rank (B2)).
//
// The models are skeletons, not planners: they omit envelopes, closure and
// tuples. A new planner plugs in as another sealPlanner arm.
// SEAL_SWEEP_NAMES overrides N (comma-separated; default 1000000).
//
// planParts plans from a frozen pendingHead, as the seal does; allocB/point
// is what each seal allocates and drops, beside the retained transientB.
func BenchmarkSealSweep(b *testing.B) {
	sizes := []int{1_000_000}
	if v := os.Getenv("SEAL_SWEEP_NAMES"); v != "" {
		sizes = nil
		for _, f := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				b.Fatal(err)
			}
			sizes = append(sizes, n)
		}
	}
	planners := []struct {
		name string
		plan sealPlanner
	}{{"legacy", legacySealPlanner}, {"planParts", todaySealPlanner}}
	for _, names := range sizes {
		var r *coreRollups // the production planner fixture, shared by both k
		for _, k := range []float64{1.1, 5.7} {
			w := sweepModelWorkload{names: names, touched: names / 4, k: k}
			rng := rand.New(rand.NewSource(int64(names)))
			ids, times := sweepModelRows(w, rng)
			// local ID = arrival order; rank = name order, an unrelated permutation
			rank := make([]uint32, names)
			for i, x := range rng.Perm(names) {
				rank[i] = uint32(x)
			}
			points, nameSeals := len(ids), w.touched
			report := func(b *testing.B, records int) {
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nameSeals), "ns/name-seal")
				b.ReportMetric(float64(records), "records")
			}
			for _, p := range planners {
				b.Run(fmt.Sprintf("%s/names=%d/k=%.1f", p.name, names, k), func(b *testing.B) {
					if r == nil {
						r = sweepPlannerFixture(b, names)
					}
					arena := make([]row, len(ids))
					in := make([]Batch, len(ids))
					handles := make([]*ApplicationPolicy, names+2)
					// One arrival clock per 512-application receiver packet:
					// the latest point time in it.
					clocks := make([]int64, (len(ids)+511)/512)
					for i := range ids {
						clocks[i/512] = max(clocks[i/512], times[i])
					}
					for i, id := range ids {
						arena[i] = row{ID: uint64(id) + 1, Time: times[i], Bits: uint64(i) * 0x9e3779b97f4a7c15, Seq: uint64(i + 1)}
						in[i] = Batch{ID: uint64(id) + 1, Order: uint64(i + 1), Clock: clocks[i/512], Registered: r.installed(uint64(id) + 1), Points: arena[i : i+1]}
						handles[id+1] = in[i].Registered
					}
					// The seal's real input: the owner's frozen pending head,
					// built from receiver-shape packets of 512 applications.
					input := func() SealInput { return SealInput{Batches: in} }
					if p.name == "planParts" {
						var head pendingHead
						lookup := func(id uint64) *ApplicationPolicy { return handles[id] }
						for lo := 0; lo < len(in); lo += 512 {
							head.add(in[lo:min(lo+512, len(in))], lookup)
						}
						view := head.view(lookup)
						input = func() SealInput { return sealViewInput(view) }
					}
					plan, err := p.plan(r, input())
					if err != nil {
						b.Fatal(err)
					}
					scratch, pointerful := sealScratchBytes(r)
					records := 0
					for _, part := range plan.Parts {
						for _, segment := range part.Segments {
							for _, kv := range segment {
								if kv.record != nil {
									records++
								}
							}
						}
					}
					plan.Abort()
					var m0, m1 runtime.MemStats
					runtime.ReadMemStats(&m0)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						plan, err := p.plan(r, input())
						if err != nil {
							b.Fatal(err)
						}
						plan.Abort()
					}
					b.StopTimer()
					runtime.ReadMemStats(&m1)
					report(b, records)
					b.ReportMetric(float64(scratch)/float64(points), "transientB/point")
					b.ReportMetric(float64(pointerful)/float64(points), "ptrTransientB/point")
					b.ReportMetric(float64(m1.NumGC-m0.NumGC)/float64(b.N), "gc/op")
					b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc)/float64(b.N*points), "allocB/point")
				})
			}
			b.Run(fmt.Sprintf("model-A/names=%d/k=%.1f", names, k), func(b *testing.B) {
				a := &sweepModelA{metrics: make(map[uint64]*sweepModelMetric, names), positions: map[uint64]int{}}
				// metric, identity, policy, prepared objects in shuffled order (scattered heap)
				for _, id := range rng.Perm(names) {
					p := &sweepModelPolicy{name: sweepBenchName(id), prepared: &sweepModelPrepared{sweepModelStep, sweepModelCount}}
					ident := &sweepModelIdentity{id: uint64(id + 1), policy: p}
					a.metrics[uint64(id+1)] = &sweepModelMetric{sweepModelIdentity: ident, rank: rank[id]}
				}
				arena := make([]sweepModelRow, len(ids))
				in := make([]sweepModelBatch, len(ids))
				for i, id := range ids {
					arena[i] = sweepModelRow{ID: uint64(id + 1), Time: times[i], Bits: uint64(i) * 0x9e3779b97f4a7c15, Seq: uint64(i + 1)}
					in[i] = sweepModelBatch{ID: uint64(id + 1), Order: uint64(i + 1), Clock: times[i], Registered: a.metrics[uint64(id+1)].sweepModelIdentity, Points: arena[i : i+1]}
				}
				a.seal(in)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					a.seal(in)
				}
				report(b, len(a.recs))
			})
			for _, byRank := range []bool{false, true} {
				nm := "model-B1"
				if byRank {
					nm = "model-B2"
				}
				b.Run(fmt.Sprintf("%s/names=%d/k=%.1f", nm, names, k), func(b *testing.B) {
					a := &sweepModelB{byRank: byRank, policies: []sweepModelPrepared{{sweepModelStep, sweepModelCount}}}
					a.st = sweepModelSoA{fOrder: make([]uint64, names), fMax: make([]int64, names), lastUpd: make([]int64, names), through: make([]int64, names), revision: make([]uint64, names), policy: make([]uint16, names)}
					if !byRank {
						a.st.rank = rank
					}
					rows := make([]sweepModelPendingRow, len(ids))
					for i, id := range ids {
						x := id
						if byRank {
							x = rank[id] // the receive lookup yields the rank-indexed slot
						}
						rows[i] = sweepModelPendingRow{id: x, tRel: int32(times[i] - sweepModelBase), bits: uint64(i) * 0x9e3779b97f4a7c15}
					}
					a.seal(rows, 1, sweepModelBase, sweepModelBase+10)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						a.seal(rows, 1, sweepModelBase, sweepModelBase+10)
					}
					report(b, len(a.recs))
				})
			}
		}
	}
}

func sweepBenchName(id int) string {
	return fmt.Sprintf("containers.k8s.prod.ns-%03d.pod-%07x.cpu", id%311, id)
}

// sweepPlannerFixture installs names metrics (ID = local ID + 1) in a
// production planner and ranks them, as a warm owner has.
func sweepPlannerFixture(tb testing.TB, names int) *coreRollups {
	r := newCoreRollups()
	r.parallelism = 1
	cfg := sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: sweepModelStep, Count: sweepModelCount}}}
	for i := 0; i < names; i++ {
		name := v2part.ID(sweepBenchName(i))
		raw, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: "test", Config: cfg})
		if err != nil {
			tb.Fatal(err)
		}
		m, err := r.metric(uint64(i+1), raw)
		if err != nil {
			tb.Fatal(err)
		}
		rollupTestInstall(r, uint64(i+1), m, freshMetadata)
	}
	r.renumber()
	return r
}

// ---- the name-once laptop model (test-only skeletons) ----

const (
	sweepModelStep  = 10 // single-archive 10s:3d
	sweepModelCount = 25920
	sweepModelBase  = 1_700_000_000
)

type sweepModelRec struct { // one per (name, slot) winner
	rank  uint32
	slot  uint32
	order uint64
	bits  uint64
}

type sweepModelHeader struct { // one per name per part: frontier + metadata
	rank                         uint32
	frontierOrder                uint64
	maxTime, lastUpdate, through int64
	revision                     uint64
}

// model A: today-like pointerful planner.
type sweepModelPrepared struct{ step, count int64 }
type sweepModelPolicy struct {
	name     string
	digest   [32]byte
	prepared *sweepModelPrepared
}
type sweepModelIdentity struct {
	id     uint64
	policy *sweepModelPolicy
}
type sweepModelMeta struct {
	ClosedThrough int64
	Baseline      uint64
	Reset         uint64
	LastUpdate    int64
	Revision      uint64
	FOrder        uint64
	FMax          int64
	Version       uint8
	Expired       bool
}
type sweepModelMetric struct {
	*sweepModelIdentity
	meta  sweepModelMeta
	slots [5]uintptr // stands in for the sealrollup.Slots header
	rank  uint32
}
type sweepModelRow struct {
	ID        uint64
	Time      int64
	Bits, Seq uint64
}
type sweepModelBatch struct { // coreBatch-like: 80 B, 3 pointers
	ID, Order  uint64
	Clock      int64
	Policy     []byte
	Registered *sweepModelIdentity
	Points     []sweepModelRow
}
type sweepModelStage struct { // rollupStage-like, pointerful
	sortKey uint64
	metric  *sweepModelMetric
	next    sweepModelMeta
	first   int32 // into the per-seal batch order list
	n       int32
}

type sweepModelA struct {
	metrics   map[uint64]*sweepModelMetric
	positions map[uint64]int
	stages    []sweepModelStage
	bpos      []int32
	order     []int32 // batch indices grouped by stage
	keys, tmp []uint64
	recs      []sweepModelRec
	heads     []sweepModelHeader
}

func (a *sweepModelA) seal(in []sweepModelBatch) {
	clear(a.positions)
	stages := a.stages[:0]
	a.bpos = sweepModelGrow32(a.bpos, len(in))
	for bi := range in {
		b := &in[bi]
		i, ok := a.positions[b.ID]
		if !ok {
			m := a.metrics[b.ID]
			stages = append(stages, sweepModelStage{metric: m, next: m.meta, sortKey: uint64(m.rank)})
			i = len(stages) - 1
			a.positions[b.ID] = i
		}
		s := &stages[i]
		if b.Registered != s.metric.sweepModelIdentity {
			panic("policy changed")
		}
		s.n++
		a.bpos[bi] = int32(i)
	}
	// group batches by stage (counting by stage index)
	off := int32(0)
	for i := range stages {
		stages[i].first, off = off, off+stages[i].n
		stages[i].n = 0
	}
	a.order = sweepModelGrow32(a.order, len(in))
	for bi := range in {
		s := &stages[a.bpos[bi]]
		a.order[s.first+s.n] = int32(bi)
		s.n++
	}
	// rank sort of stages (radix, as sortRollupRanks)
	a.keys = sweepModelGrow64(a.keys, len(stages))
	for i := range stages {
		a.keys[i] = stages[i].sortKey<<32 | uint64(i)
	}
	a.tmp = sweepModelGrow64(a.tmp, len(stages))
	sweepModelRadix(a.keys, a.tmp)
	recs, heads := a.recs[:0], a.heads[:0]
	for _, k := range a.keys {
		s := &stages[uint32(k)]
		m := s.metric
		p := m.policy.prepared // dependent chain: metric -> identity -> policy -> prepared
		_ = m.slots[0]
		recs = sweepModelReduceA(recs, in, a.order[s.first:s.first+s.n], m.rank, p.step, p.count, &s.next)
		heads = append(heads, sweepModelHeader{rank: m.rank, frontierOrder: s.next.FOrder, maxTime: s.next.FMax, lastUpdate: s.next.LastUpdate, through: s.next.ClosedThrough, revision: s.next.Revision})
	}
	// commit: second touch of every metric
	for i := range stages {
		stages[i].metric.meta = stages[i].next
	}
	a.stages, a.recs, a.heads = stages, recs, heads
}

func sweepModelReduceA(recs []sweepModelRec, in []sweepModelBatch, idx []int32, rank uint32, st, cnt int64, next *sweepModelMeta) []sweepModelRec {
	start := len(recs)
	for _, bi := range idx {
		b := &in[bi]
		for _, p := range b.Points {
			slot := uint32((p.Time / st) % cnt)
			recs = sweepModelPutLWW(recs, start, sweepModelRec{rank: rank, slot: slot, order: b.Order, bits: p.Bits})
			next.FOrder = max(next.FOrder, b.Order)
			next.FMax = max(next.FMax, p.Time)
		}
		next.LastUpdate = b.Clock
	}
	next.Revision++
	next.ClosedThrough = next.FMax - next.FMax%st
	next.Version = 3
	return recs
}

// sweepModelPutLWW keeps one record per slot; later arrival (greater order)
// wins. Runs are 1-6 rows, so a linear scan is cheaper than a map.
func sweepModelPutLWW(recs []sweepModelRec, start int, r sweepModelRec) []sweepModelRec {
	for i := start; i < len(recs); i++ {
		if recs[i].slot == r.slot {
			if r.order > recs[i].order {
				recs[i] = r
			}
			return recs
		}
	}
	return append(recs, r)
}

// models B: pointer-free rows + stable radix + SoA sweep.
type sweepModelPendingRow struct { // resident pending row, 16 B
	id   uint32
	tRel int32 // seconds since the packet base
	bits uint64
}
type sweepModelSoA struct { // per-name permanent state, one array per field
	rank          []uint32 // local ID -> name-order rank (B1 only)
	fOrder        []uint64
	fMax, lastUpd []int64
	through       []int64
	revision      []uint64
	policy        []uint16 // index into a small prepared-policy table
}
type sweepModelB struct {
	st        sweepModelSoA
	byRank    bool // state indexed by rank (sequential) vs local ID (gather)
	policies  []sweepModelPrepared
	keys, tmp []uint64
	recs      []sweepModelRec
	heads     []sweepModelHeader
}

// seal: rows are in arrival order; order = base+i, one clock per packet.
func (a *sweepModelB) seal(rows []sweepModelPendingRow, base uint64, tBase, clock int64) {
	a.keys = sweepModelGrow64(a.keys, len(rows))
	for i := range rows {
		r := uint64(rows[i].id)
		if !a.byRank {
			r = uint64(a.st.rank[rows[i].id])
		}
		a.keys[i] = r<<32 | uint64(i)
	}
	a.tmp = sweepModelGrow64(a.tmp, len(rows))
	sweepModelRadix(a.keys, a.tmp)
	recs, heads := a.recs[:0], a.heads[:0]
	st := &a.st
	for i := 0; i < len(a.keys); {
		rank := uint32(a.keys[i] >> 32)
		j := i + 1
		for j < len(a.keys) && uint32(a.keys[j]>>32) == rank {
			j++
		}
		x := rank // state index
		if !a.byRank {
			x = rows[uint32(a.keys[i])].id
		}
		p := a.policies[st.policy[x]]
		fo, fm := st.fOrder[x], st.fMax[x]
		start := len(recs)
		for _, k := range a.keys[i:j] {
			ri := uint32(k)
			r := rows[ri]
			t := tBase + int64(r.tRel)
			o := base + uint64(ri)
			recs = sweepModelPutLWW(recs, start, sweepModelRec{rank: rank, slot: uint32((t / p.step) % p.count), order: o, bits: r.bits})
			fo, fm = max(fo, o), max(fm, t)
		}
		rev := st.revision[x] + 1
		th := fm - fm%p.step
		heads = append(heads, sweepModelHeader{rank: rank, frontierOrder: fo, maxTime: fm, lastUpdate: clock, through: th, revision: rev})
		// commit in the same sweep (production: after the part is durable, from heads)
		st.fOrder[x], st.fMax[x], st.lastUpd[x], st.through[x], st.revision[x] = fo, fm, clock, th, rev
		i = j
	}
	a.recs, a.heads = recs, heads
}

// sweepModelRadix sorts by the high 32 bits (rank), stable, 2 passes of 12
// bits (rank < 2^24).
func sweepModelRadix(keys, tmp []uint64) {
	var c [4096]int
	for pass := 0; pass < 2; pass++ {
		sh := 32 + 12*pass
		clear(c[:])
		for _, k := range keys {
			c[(k>>sh)&4095]++
		}
		s := 0
		for i := range c {
			c[i], s = s, s+c[i]
		}
		for _, k := range keys {
			d := (k >> sh) & 4095
			tmp[c[d]] = k
			c[d]++
		}
		copy(keys, tmp)
	}
}

func sweepModelGrow64(s []uint64, n int) []uint64 {
	if cap(s) < n {
		return make([]uint64, n)
	}
	return s[:n]
}

func sweepModelGrow32(s []int32, n int) []int32 {
	if cap(s) < n {
		return make([]int32, n)
	}
	return s[:n]
}

type sweepModelWorkload struct {
	names, touched int
	k              float64
}

// sweepModelRows returns touched local IDs (arrival-order assigned) with k
// points each on average, shuffled into arrival order.
func sweepModelRows(w sweepModelWorkload, rng *rand.Rand) (ids []uint32, times []int64) {
	perm := rng.Perm(w.names)[:w.touched]
	for _, id := range perm {
		n := int(w.k)
		if rng.Float64() < w.k-float64(n) {
			n++
		}
		for j := 0; j < n; j++ {
			ids = append(ids, uint32(id))
			times = append(times, sweepModelBase+int64(j))
		}
	}
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i]; times[i], times[j] = times[j], times[i] })
	return ids, times
}

// sealViewInput is the seal's planner input for a frozen pending view.
func sealViewInput(v PendingView) SealInput { return SealInput{Pending: v} }
