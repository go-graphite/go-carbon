package sealrollup

import (
	"fmt"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Rollup as a pure function of stored records (arch-review step B3).
//
// A "sort + encode only" seal stores, per original batch, each point in the
// archive its admission clock routes it to, aligned to that archive's step:
// (archive, time, value bits, order). No coarse value is computed at the seal.
// Readers derive every coarse archive from those records:
//
//   - fnCurrent: current records only (newest per physical slot), no history:
//     what a stateless read-time rollup over fully merged parts would see.
//   - fnStore over all versions: a coarse window is the value of its newest
//     successful Whisper event, its finer slots resolved as of that event.
//   - fnStore after merges: before a merge drops superseded versions it writes
//     the windows whose history they carry as materialized records (a value or
//     an absence, with the newest event order it covers).
//
// The exact rules the function needs, each found by the property corpus:
// order = (batch order, target archive), because Whisper applies one batch an
// archive at a time; a point whose physical slot a later point of the same
// batch takes is still an event (ghost); a failed XFF keeps the older value,
// so the walk continues to older events; a pass stops at the first level it
// writes nothing (chain rule); a part keeps one record per slot per batch.
//
// Each reader is compared, after every seal, with the eager Whisper reducer
// (Reduce, proven equal to go-whisper by TestDifferential) and with the
// current v2 path (PlanClosedInto + ProjectOpen), on the six differential
// corpora and a randomized property corpus.

// rawRec is one stored record. Raw records come from seals. mat records are
// materialized coarse values written by merges: order is the write order of the
// value (it competes for the physical slot with it), upto is the newest event
// order of its window that the value already accounts for.
type rawRec struct {
	archive     int
	time        int64
	bits        uint64
	order, upto uint64
	mat         bool
	// ghost is a point whose physical slot a later point of the same batch
	// (another epoch) took. It is never a value, but Whisper still propagated
	// its window in that batch, so it is an event there.
	ghost bool
	// none is a materialized absence: as of upto the window had no value. It
	// only bounds the event walk; it never competes for a slot.
	none bool
}

// rawPart is one seal's part: every batch routed by its own admission clock
// and aligned, with the Whisper order inside the batch (stable by time, then
// source sequence; the last write of a physical slot wins). The part keeps one
// record per (physical slot, batch); only merges drop superseded versions, so
// a merge, not the seal, decides when an older version may go.
func rawPart(c Config, batches []Batch) []rawRec {
	var out []rawRec
	for _, b := range batches {
		out = append(out, rawBatch(c, b)...)
	}
	return out
}

// fnTick orders sub-steps of one batch. Whisper applies a batch one target
// archive at a time, ascending: archive j's writes and their propagation see
// direct writes of the same batch to archives <= j only. A record's order is
// therefore (batch order, target archive).
func fnTick(order uint64, archive int) uint64 {
	if archive >= 16 {
		panic("too many archives for fnTick")
	}
	return order<<4 | uint64(archive)
}

// rawBatch routes and aligns one batch; the last write of a physical slot wins.
func rawBatch(c Config, b Batch) []rawRec {
	points := slices.Clone(b.Points)
	slices.SortStableFunc(points, func(a, z Sample) int {
		if a.Time != z.Time {
			return cmpInt(a.Time, z.Time)
		}
		return cmpInt(a.Sequence, z.Sequence)
	})
	last := map[key]int{}
	var recs, ghosts []rawRec
	for _, p := range points {
		target := -1
		for j, r := range c.Archives {
			if p.Time >= b.Now-r.Step*r.Count {
				target = j
				break
			}
		}
		if target < 0 {
			continue
		}
		a := c.Archives[target]
		t := align(p.Time, a.Step)
		k := key{target, slot(t, a)}
		tick := fnTick(b.Order, target)
		r := rawRec{archive: target, time: t, bits: p.Bits, order: tick, upto: tick}
		if i, ok := last[k]; ok {
			if recs[i].time != t {
				g := recs[i]
				g.ghost = true
				ghosts = append(ghosts, g)
			}
			recs[i] = r
		} else {
			last[k] = len(recs)
			recs = append(recs, r)
		}
	}
	return append(recs, ghosts...)
}

func cmpInt[T int64 | uint64 | int](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// newer reports whether a (order, stored, time) wins a physical slot over b.
// Greatest order wins. In one batch Whisper applies the higher target archive
// after the lower archive's propagation, so a stored record beats a computed
// one of the same order; between equals the later (greater) time was written
// last in the ascending pass.
func newer(ao uint64, as bool, at int64, bo uint64, bs bool, bt int64) bool {
	if ao != bo {
		return ao > bo
	}
	if as != bs {
		return as
	}
	return at > bt
}

// fnCurrent derives every archive from current records alone.
func fnCurrent(c Config, recs []rawRec) map[key]Sample {
	type entry struct {
		s      Sample
		stored bool
	}
	img := map[key]entry{}
	for _, r := range recs {
		if r.ghost {
			continue
		}
		k := key{r.archive, slot(r.time, c.Archives[r.archive])}
		if old, ok := img[k]; !ok || newer(r.order, true, r.time, old.s.Sequence, true, old.s.Time) {
			img[k] = entry{Sample{r.time, r.bits, r.order}, true}
		}
	}
	for a := 1; a < len(c.Archives); a++ {
		fine, coarse := c.Archives[a-1], c.Archives[a]
		windows := map[int64][]Sample{}
		for k, e := range img {
			if k.archive == a-1 && k.slot == slot(e.s.Time, fine) {
				w := align(e.s.Time, coarse.Step)
				windows[w] = append(windows[w], e.s)
			}
		}
		starts := make([]int64, 0, len(windows))
		for w := range windows {
			starts = append(starts, w)
		}
		slices.Sort(starts)
		for _, w := range starts {
			ss := windows[w]
			slices.SortFunc(ss, func(x, y Sample) int { return cmpInt(x.Time, y.Time) })
			p, ok := AggregateWindow(c, a, w, ss)
			if !ok {
				continue
			}
			k := key{a, slot(w, coarse)}
			if old, has := img[k]; has && !newer(p.Sequence, false, p.Time, old.s.Sequence, old.stored, old.s.Time) {
				continue
			}
			img[k] = entry{p, false}
		}
	}
	out := make(map[key]Sample, len(img))
	for k, e := range img {
		out[k] = e.s
	}
	return out
}

// fnStore evaluates coarse windows from every stored version.
type fnStore struct {
	c      Config
	recs   []rawRec
	orders []uint64                       // distinct stored orders, ascending
	bySlot []map[int64][]int              // level -> physical slot -> stored records
	wins   []map[int64]map[int64]bool     // level -> physical slot -> window starts with events/records
	events []map[int64][]uint64           // level -> window start -> distinct raw orders below the level inside it
	absent map[[2]int64]uint64            // (level, window) -> newest absence marker
	passes map[uint64][]rawRec            // tick -> raw records (and ghosts) its pass wrote
	memoC  map[[2]uint64]bool             // (tick, level) -> the pass wrote at that level
	memoV  map[[3]int64]fnValue           // (level, window, asOf)
	memoS  map[[3]int64]fnValue           // (level, slot, asOf)
	stats  struct{ walks, deepWalks int } // events evaluated beyond the newest one
}

type fnValue struct {
	s      Sample // Sequence is the write order
	stored bool
	ok     bool
}

func (f *fnStore) index() {
	n := len(f.c.Archives)
	f.bySlot = make([]map[int64][]int, n)
	f.wins = make([]map[int64]map[int64]bool, n)
	f.events = make([]map[int64][]uint64, n)
	for l := range n {
		f.bySlot[l], f.wins[l], f.events[l] = map[int64][]int{}, map[int64]map[int64]bool{}, map[int64][]uint64{}
	}
	addWin := func(l int, w int64) {
		s := slot(w, f.c.Archives[l])
		if f.wins[l][s] == nil {
			f.wins[l][s] = map[int64]bool{}
		}
		f.wins[l][s][w] = true
	}
	f.orders = f.orders[:0]
	f.absent = map[[2]int64]uint64{}
	f.passes = map[uint64][]rawRec{}
	f.memoC = map[[2]uint64]bool{}
	for i, r := range f.recs {
		if !r.mat && !r.none {
			f.passes[r.order] = append(f.passes[r.order], r)
		}
		s := slot(r.time, f.c.Archives[r.archive])
		if r.none {
			k := [2]int64{int64(r.archive), r.time}
			f.absent[k] = max(f.absent[k], r.upto)
			f.orders = append(f.orders, r.upto)
			addWin(r.archive, r.time)
			continue
		}
		if !r.ghost {
			f.bySlot[r.archive][s] = append(f.bySlot[r.archive][s], i)
			addWin(r.archive, r.time)
		}
		f.orders = append(f.orders, r.order)
		for a := r.archive + 1; a < n; a++ {
			w := align(r.time, f.c.Archives[a].Step)
			addWin(a, w)
			if !r.mat {
				f.events[a][w] = append(f.events[a][w], r.order)
			}
		}
	}
	slices.Sort(f.orders)
	f.orders = slices.Compact(f.orders)
	for a := range f.events {
		for w, ev := range f.events[a] {
			slices.Sort(ev)
			f.events[a][w] = slices.Compact(ev)
		}
	}
	f.memoV, f.memoS = map[[3]int64]fnValue{}, map[[3]int64]fnValue{}
}

// norm maps asOf to the newest stored order at or below it, so equal stored
// prefixes share memo entries.
func (f *fnStore) norm(asOf uint64) uint64 {
	i := sort.Search(len(f.orders), func(i int) bool { return f.orders[i] > asOf })
	if i == 0 {
		return 0
	}
	return f.orders[i-1]
}

// stored returns the newest stored record of window w at level a at or before
// asOf (raw direct write or materialized value).
func (f *fnStore) stored(a int, w int64, asOf uint64) (rawRec, bool) {
	var best rawRec
	has := false
	for _, i := range f.bySlot[a][slot(w, f.c.Archives[a])] {
		r := f.recs[i]
		if r.time != w || r.order > asOf {
			continue
		}
		if !has || r.order > best.order || r.order == best.order && r.mat {
			best, has = r, true
		}
	}
	return best, has
}

// valueAt is the Whisper value of coarse window w at level a after every event
// with order <= asOf: the newest event whose visible finer slots pass XFF, or
// the stored record when it was written at/after that event. Failed XFF keeps
// the older value, so the walk continues to older events.
func (f *fnStore) valueAt(a int, w int64, asOf uint64) fnValue {
	asOf = f.norm(asOf)
	mk := [3]int64{int64(a), w, int64(asOf)}
	if v, ok := f.memoV[mk]; ok {
		return v
	}
	d, hasD := f.stored(a, w, asOf)
	ev := f.events[a][w]
	result := fnValue{}
	stop := uint64(0)
	if hasD {
		result = fnValue{Sample{d.time, d.bits, d.order}, !d.mat, true}
		stop = d.upto
	}
	if u, ok := f.absent[[2]int64{int64(a), w}]; ok && u <= asOf {
		stop = max(stop, u)
	}
	first := true
	for j := len(ev) - 1; j >= 0; j-- {
		o := ev[j]
		if o > asOf {
			continue
		}
		if o <= stop {
			break
		}
		if !f.chain(a, o) {
			continue
		}
		if !first {
			f.stats.deepWalks++
		}
		first = false
		f.stats.walks++
		samples := f.inputsAt(a-1, w, f.c.Archives[a].Step, o)
		if p, ok := AggregateWindow(f.c, a, w, samples); ok {
			p.Sequence = o
			result = fnValue{p, false, true}
			break
		}
	}
	f.memoV[mk] = result
	return result
}

// chain is Whisper's propagation stop: a pass reaches level a only if it wrote
// some window at every level between its target archive and a.
func (f *fnStore) chain(a int, tick uint64) bool {
	pass := f.passes[tick]
	if len(pass) == 0 {
		return true
	}
	for l := pass[0].archive + 1; l < a; l++ {
		mk := [2]uint64{tick, uint64(l)}
		wrote, ok := f.memoC[mk]
		if !ok {
			step := f.c.Archives[l].Step
			for _, r := range pass {
				if v := f.valueAt(l, align(r.time, step), tick); v.ok && !v.stored && v.s.Sequence == tick {
					wrote = true
					break
				}
			}
			f.memoC[mk] = wrote
		}
		if !wrote {
			return false
		}
	}
	return true
}

// inputsAt returns level l's visible winners inside [w, w+span) at order o.
func (f *fnStore) inputsAt(l int, w, span int64, o uint64) []Sample {
	al := f.c.Archives[l]
	var samples []Sample
	for ts := w; ts < w+span; ts += al.Step {
		if v := f.slotAt(l, slot(ts, al), o); v.ok && v.s.Time == ts {
			samples = append(samples, v.s)
		}
	}
	return samples
}

// slotAt is the content of physical slot s at level l after events <= o.
func (f *fnStore) slotAt(l int, s int64, o uint64) fnValue {
	o = f.norm(o)
	mk := [3]int64{int64(l), s, int64(o)}
	if v, ok := f.memoS[mk]; ok {
		return v
	}
	best := fnValue{}
	consider := func(v fnValue) {
		if v.ok && (!best.ok || newer(v.s.Sequence, v.stored, v.s.Time, best.s.Sequence, best.stored, best.s.Time)) {
			best = v
		}
	}
	for _, i := range f.bySlot[l][s] {
		// A materialized value competes for its slot as the computed value it
		// stands for; a raw direct write of the same batch was applied later.
		if r := f.recs[i]; r.order <= o {
			consider(fnValue{Sample{r.time, r.bits, r.order}, !r.mat, true})
		}
	}
	if l > 0 {
		for w := range f.wins[l][s] {
			consider(f.valueAt(l, w, o))
		}
	}
	f.memoS[mk] = best
	return best
}

func (f *fnStore) image() map[key]Sample {
	f.index()
	top := uint64(math.MaxUint64)
	out := map[key]Sample{}
	for l := range f.c.Archives {
		slots := map[int64]bool{}
		for s := range f.bySlot[l] {
			slots[s] = true
		}
		for s := range f.wins[l] {
			slots[s] = true
		}
		for s := range slots {
			if v := f.slotAt(l, s, top); v.ok {
				out[key{l, s}] = v.s
			}
		}
	}
	return out
}

// merge drops every superseded version (all but the physical-slot winner).
// Before that it materializes each coarse window a dropped version feeds, and
// the windows above it, from the full pre-merge store. It returns the number
// of dropped and materialized records.
func (f *fnStore) merge() (dropped, materialized int) {
	f.index()
	drop := map[int]bool{}
	for _, m := range f.bySlot {
		for _, idx := range m {
			win := idx[0]
			for _, i := range idx[1:] {
				r, b := f.recs[i], f.recs[win]
				if r.time == b.time && r.order == b.order {
					if r.mat && !b.mat {
						win = i
					}
				} else if newer(r.order, !r.mat, r.time, b.order, !b.mat, b.time) {
					win = i
				}
			}
			for _, i := range idx {
				if i != win {
					drop[i] = true
				}
			}
		}
	}
	for i, r := range f.recs {
		if r.ghost {
			drop[i] = true
		}
	}
	if len(drop) == 0 {
		return 0, 0
	}
	// A dropped version changes the history of its slot. Every window whose
	// event walk can read that slot (the parents of every epoch the slot held,
	// at every level above) is materialized first, value or absence, and so on
	// for the slots of those parents.
	type lslot struct {
		l int
		s int64
	}
	need := map[[2]int64]bool{}
	seen := map[lslot]bool{}
	var queue []lslot
	addParents := func(l int, t int64) {
		for a := l + 1; a < len(f.c.Archives); a++ {
			w := align(t, f.c.Archives[a].Step)
			need[[2]int64{int64(a), w}] = true
			if k := (lslot{a, slot(w, f.c.Archives[a])}); !seen[k] {
				seen[k] = true
				queue = append(queue, k)
			}
		}
	}
	for i := range drop {
		r := f.recs[i]
		addParents(r.archive, r.time)
		if k := (lslot{r.archive, slot(r.time, f.c.Archives[r.archive])}); !r.ghost && !seen[k] {
			seen[k] = true
			queue = append(queue, k)
		}
	}
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		for w := range f.wins[k.l][k.s] {
			addParents(k.l, w)
		}
	}
	top := uint64(math.MaxUint64)
	var mats []rawRec
	for k := range need {
		a, w := int(k[0]), k[1]
		v := f.valueAt(a, w, top)
		upto := uint64(0)
		if ev := f.events[a][w]; len(ev) > 0 {
			upto = ev[len(ev)-1]
		}
		if d, ok := f.stored(a, w, top); ok {
			upto = max(upto, d.upto)
		}
		if u, ok := f.absent[k]; ok {
			upto = max(upto, u)
		}
		if !v.ok {
			if upto > 0 {
				mats = append(mats, rawRec{archive: a, time: w, order: upto, upto: upto, none: true})
			}
			continue
		}
		upto = max(upto, v.s.Sequence)
		mats = append(mats, rawRec{archive: a, time: w, bits: v.s.Bits, order: v.s.Sequence, upto: upto, mat: true})
	}
	// Older absence markers of a window are superseded by the newest.
	for i, r := range f.recs {
		if r.none && (need[[2]int64{int64(r.archive), r.time}] || r.upto < f.absent[[2]int64{int64(r.archive), r.time}]) {
			drop[i] = true
		}
	}
	kept := f.recs[:0:0]
	for i, r := range f.recs {
		if !drop[i] {
			kept = append(kept, r)
		}
	}
	f.recs = append(kept, mats...)
	return len(drop), len(mats)
}

// ---- corpora ----

type fnMetric struct {
	cfg   Config
	seals [][]Batch // per seal, this metric's batches in increasing order
}

type fnCorpus struct {
	name    string
	metrics []fnMetric
}

func fnConfig(def string, method Method, xff float32) Config { return config(def, method, xff) }

// fnHandoff regenerates TestSealHandoffByteDifferential's input. An expired
// idle name starts a new incarnation (v2 masks its history; C4's new ID).
func fnHandoff() fnCorpus {
	configs := []Config{
		{Method: Last, Archives: []Archive{{10, 8640}}},
		{Method: Average, Archives: []Archive{{10, 8640}, {60, 10080}, {600, 52560}}},
		{Method: Max, Archives: []Archive{{60, 1440}, {3600, 24 * 400}}},
	}
	out := fnCorpus{name: "handoff"}
	for seed := int64(1); seed <= 3; seed++ {
		for _, lanes := range []int64{1, 3} {
			rng := rand.New(rand.NewSource(seed*7 + lanes))
			names := 500 + rng.Intn(400)
			cfgs := make([]Config, names)
			for i := range cfgs {
				_, _ = rng.Intn(3), rng.Intn(1<<20) // cluster, pod
				cfgs[i] = configs[rng.Intn(len(configs))]
				_ = rng.Intn(2) // group
			}
			cur := make([]int, names) // index into out.metrics
			for i := range cur {
				cur[i] = len(out.metrics)
				out.metrics = append(out.metrics, fnMetric{cfg: cfgs[i], seals: make([][]Batch, 4)})
			}
			order := uint64(1)
			base := int64(1_700_000_000)
			for seal := 0; seal < 4; seal++ {
				clock := base + int64(seal)*86400*11
				for i := range cfgs {
					if rng.Intn(3) == 0 {
						if seal > 0 && rng.Intn(4) == 0 {
							cur[i] = len(out.metrics)
							out.metrics = append(out.metrics, fnMetric{cfg: cfgs[i], seals: make([][]Batch, 4)})
						}
						continue
					}
					rows := make([]Sample, 1+rng.Intn(4))
					for j := range rows {
						offset := int64(rng.Intn(40)) * cfgs[i].Archives[0].Step
						if rng.Intn(8) == 0 {
							offset += int64(rng.Intn(40)) * 86400
						}
						bits := uint64(rng.Int63())
						rows[j] = Sample{Time: clock - offset, Bits: bits, Sequence: uint64(rng.Intn(3) + 1)}
					}
					m := &out.metrics[cur[i]]
					m.seals[seal] = append(m.seals[seal], Batch{Order: order, Now: clock, Points: rows})
					order++
				}
			}
		}
	}
	return out
}

// fnDigest regenerates TestSealPlanDifferentialDigest's input (p1 and p4 share
// it). Registered is drawn only for names installed by an earlier seal.
func fnDigest() fnCorpus {
	configs := []Config{
		{Method: Last, Archives: []Archive{{Step: 1, Count: 86400}}},
		{Method: Last, Archives: []Archive{{Step: 10, Count: 8640}}},
		{Method: Average, Archives: []Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}},
		{Method: Max, Archives: []Archive{{Step: 60, Count: 1440}, {Step: 3600, Count: 24 * 400}}},
	}
	out := fnCorpus{name: "digest"}
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		names := 300 + rng.Intn(700)
		first := len(out.metrics)
		for range names {
			switch rng.Intn(4) {
			case 0:
				_ = rng.Intn(5000)
			case 1:
				_, _, _ = rng.Intn(3), rng.Intn(9), rng.Intn(1<<20)
			case 2:
				_ = rng.Intn(100000)
			}
			cfg := configs[rng.Intn(len(configs))]
			_ = rng.Intn(3) // group
			out.metrics = append(out.metrics, fnMetric{cfg: cfg, seals: make([][]Batch, 4)})
		}
		installed := make([]bool, names)
		order := uint64(1)
		base := int64(1_700_000_000)
		type gen struct {
			i int
			b Batch
		}
		for seal := 0; seal < 4; seal++ {
			var batches []gen
			clock := base + int64(seal)*86400*11
			for i := range names {
				if rng.Intn(3) == 0 {
					continue
				}
				cfg := out.metrics[first+i].cfg
				for k := 0; k <= rng.Intn(3)/2; k++ {
					rows := make([]Sample, 1+rng.Intn(4))
					for j := range rows {
						step := cfg.Archives[0].Step
						offset := int64(rng.Intn(40)) * step
						if rng.Intn(8) == 0 {
							offset += int64(rng.Intn(40)) * 86400
						}
						bits := uint64(rng.Int63())
						rows[j] = Sample{Time: clock - offset, Bits: bits, Sequence: uint64(rng.Intn(3) + 1)}
					}
					if installed[i] {
						_ = rng.Intn(2)
					}
					order++
					batches = append(batches, gen{i, Batch{Order: order, Now: clock, Points: rows}})
				}
			}
			rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
			for i := range batches {
				batches[i].b.Order = order
				order++
			}
			for _, g := range batches {
				m := &out.metrics[first+g.i]
				m.seals[seal] = append(m.seals[seal], g.b)
				installed[g.i] = true
			}
			for i := range names {
				slices.SortFunc(out.metrics[first+i].seals[seal], func(a, b Batch) int { return cmpInt(a.Order, b.Order) })
			}
		}
	}
	return out
}

// fnHistory regenerates TestRollupHistoryDifferential: one name per method and
// schema, 40 epochs, special float bits, duplicate late stamps, XFF 0.1.
func fnHistory() fnCorpus {
	out := fnCorpus{name: "history"}
	bits := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
	for method := Average; method <= First; method++ {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			m := fnMetric{cfg: fnConfig(schema, method, .1)}
			now := int64(1794700800)
			var order uint64
			for epoch := int64(0); epoch < 40; epoch++ {
				now += 600
				order++
				var rows []Sample
				for j := int64(0); j < 60; j++ {
					rows = append(rows, Sample{Time: now - 600 + j*10, Bits: bits[(j+epoch)%int64(len(bits))], Sequence: uint64(j + 1)})
				}
				for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
					stamp := now - age
					rows = append(rows, Sample{Time: stamp, Bits: math.Float64bits(float64(epoch)), Sequence: 100}, Sample{Time: stamp, Bits: bits[epoch%int64(len(bits))], Sequence: 101})
				}
				m.seals = append(m.seals, []Batch{{Order: order, Now: now, Points: rows}})
			}
			out.metrics = append(out.metrics, m)
		}
	}
	return out
}

// fnSlim regenerates TestSlimEnvelopeEngineDifferential's 32 epochs.
func fnSlim() fnCorpus {
	out := fnCorpus{name: "slim"}
	ladders := []string{"10s:3d", "10s:2d,1m:14d,30m:2y", "1m:30d,1h:1y,1d:10y", "1s:1d,1m:30d,1h:1y,1d:10y", "1s:1h,1m:1d,1h:1y"}
	xffs := []float32{0, .1, .5, 1}
	for i := range 48 {
		method := Average + Method(i%int(First))
		out.metrics = append(out.metrics, fnMetric{cfg: fnConfig(ladders[i%len(ladders)], method, xffs[i%len(xffs)])})
	}
	rng := randv2.New(randv2.NewPCG(3, 5))
	now, order := int64(1794700800), uint64(0)
	for range 32 {
		now += 600
		clock := now
		for i := range out.metrics {
			m := &out.metrics[i]
			order++
			step := m.cfg.Archives[0].Step
			rows := make([]Sample, 2+rng.IntN(5))
			for j := range rows {
				ts := clock - int64(rng.IntN(60))*step
				rows[j] = Sample{Time: ts, Bits: rng.Uint64(), Sequence: uint64(j + 1)}
			}
			m.seals = append(m.seals, []Batch{{Order: order, Now: clock, Points: rows}})
		}
	}
	return out
}

// fnRank regenerates TestRollupPlanRankDifferential's 100 seals for 1 and 3
// workers. A name idle for more than 50 s expires and starts a new incarnation.
func fnRank() fnCorpus {
	out := fnCorpus{name: "rank"}
	runs := []uint64{1, 3}
	if testing.Short() {
		runs = runs[:1] // the rank corpus has no coarse ring wrap
	}
	for _, workers := range runs {
		rng := randv2.New(randv2.NewPCG(workers, 7))
		const ids = 1500
		cfgs := make([]Config, ids+1)
		births := make([]int, ids+1)
		cur := make([]int, ids+1)
		last := make([]int64, ids+1)
		for id := 1; id <= ids; id++ {
			def := "10s:3d"
			if id%3 == 0 {
				def = "10s:2d,1m:14d,30m:2y"
			}
			if id > ids/2 {
				births[id] = 5 + (id*7919)%80
			}
			cfgs[id] = fnConfig(def, Average, 0)
			cur[id] = -1
		}
		clock := int64(1801958400)
		order := uint64(0)
		for seal := 0; seal < 100; seal++ {
			clock += 10
			n := 0
			for id := 1; id <= ids; id++ {
				if seal < births[id] || rng.IntN(3) == 0 {
					if cur[id] >= 0 && last[id] > 0 && clock-last[id] > 50 {
						cur[id], last[id] = -1, 0 // expired at this idle stage
					}
					continue
				}
				order++
				n++
				v := float64(rng.IntN(1000)) / 4
				if cur[id] < 0 {
					cur[id] = len(out.metrics)
					out.metrics = append(out.metrics, fnMetric{cfg: cfgs[id], seals: make([][]Batch, 100)})
				}
				last[id] = clock
				out.metrics[cur[id]].seals[seal] = []Batch{{Order: order, Now: clock, Points: []Sample{{Time: clock - 5, Bits: math.Float64bits(v), Sequence: order}}}}
			}
			rng.Shuffle(n, func(i, j int) {})
		}
	}
	return out
}

// fnBackground regenerates TestBackgroundSealRendersMatchClassic's intake. The
// fill loop's length depends on seal timing there; here it stops at the 24-row
// seal threshold. Each append is one batch and one seal.
func fnBackground() fnCorpus {
	out := fnCorpus{name: "background"}
	rng := rand.New(rand.NewSource(1559))
	now := int64(1794700800)
	clock := now
	const metrics = 10
	for i := range metrics {
		out.metrics = append(out.metrics, fnMetric{cfg: fnConfig("1s:1d,1m:30d,1h:1y", Method(1+i%int(First)), .5)})
	}
	var cursor, seq uint64
	rows := 0
	appendOne := func(i int) {
		var pts []Sample
		for k := 1 + rng.Intn(5); k > 0; k-- {
			seq++
			ts := clock - int64(rng.Intn(600))
			v := float64(rng.Intn(1000)) / 8
			pts = append(pts, Sample{Time: ts, Bits: math.Float64bits(v), Sequence: seq})
		}
		rows += len(pts)
		cursor++
		out.metrics[i].seals = append(out.metrics[i].seals, []Batch{{Order: cursor, Now: clock, Points: pts}})
	}
	for i := range metrics {
		appendOne(i)
	}
	for rows = 0; rows < 24; {
		appendOne(rng.Intn(metrics))
	}
	for range 6 {
		clock += 3
		appendOne(rng.Intn(metrics))
	}
	for step := 0; step < 12; step++ {
		clock = now + 100 + int64(step)*7
		perm := rng.Perm(metrics)
		for _, i := range perm[:1+rng.Intn(4)] {
			appendOne(i)
		}
		_ = rng.Intn(6)
	}
	return out
}

// fnRandom is the property corpus: short rings so fine and coarse slots wrap
// within a run; late points beyond fine retention (direct coarse writes),
// future points, corrections of one stamp, regressing admission clocks,
// special float bits, several batches per seal, every method and XFF.
func fnRandom(seed uint64, names int) fnCorpus {
	out := fnCorpus{name: fmt.Sprintf("random-%d", seed)}
	rng := randv2.New(randv2.NewPCG(seed, 99))
	// The last two ladders have a coarse step close to the finer retention, so
	// a finer ring wraps inside one parent window (Whisper's chain rule).
	ladders := []string{"1s:60,10s:600,60s:3600", "1s:30,5s:300,60s:1200", "10s:120,60s:1800,600s:36000", "2s:40,10s:400", "1s:20,4s:200,20s:1000,100s:10000", "1s:3600", "1s:10,5s:30,30s:300", "1s:8,4s:32,32s:320,320s:3200"}
	xffs := []float32{0, .1, .3, .5, .75, 1}
	specials := []uint64{0x7ff8000000000042, 0x7ff0000000000001, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000}
	order := uint64(0)
	for range names {
		c := fnConfig(ladders[rng.IntN(len(ladders))], Average+Method(rng.IntN(int(First))), xffs[rng.IntN(len(xffs))])
		m := fnMetric{cfg: c}
		fine := c.Archives[0]
		span := c.Archives[len(c.Archives)-1].Step * c.Archives[len(c.Archives)-1].Count
		clock := int64(1794700800) + int64(rng.IntN(1000))
		seals := 10 + rng.IntN(50)
		for range seals {
			var seal []Batch
			for range 1 + rng.IntN(3) {
				switch r := rng.IntN(20); {
				case r == 0 && clock > 1794700800:
					clock -= int64(rng.IntN(int(fine.Step*fine.Count)/2) + 1) // regressing admission clock
				case r < 4:
					clock += fine.Step * fine.Count * int64(1+rng.IntN(3)) / 2 // ring-wrapping jump
				default:
					clock += int64(rng.IntN(int(fine.Step*8)) + 1)
				}
				var pts []Sample
				for range 1 + rng.IntN(12) {
					var ts int64
					switch r := rng.IntN(16); {
					case r == 0:
						ts = clock + int64(rng.IntN(int(fine.Step*fine.Count))) // future
					case r < 4:
						ts = clock - fine.Step*fine.Count - int64(rng.IntN(int(span))) // beyond fine retention
					case r < 6:
						ts = clock - fine.Step*fine.Count + int64(rng.IntN(int(fine.Step*4))) - fine.Step*2 // fine retention edge
					default:
						ts = clock - int64(rng.IntN(int(fine.Step*20)))
					}
					if ts <= 0 {
						ts = 1
					}
					b := math.Float64bits(float64(rng.IntN(64)) / 4)
					if rng.IntN(10) == 0 {
						b = specials[rng.IntN(len(specials))]
					}
					pts = append(pts, Sample{Time: ts, Bits: b, Sequence: uint64(rng.IntN(4))})
					if rng.IntN(6) == 0 { // correction of the same stamp in this batch
						pts = append(pts, Sample{Time: ts, Bits: math.Float64bits(float64(rng.IntN(64))), Sequence: uint64(rng.IntN(4))})
					}
				}
				order++
				seal = append(seal, Batch{Order: order, Now: clock, Points: pts})
			}
			m.seals = append(m.seals, seal)
		}
		out.metrics = append(out.metrics, m)
	}
	return out
}

// ---- comparison ----

type fnStats struct {
	compares, coarse   int // metric-seal comparisons, coarse physical slots compared
	closedOK           int // current v2 path (closed windows + projection) vs eager Whisper
	currentOK          int // fnCurrent vs current v2 path
	versionsOK         int // all-versions function vs current v2 path
	mergedOK           int // merged + materialized function vs current v2 path
	classes            map[string]int
	examples           map[string]string
	merges, drops, mat int
	walks, deepWalks   int
	firstMergedMiss    string
	firstClosedMiss    string
	firstVersionsMiss  string
	firstVersionsLen   int
	versionsWhisperOK  int // all-versions function vs eager Whisper
	mergedWhisperOK    int // merged function vs eager Whisper
}

func logical(img map[key]Sample) map[key][2]uint64 {
	out := make(map[key][2]uint64, len(img))
	for k, p := range img {
		out[k] = [2]uint64{uint64(p.Time), p.Bits}
	}
	return out
}

func sameSlot(a, b map[key][2]uint64, k key) bool {
	x, okx := a[k]
	y, oky := b[k]
	return okx == oky && x == y
}

// classify names the Whisper rule fnCurrent misses for one coarse slot.
func classify(c Config, recs []rawRec, want, cur map[key][2]uint64, k key) string {
	w := int64(want[k][0])
	if _, ok := want[k]; !ok {
		w = int64(cur[k][0])
	}
	a := k.archive
	fine := c.Archives[a-1]
	for ts := w; ts < w+c.Archives[a].Step; ts += fine.Step {
		if !sameSlot(want, cur, key{a - 1, slot(ts, fine)}) {
			return "cascade from a finer level"
		}
	}
	// A version inside the window whose physical slot now holds another stamp.
	newest := map[key]rawRec{}
	for _, r := range recs {
		if r.ghost {
			continue
		}
		pk := key{r.archive, slot(r.time, c.Archives[r.archive])}
		if o, ok := newest[pk]; !ok || r.order > o.order || r.order == o.order && r.time > o.time {
			newest[pk] = r
		}
	}
	for _, r := range recs {
		if r.ghost && r.archive < a && align(r.time, c.Archives[a].Step) == w {
			return "same-batch slot takeover (ghost event)"
		}
	}
	for _, r := range recs {
		if !r.ghost && r.archive < a && align(r.time, c.Archives[a].Step) == w {
			if n := newest[key{r.archive, slot(r.time, c.Archives[r.archive])}]; n.time != r.time {
				return "partial finer expiry (superseded input)"
			}
		}
	}
	for _, r := range recs {
		if !r.ghost && r.archive == a && r.time == w {
			return "direct coarse write"
		}
	}
	if c.XFilesFactor > 0 {
		return "xff fallback"
	}
	return "partial coarser expiry (superseded computed input)"
}

func runFnMetric(t *testing.T, m fnMetric, mergeRng *randv2.Rand, st *fnStats) {
	t.Helper()
	c := m.cfg
	p, err := Prepare(c)
	if err != nil {
		t.Fatal(err)
	}
	eager, deferred := map[key]Sample{}, map[key]Sample{}
	var old, frontier Frontier
	var through int64
	var store []rawRec
	merged := &fnStore{c: c}
	for si, seal := range m.seals {
		if len(seal) == 0 {
			continue
		}
		want, err := p.Plan(old, view(eager), nil, seal)
		if err != nil {
			t.Fatal(err)
		}
		got, nextThrough, err := p.PlanClosedInto(nil, frontier, through, 0, view(deferred), nil, seal, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range want.Records {
			install(eager, []Delta{r.Delta})
		}
		// The engine resolves a physical slot by the greatest record order across
		// parts (later part on ties), not by install sequence.
		for _, r := range got.Records {
			k := key{r.Archive, r.Slot}
			if o, ok := deferred[k]; !ok || r.Order >= o.Sequence {
				s := r.Sample
				s.Sequence = r.Order
				deferred[k] = s
			}
		}
		old, frontier, through = want.Next, got.Next, nextThrough
		closed := logical(projectOpenImage(c, deferred, frontier.MaxTime, through))
		whisperImg := logical(eager)

		part := rawPart(c, seal)
		store = append(store, part...)
		merged.recs = append(merged.recs, part...)
		if mergeRng.IntN(3) == 0 {
			d, n := merged.merge()
			st.merges++
			st.drops += d
			st.mat += n
		}
		current := logical(fnCurrent(c, store))
		all := &fnStore{c: c, recs: store}
		versions := logical(all.image())
		mergedImg := logical(merged.image())
		st.walks += all.stats.walks + merged.stats.walks
		st.deepWalks += all.stats.deepWalks + merged.stats.deepWalks
		all.stats.walks, merged.stats.walks, all.stats.deepWalks, merged.stats.deepWalks = 0, 0, 0, 0

		keys := map[key]bool{}
		for _, img := range []map[key][2]uint64{whisperImg, closed, current, versions, mergedImg} {
			for k := range img {
				keys[k] = true
			}
		}
		st.compares++
		for k := range keys {
			if k.archive == 0 {
				if !sameSlot(whisperImg, closed, k) || !sameSlot(closed, current, k) || !sameSlot(closed, versions, k) || !sameSlot(closed, mergedImg, k) {
					t.Fatalf("seal %d: fine slot %v differs", si, k)
				}
				continue
			}
			st.coarse++
			if sameSlot(whisperImg, closed, k) {
				st.closedOK++
			} else if st.firstClosedMiss == "" {
				st.firstClosedMiss = fmt.Sprintf("%s m%d xff%g seal%d a%d slot%d whisper%v closed%v history %+v", ladderString(c), c.Method, c.XFilesFactor, si, k.archive, k.slot, whisperImg[k], closed[k], m.seals[:si+1])
			}
			if sameSlot(whisperImg, versions, k) {
				st.versionsWhisperOK++
			} else if st.firstVersionsMiss == "" || len(m.seals[:si+1]) < st.firstVersionsLen {
				st.firstVersionsLen = len(m.seals[:si+1])
				st.firstVersionsMiss = fmt.Sprintf("%s m%d xff%g seal%d a%d slot%d whisper%v versions%v history %+v", ladderString(c), c.Method, c.XFilesFactor, si, k.archive, k.slot, whisperImg[k], versions[k], m.seals[:si+1])
			}
			if sameSlot(whisperImg, mergedImg, k) {
				st.mergedWhisperOK++
			}
			if sameSlot(closed, current, k) {
				st.currentOK++
			} else {
				cl := classify(c, store, closed, current, k)
				st.classes[cl]++
				if _, ok := st.examples[cl]; !ok {
					st.examples[cl] = fmt.Sprintf("%s xff%g seal%d a%d slot%d want%v got%v", ladderString(c), c.XFilesFactor, si, k.archive, k.slot, closed[k], current[k])
				}
			}
			if sameSlot(closed, versions, k) {
				st.versionsOK++
			}
			if sameSlot(closed, mergedImg, k) {
				st.mergedOK++
			} else if st.firstMergedMiss == "" {
				st.firstMergedMiss = fmt.Sprintf("%s m%d xff%g seal%d a%d slot%d want%v got%v", ladderString(c), c.Method, c.XFilesFactor, si, k.archive, k.slot, closed[k], mergedImg[k])
			}
		}
	}
}

// projectOpenImage is the read surface of the current v2 path: stored closed
// records plus ProjectOpen's virtual open windows (the engine's read rule).
func projectOpenImage(c Config, state map[key]Sample, maxTime, through int64) map[key]Sample {
	records := make([]Record, 0, len(state))
	for k, p := range state {
		records = append(records, Record{Delta: Delta{Archive: k.archive, Slot: k.slot, Order: p.Sequence, Sample: p}})
	}
	out := map[key]Sample{}
	for _, r := range ProjectOpen(c, maxTime, through, records) {
		out[key{r.Archive, r.Slot}] = r.Sample
	}
	return out
}

func ladderString(c Config) string {
	var parts []string
	for _, a := range c.Archives {
		parts = append(parts, fmt.Sprintf("%ds:%d", a.Step, a.Count))
	}
	return strings.Join(parts, ",")
}

func pct(n, d int) string {
	if d == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.4f%%", 100*float64(n)/float64(d))
}

func runFnCorpus(t *testing.T, corpus fnCorpus) fnStats {
	t.Helper()
	st := fnStats{classes: map[string]int{}, examples: map[string]string{}}
	mergeRng := randv2.New(randv2.NewPCG(1, 2))
	for _, m := range corpus.metrics {
		runFnMetric(t, m, mergeRng, &st)
	}
	t.Logf("RESULT corpus=%s metrics=%d comparisons=%d coarse_slots=%d closed_vs_whisper=%s current_records=%s all_versions=%s merged_materialized=%s versions_vs_whisper=%s merged_vs_whisper=%s merges=%d dropped=%d materialized=%d window_evals=%d older_event_evals=%d",
		corpus.name, len(corpus.metrics), st.compares, st.coarse, pct(st.closedOK, st.coarse), pct(st.currentOK, st.coarse), pct(st.versionsOK, st.coarse), pct(st.mergedOK, st.coarse), pct(st.versionsWhisperOK, st.coarse), pct(st.mergedWhisperOK, st.coarse), st.merges, st.drops, st.mat, st.walks, st.deepWalks)
	if st.firstVersionsMiss != "" {
		t.Logf("RESULT corpus=%s shortest versions-vs-whisper miss: %.4000s", corpus.name, st.firstVersionsMiss)
	}
	if st.firstClosedMiss != "" {
		t.Logf("RESULT corpus=%s first closed-vs-whisper miss: %.3000s", corpus.name, st.firstClosedMiss)
	}
	for cl, n := range st.classes {
		t.Logf("RESULT corpus=%s current_records_miss class=%q slots=%d example: %s", corpus.name, cl, n, st.examples[cl])
	}
	if st.versionsWhisperOK != st.coarse {
		t.Errorf("%s: all-versions function misses %d of %d slots; shortest: %.2000s", corpus.name, st.coarse-st.versionsWhisperOK, st.coarse, st.firstVersionsMiss)
	}
	if st.mergedWhisperOK != st.coarse {
		t.Errorf("%s: merged function misses %d of %d slots", corpus.name, st.coarse-st.mergedWhisperOK, st.coarse)
	}
	return st
}

// TestRollupAsFunctionCorpora is the B3 oracle on the six differential corpora.
func TestRollupAsFunctionCorpora(t *testing.T) {
	for _, gen := range []func() fnCorpus{fnHandoff, fnDigest, fnHistory, fnSlim, fnRank, fnBackground} {
		corpus := gen()
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			if st := runFnCorpus(t, corpus); st.closedOK != st.coarse {
				t.Errorf("%s: current v2 path differs from eager Whisper; first: %.2000s", corpus.name, st.firstClosedMiss)
			}
		})
	}
}

// TestRollupAsFunctionProperty is the randomized property test.
func TestRollupAsFunctionProperty(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	if testing.Short() {
		seeds = []uint64{3, 8} // both hit the chain rule
	}
	for _, seed := range seeds {
		corpus := fnRandom(seed, 60)
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			runFnCorpus(t, corpus)
		})
	}
}

// fnImages runs one metric and calls check after every seal with the eager
// Whisper image and the images under test.
func fnImages(m fnMetric, mergeSeed uint64, check func(si int, whisperImg, closed, versions, merged map[key][2]uint64, store []rawRec) bool) {
	c := m.cfg
	p, err := Prepare(c)
	if err != nil {
		panic(err)
	}
	mergeRng := randv2.New(randv2.NewPCG(mergeSeed, 2))
	eager, deferred := map[key]Sample{}, map[key]Sample{}
	var old, frontier Frontier
	var through int64
	var store []rawRec
	mg := &fnStore{c: c}
	for si, seal := range m.seals {
		if len(seal) == 0 {
			continue
		}
		want, err := p.Plan(old, view(eager), nil, seal)
		if err != nil {
			panic(err)
		}
		got, nextThrough, err := p.PlanClosedInto(nil, frontier, through, 0, view(deferred), nil, seal, nil)
		if err != nil {
			panic(err)
		}
		for _, r := range want.Records {
			install(eager, []Delta{r.Delta})
		}
		for _, r := range got.Records {
			k := key{r.Archive, r.Slot}
			if o, ok := deferred[k]; !ok || r.Order >= o.Sequence {
				s := r.Sample
				s.Sequence = r.Order
				deferred[k] = s
			}
		}
		old, frontier, through = want.Next, got.Next, nextThrough
		part := rawPart(c, seal)
		store = append(store, part...)
		mg.recs = append(mg.recs, part...)
		if mergeRng.IntN(3) == 0 {
			mg.merge()
		}
		all := &fnStore{c: c, recs: store}
		if !check(si, logical(eager), logical(projectOpenImage(c, deferred, frontier.MaxTime, through)), logical(all.image()), logical(mg.image()), store) {
			return
		}
	}
}

// fnMinimize removes seals, batches and points while miss still holds.
func fnMinimize(m fnMetric, miss func(fnMetric) bool) fnMetric {
	clone := func(m fnMetric) fnMetric {
		out := fnMetric{cfg: m.cfg}
		for _, s := range m.seals {
			var seal []Batch
			for _, b := range s {
				b.Points = slices.Clone(b.Points)
				seal = append(seal, b)
			}
			out.seals = append(out.seals, seal)
		}
		return out
	}
	for changed := true; changed; {
		changed = false
		for i := 0; i < len(m.seals); i++ {
			try := clone(m)
			try.seals = slices.Delete(try.seals, i, i+1)
			if miss(try) {
				m, changed, i = try, true, i-1
			}
		}
		for i := range m.seals {
			for j := 0; j < len(m.seals[i]); j++ {
				try := clone(m)
				try.seals[i] = slices.Delete(try.seals[i], j, j+1)
				if miss(try) {
					m, changed, j = try, true, j-1
				}
			}
		}
		for i := range m.seals {
			for j := range m.seals[i] {
				for k := 0; k < len(m.seals[i][j].Points); k++ {
					try := clone(m)
					try.seals[i][j].Points = slices.Delete(try.seals[i][j].Points, k, k+1)
					if miss(try) {
						m, changed, k = try, true, k-1
					}
				}
			}
		}
		// Split multi-batch seals into one batch per seal.
		for i := 0; i < len(m.seals); i++ {
			if len(m.seals[i]) < 2 {
				continue
			}
			try := clone(m)
			var split [][]Batch
			for _, b := range try.seals[i] {
				split = append(split, []Batch{b})
			}
			try.seals = slices.Concat(try.seals[:i], split, try.seals[i+1:])
			if miss(try) {
				m, changed = try, true
			}
		}
	}
	return m
}

func fnDescribe(m fnMetric) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ladder %s method %d xff %g\n", ladderString(m.cfg), m.cfg.Method, m.cfg.XFilesFactor)
	for i, s := range m.seals {
		for _, bt := range s {
			fmt.Fprintf(&b, "  seal %d order %d now %d:", i, bt.Order, bt.Now)
			for _, p := range bt.Points {
				fmt.Fprintf(&b, " (t%+d v%g s%d)", p.Time-bt.Now, math.Float64frombits(p.Bits), p.Sequence)
			}
			b.WriteByte('\n')
		}
	}
	return b.String()
}

var fnMinCases = 10

// TestRollupAsFunctionMinimize prints minimal failing cases (FN_DEBUG=1).
func TestRollupAsFunctionMinimize(t *testing.T) {
	if !testing.Verbose() || os.Getenv("FN_DEBUG") == "" {
		t.Skip("set FN_DEBUG=1 and -v")
	}
	kinds := map[string]func(w, c, v, mg map[key][2]uint64, k key) bool{
		"closed": func(w, c, v, mg map[key][2]uint64, k key) bool {
			return os.Getenv("FN_CLOSED") != "" && !sameSlot(w, c, k)
		},
		"versions": func(w, c, v, mg map[key][2]uint64, k key) bool { return !sameSlot(w, v, k) },
		"merged":   func(w, c, v, mg map[key][2]uint64, k key) bool { return sameSlot(w, v, k) && !sameSlot(w, mg, k) },
	}
	for kind, bad := range kinds {
		found := 0
		for seed := uint64(1); seed <= 8 && found < fnMinCases; seed++ {
			for _, m := range fnRandom(seed, 60).metrics {
				miss := func(m fnMetric) (hit bool) {
					fnImages(m, 1, func(_ int, w, c, v, mg map[key][2]uint64, _ []rawRec) bool {
						keys := map[key]bool{}
						for _, img := range []map[key][2]uint64{w, c, v, mg} {
							for k := range img {
								keys[k] = true
							}
						}
						for k := range keys {
							if k.archive > 0 && bad(w, c, v, mg, k) {
								hit = true
								return false
							}
						}
						return true
					})
					return hit
				}
				if !miss(m) {
					continue
				}
				small := fnMinimize(m, miss)
				var detail []string
				fnImages(small, 1, func(si int, w, c, v, mg map[key][2]uint64, _ []rawRec) bool {
					for k := range w {
						if k.archive > 0 && bad(w, c, v, mg, k) {
							detail = append(detail, fmt.Sprintf("seal%d a%d slot%d whisper%v closed%v versions%v merged%v", si, k.archive, k.slot, w[k], c[k], v[k], mg[k]))
						}
					}
					extra := map[key]bool{}
					for k := range v {
						extra[k] = true
					}
					for k := range mg {
						extra[k] = true
					}
					for k := range extra {
						if _, ok := w[k]; !ok && k.archive > 0 && bad(w, c, v, mg, k) {
							detail = append(detail, fmt.Sprintf("seal%d a%d slot%d whisper-absent closed%v versions%v merged%v", si, k.archive, k.slot, c[k], v[k], mg[k]))
						}
					}
					return len(detail) == 0
				})
				t.Logf("MIN %s seed%d:\n%s  %s", kind, seed, fnDescribe(small), strings.Join(detail, "\n  "))
				found++
				if found >= fnMinCases {
					break
				}
			}
		}
	}
}
