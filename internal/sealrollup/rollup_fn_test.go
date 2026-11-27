package sealrollup

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"os"
	"slices"
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
//   - Versions over all versions: a coarse window is the value of its newest
//     successful Whisper event, its finer slots resolved as of that event.
//   - Versions after merges: before a merge drops superseded versions it writes
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

// rawPart is one seal's part: every batch routed by its own admission clock
// and aligned, with the Whisper order inside the batch (stable by time, then
// source sequence; the last write of a physical slot wins). The part keeps one
// record per (physical slot, batch); only merges drop superseded versions, so
// a merge, not the seal, decides when an older version may go.
func rawPart(c Config, batches []Batch) []Version {
	var out []Version
	for _, b := range batches {
		out = append(out, rawBatch(c, b)...)
	}
	return out
}

// rawBatch routes and aligns one batch; the last write of a physical slot wins.
func rawBatch(c Config, b Batch) []Version {
	points := slices.Clone(b.Points)
	slices.SortStableFunc(points, func(a, z Sample) int {
		if a.Time != z.Time {
			return cmpInt(a.Time, z.Time)
		}
		return cmpInt(a.Sequence, z.Sequence)
	})
	last := map[key]int{}
	var recs, ghosts []Version
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
		tick := Tick(b.Order, target)
		r := Version{Archive: target, Time: t, Bits: p.Bits, Order: tick, Upto: tick}
		if i, ok := last[k]; ok {
			if recs[i].Time != t {
				g := recs[i]
				g.Ghost = true
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

// fnCurrent derives every archive from current records alone.
func fnCurrent(c Config, recs []Version) map[key]Sample {
	type entry struct {
		s      Sample
		stored bool
	}
	img := map[key]entry{}
	for _, r := range recs {
		if r.Ghost {
			continue
		}
		k := key{r.Archive, slot(r.Time, c.Archives[r.Archive])}
		if old, ok := img[k]; !ok || newer(r.Order, true, r.Time, old.s.Sequence, true, old.s.Time) {
			img[k] = entry{Sample{r.Time, r.Bits, r.Order}, true}
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

func (f *Versions) image() map[key]Sample {
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

// merge is a merge of the whole store (WriteDown); it returns the number of
// dropped and materialized records.
func (f *Versions) merge() (dropped, materialized int) {
	keep, mats := WriteDown(f.c, f.recs, nil)
	kept := f.recs[:0:0]
	for i, r := range f.recs {
		if keep[i] {
			kept = append(kept, r)
		}
	}
	f.recs = append(kept, mats...)
	return len(keep) - len(kept), len(mats)
}

// ---- corpora ----

type fnMetric struct {
	cfg   Config
	seals [][]Batch // per seal, this metric's batches in increasing order
	// mutate, when set, rewrites the current v2 path's image after every
	// seal (mutation tests of the comparison rule only).
	mutate func(closed map[key][2]uint64, lost map[key]bool)
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
	slots              int // compared slots, fine and coarse
	ringLoss           int // compared slots (fine and coarse) where Whisper lost an older point to a future point
	ringLossDiffers    int // of those, slots where the current v2 path differs from Whisper (allowed)
	unexplained        int // current v2 path vs Whisper misses outside ring-loss slots (must be 0)
}

// ringLoss is one slot where eager Whisper lost an older point to a point
// that was in the future when written (Time > its batch Now): Whisper archives
// are rings, so the future point takes the physical slot of a point one ring
// earlier that is still inside retention. v2's contract keeps the older point (no ring), so the slot,
// and each coarser window built over the older point, may differ from Whisper
// while the slot still holds the ring winner (holds) and the older point is
// still inside its archive's retention (span) at the current admission clock;
// every other slot must stay equal.
type ringLoss struct{ older, holds, span int64 }

// futureRingLoss records the seal's ring losses; before is the eager image
// before the seal, after the image once it is installed.
func futureRingLoss(c Config, seal []Batch, before, after map[key]Sample, lost map[key]ringLoss) {
	for _, b := range seal {
		for _, p := range b.Points {
			if p.Time <= b.Now {
				continue
			}
			for a, arch := range c.Archives {
				k := key{a, slot(p.Time, arch)}
				old, had := before[k]
				w := align(p.Time, arch.Step)
				if !had || old.Time == w || after[k].Time != w || old.Time <= b.Now-arch.Step*arch.Count {
					continue // ordinary ring reuse: the older point had left retention
				}
				lost[k] = ringLoss{old.Time, w, arch.Step * arch.Count}
				for up := a + 1; up < len(c.Archives); up++ {
					u := c.Archives[up]
					lost[key{up, slot(old.Time, u)}] = ringLoss{old.Time, align(old.Time, u.Step), arch.Step * arch.Count}
				}
			}
		}
	}
}

// inRingLoss reports whether slot k of the Whisper image is a live ring loss
// at admission clock now.
func inRingLoss(lost map[key]ringLoss, whisper map[key][2]uint64, k key, now int64) bool {
	l, ok := lost[k]
	return ok && int64(whisper[k][0]) == l.holds && l.older > now-l.span
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
func classify(c Config, recs []Version, want, cur map[key][2]uint64, k key) string {
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
	newest := map[key]Version{}
	for _, r := range recs {
		if r.Ghost {
			continue
		}
		pk := key{r.Archive, slot(r.Time, c.Archives[r.Archive])}
		if o, ok := newest[pk]; !ok || r.Order > o.Order || r.Order == o.Order && r.Time > o.Time {
			newest[pk] = r
		}
	}
	for _, r := range recs {
		if r.Ghost && r.Archive < a && align(r.Time, c.Archives[a].Step) == w {
			return "same-batch slot takeover (ghost event)"
		}
	}
	for _, r := range recs {
		if !r.Ghost && r.Archive < a && align(r.Time, c.Archives[a].Step) == w {
			if n := newest[key{r.Archive, slot(r.Time, c.Archives[r.Archive])}]; n.Time != r.Time {
				return "partial finer expiry (superseded input)"
			}
		}
	}
	for _, r := range recs {
		if !r.Ghost && r.Archive == a && r.Time == w {
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
	var store, routed []Version
	merged := &Versions{c: c}
	lost := map[key]ringLoss{}
	var clock int64 // greatest admission clock so far
	for si, seal := range m.seals {
		if len(seal) == 0 {
			continue
		}
		want, err := p.Plan(old, view(eager), nil, seal)
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.RouteInto(nil, frontier, seal, nil)
		if err != nil {
			t.Fatal(err)
		}
		before := maps.Clone(eager)
		for _, r := range want.Records {
			install(eager, []Delta{r.Delta})
		}
		futureRingLoss(c, seal, before, eager, lost)
		for _, b := range seal {
			clock = max(clock, b.Now)
		}
		// The engine resolves a physical slot by the greatest record order across
		// parts (later part on ties), not by install sequence.
		for _, r := range got.Winners() {
			k := key{r.Archive, r.Slot}
			if o, ok := deferred[k]; !ok || r.Order >= o.Sequence {
				s := r.Sample
				s.Sequence = r.Order
				deferred[k] = s
			}
		}
		old, frontier, through = want.Next, got.Next, max(through, got.Next.MaxTime)
		routed = append(routed, recordVersions(c, got.Records)...)
		closed := logical(NewVersions(c, routed).image())
		whisperImg := logical(eager)
		live := map[key]bool{}
		for k := range lost {
			if inRingLoss(lost, whisperImg, k, clock) {
				live[k] = true
			}
		}
		if m.mutate != nil {
			m.mutate(closed, live)
		}

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
		all := &Versions{c: c, recs: store}
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
			st.slots++
			if live[k] {
				st.ringLoss++
				if !sameSlot(whisperImg, closed, k) {
					st.ringLossDiffers++
				}
			} else if !sameSlot(whisperImg, closed, k) {
				st.unexplained++
			}
			if k.archive == 0 {
				if !live[k] && !sameSlot(whisperImg, closed, k) || !sameSlot(closed, current, k) || !sameSlot(closed, versions, k) || !sameSlot(closed, mergedImg, k) {
					t.Fatalf("seal %d: fine slot %v differs", si, k)
				}
				continue
			}
			st.coarse++
			if live[k] || sameSlot(whisperImg, closed, k) {
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
	for _, r := range projectOpen(c, maxTime, through, records) {
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
	t.Logf("RESULT corpus=%s future_ring_loss_slots=%d (%s of compared) where_v2_differs=%d unexplained_v2_vs_whisper=%d", corpus.name, st.ringLoss, pct(st.ringLoss, st.slots), st.ringLossDiffers, st.unexplained)
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
			if st := runFnCorpus(t, corpus); st.unexplained != 0 || st.closedOK != st.coarse || st.versionsWhisperOK != st.coarse || st.mergedWhisperOK != st.coarse {
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

// TestFutureRingLossRule checks the oracle's future-point rule with mutations
// of the current v2 path's image: changing every coarse ring-loss slot (v2
// keeping the older point instead of Whisper's ring winner) is allowed, while
// changing one coarse slot outside them is an unexplained difference.
func TestFutureRingLossRule(t *testing.T) {
	corpus := fnRandom(8, 60) // seeds 3 and 7 each keep one older closed miss outside ring loss
	run := func(mutate func(map[key][2]uint64, map[key]bool)) fnStats {
		st := fnStats{classes: map[string]int{}, examples: map[string]string{}}
		rng := randv2.New(randv2.NewPCG(1, 2))
		for _, m := range corpus.metrics {
			m.mutate = mutate
			runFnMetric(t, m, rng, &st)
		}
		return st
	}
	base := run(nil)
	if base.ringLoss == 0 || base.unexplained != 0 {
		t.Fatalf("baseline: ring-loss slots %d unexplained %d, want some and 0", base.ringLoss, base.unexplained)
	}
	kept := run(func(closed map[key][2]uint64, lost map[key]bool) {
		for k := range lost {
			if k.archive > 0 {
				closed[k] = [2]uint64{closed[k][0], closed[k][1] ^ 1}
			}
		}
	})
	if kept.ringLossDiffers == 0 || kept.unexplained != 0 {
		t.Fatalf("ring-loss slots changed: differs %d unexplained %d, want some and 0", kept.ringLossDiffers, kept.unexplained)
	}
	once := false
	other := run(func(closed map[key][2]uint64, lost map[key]bool) {
		for k, v := range closed {
			if !once && k.archive > 0 && !lost[k] {
				closed[k], once = [2]uint64{v[0], v[1] ^ 1}, true
			}
		}
	})
	if other.unexplained == 0 {
		t.Fatal("a changed slot outside ring loss went unnoticed")
	}
	t.Logf("RESULT ring-loss rule: baseline ring_loss=%d of %d slots (%s) where_v2_differs=%d; mutation kept-older differs=%d unexplained=%d; mutation other-slot unexplained=%d",
		base.ringLoss, base.slots, pct(base.ringLoss, base.slots), base.ringLossDiffers, kept.ringLossDiffers, kept.unexplained, other.unexplained)
}

// fnImages runs one metric and calls check after every seal with the eager
// Whisper image and the images under test.
func fnImages(m fnMetric, mergeSeed uint64, check func(si int, whisperImg, closed, versions, merged map[key][2]uint64, store []Version) bool) {
	c := m.cfg
	p, err := Prepare(c)
	if err != nil {
		panic(err)
	}
	mergeRng := randv2.New(randv2.NewPCG(mergeSeed, 2))
	eager, deferred := map[key]Sample{}, map[key]Sample{}
	var old, frontier Frontier
	var through int64
	var store, routed []Version
	mg := &Versions{c: c}
	for si, seal := range m.seals {
		if len(seal) == 0 {
			continue
		}
		want, err := p.Plan(old, view(eager), nil, seal)
		if err != nil {
			panic(err)
		}
		got, err := p.RouteInto(nil, frontier, seal, nil)
		if err != nil {
			panic(err)
		}
		for _, r := range want.Records {
			install(eager, []Delta{r.Delta})
		}
		for _, r := range got.Winners() {
			k := key{r.Archive, r.Slot}
			if o, ok := deferred[k]; !ok || r.Order >= o.Sequence {
				s := r.Sample
				s.Sequence = r.Order
				deferred[k] = s
			}
		}
		old, frontier, through = want.Next, got.Next, max(through, got.Next.MaxTime)
		part := rawPart(c, seal)
		store = append(store, part...)
		mg.recs = append(mg.recs, part...)
		if mergeRng.IntN(3) == 0 {
			mg.merge()
		}
		all := &Versions{c: c, recs: store}
		routed = append(routed, recordVersions(c, got.Records)...)
		if !check(si, logical(eager), logical(NewVersions(c, routed).image()), logical(all.image()), logical(mg.image()), store) {
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
					fnImages(m, 1, func(_ int, w, c, v, mg map[key][2]uint64, _ []Version) bool {
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
				fnImages(small, 1, func(si int, w, c, v, mg map[key][2]uint64, _ []Version) bool {
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

// Open-window read (arch-review step C3a). The read computes a coarse slot
// that is not materialized with SlotValue over the versions WindowRanges
// selects. Three part contents are compared with eager Whisper on every open
// coarse window after every seal, with ProjectOpen (the rule the read would
// replace) counted alongside:
//
//   - c3: what a sort + encode seal stores (rawPart: one version per slot per
//     batch, ghosts, (batch, target archive) orders, no seal-computed
//     windows). The read must be exact here: this is the gate.
//   - parts: what the current seal stores (PlanClosedInto records, one per
//     slot per seal, batch orders, closed windows indistinguishable from
//     direct writes), mapped with recordVersions. Reported only.
//   - parts+batches: the current seal's records plus every per-batch version
//     and ghost (a kind bit tells its computed windows from direct writes).
//     Reported only.
//   - parts+batches-fallback: as parts+batches, but a seal-computed window is
//     a fallback only: it bounds no event walk and loses order ties to the
//     computed value. Reported only.
//
// openStores are the part contents the open-window read is measured on. seal
// is what PlanClosedInto writes (one version per slot per batch, Ghosts,
// closed windows as fallback Mat records), read through Record.Version, and
// must match Whisper on every slot; so must seal+merged, the same parts after
// a merge of every part so far at every second seal (WriteDown, its windows
// through the stored record form). parts+batches-fallback models it with
// guessed kinds and a stronger fallback order.
var openStores = []string{"c3", "seal", "seal+merged", "seal-lazy", "seal+merged-lazy", "seal+merged-wd-lazy", "seal+up-lazy", "seal+merged-open", "seal+up-open", "parts", "parts+batches", "parts+batches-fallback"}

func TestOpenWindowRead(t *testing.T) {
	gens := []func() fnCorpus{fnHandoff, fnDigest, fnHistory, fnSlim, fnRank, fnBackground}
	seeds := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	if testing.Short() {
		seeds = []uint64{3, 8}
	}
	for _, seed := range seeds {
		gens = append(gens, func() fnCorpus { return fnRandom(seed, 60) })
	}
	for _, gen := range gens {
		corpus := gen()
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			stores := openStores
			if testing.Short() {
				stores = stores[:5]
			}
			for _, store := range stores {
				var slots, readOK, projectOK, worse int
				var firstMiss string
				var loaded [2]int64 // versions read: eager over WindowRanges, lazy descent
				for _, m := range corpus.metrics {
					openWindowRead(t, m, store, &loaded, func(si, a int, o openOutcome) {
						slots++
						if o.projectOK() {
							projectOK++
						}
						if o.readOK() {
							readOK++
							return
						}
						if o.projectOK() {
							worse++
						}
						if firstMiss == "" {
							firstMiss = fmt.Sprintf("%s m%d xff%g seal%d a%d whisper%v read%v history %+v", ladderString(m.cfg), m.cfg.Method, m.cfg.XFilesFactor, si, a, o.want, o.read, m.seals[:si+1])
						}
					})
				}
				t.Logf("RESULT open-window store=%s corpus=%s slots=%d read_vs_whisper=%s projectopen_vs_whisper=%s read_misses_where_projectopen_matches=%d versions_eager=%d versions_lazy=%d", store, corpus.name, slots, pct(readOK, slots), pct(projectOK, slots), worse, loaded[0], loaded[1])
				if store == "c3" && readOK != slots {
					t.Errorf("%s: open-window read misses %d of %d slots; first: %.3000s", corpus.name, slots-readOK, slots, firstMiss)
				}
				if (store == "seal" || store == "seal+merged" || strings.HasSuffix(store, "-open") || os.Getenv("FN_LAZY_GATE") != "" && strings.HasSuffix(store, "-lazy")) && readOK != slots {
					t.Errorf("%s: open-window read of sealed parts misses %d of %d slots (%d where ProjectOpen matches); first: %.3000s", corpus.name, slots-readOK, slots, worse, firstMiss)
				}
			}
		})
	}
}

// openOutcome is one open coarse window's physical slot: eager Whisper, the
// read under test and ProjectOpen, each as (time, bits) and presence.
type openOutcome struct {
	want, read, project      [2]uint64
	wantOK, readHas, projHas bool
}

func (o openOutcome) readOK() bool    { return o.readHas == o.wantOK && o.read == o.want }
func (o openOutcome) projectOK() bool { return o.projHas == o.wantOK && o.project == o.want }

// recordVersions maps current-format records to versions: a record's target
// archive is its own; a coarse record whose batch order equals a finer
// record's inside its window counts as a window the seal computed (at the
// finer target's sub-step) and is no event. The format carries neither the
// target archive nor the kind, so this is a best guess.
func recordVersions(c Config, recs []Record) []Version {
	type event struct {
		a int
		w int64
		o uint64
	}
	finer := map[event]bool{}
	for _, r := range recs {
		for a := r.Archive + 1; a < len(c.Archives); a++ {
			finer[event{a, align(r.Sample.Time, c.Archives[a].Step), r.Order}] = true
		}
	}
	out := make([]Version, len(recs))
	for i, r := range recs {
		v := Version{Archive: r.Archive, Time: r.Sample.Time, Bits: r.Sample.Bits, Order: Tick(r.Order, r.Archive)}
		if r.Archive > 0 && finer[event{r.Archive, r.Sample.Time, r.Order}] {
			v.Order, v.Mat = Tick(r.Order, r.Archive-1), true
		}
		v.Upto = v.Order
		out[i] = v
	}
	return out
}

// openWindowRead seals m with the current seal, keeps the part content store
// names, and calls check for each open coarse window after every seal.
func openWindowRead(t *testing.T, m fnMetric, store string, loaded *[2]int64, check func(si, a int, o openOutcome)) {
	t.Helper()
	c := m.cfg
	p, err := Prepare(c)
	if err != nil {
		t.Fatal(err)
	}
	eager, deferred := map[key]Sample{}, map[key]Sample{}
	var old, frontier Frontier
	var through int64
	var stored []Version
	var storedRecs []Record
	for si, seal := range m.seals {
		if len(seal) == 0 {
			continue
		}
		want, err := p.Plan(old, view(eager), nil, seal)
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.RouteInto(nil, frontier, seal, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range want.Records {
			install(eager, []Delta{r.Delta})
		}
		for _, r := range got.Winners() {
			k := key{r.Archive, r.Slot}
			if o, ok := deferred[k]; !ok || r.Order >= o.Sequence {
				s := r.Sample
				s.Sequence = r.Order
				deferred[k] = s
			}
		}
		switch store {
		case "c3":
			stored = append(stored, rawPart(c, seal)...)
		case "seal-lazy", "seal+merged-lazy", "seal+merged-wd-lazy", "seal+up-lazy", "seal+merged-open", "seal+up-open":
			storedRecs = append(storedRecs, got.Records...)
			if store != "seal-lazy" && si%2 == 1 {
				vs := make([]Version, len(storedRecs))
				for i, r := range storedRecs {
					vs[i] = r.Version()
				}
				keep, mats := WriteDown(c, vs, nil)
				kept := storedRecs[:0:0]
				for i, r := range storedRecs {
					if keep[i] {
						kept = append(kept, r)
					}
				}
				for _, v := range mats {
					kept = append(kept, v.Record(c))
				}
				storedRecs = kept
				if strings.HasPrefix(store, "seal+up") {
					storedRecs = append(storedRecs, upgradeClosed(c, storedRecs)...)
				}
			}
		case "seal", "seal+merged":
			for _, r := range got.Records {
				stored = append(stored, r.Version())
			}
			if store == "seal+merged" && si%2 == 1 {
				// A merge of every part so far, its windows through the
				// stored record form.
				keep, mats := WriteDown(c, stored, nil)
				kept := stored[:0:0]
				for i, v := range stored {
					if keep[i] {
						kept = append(kept, v)
					}
				}
				for _, v := range mats {
					kept = append(kept, v.Record(c).Version())
				}
				stored = kept
			}
		case "parts":
			stored = append(stored, recordVersions(c, got.Winners())...)
		case "parts+batches", "parts+batches-fallback":
			raw := rawPart(c, seal)
			for _, v := range recordVersions(c, got.Winners()) {
				if v.Mat {
					if store == "parts+batches-fallback" {
						// A seal-computed window is only a fallback: it bounds
						// no walk and loses ties to the computed value.
						v.Order, v.Upto = v.Order-1, 0
					}
					stored = append(stored, v)
				}
			}
			stored = append(stored, raw...)
		}
		old, frontier, through = want.Next, got.Next, max(through, got.Next.MaxTime)
		projected := projectOpenImage(c, deferred, frontier.MaxTime, through)
		for a := 1; a < len(c.Archives); a++ {
			w := align(frontier.MaxTime, c.Archives[a].Step)
			if w+c.Archives[a].Step <= through {
				continue
			}
			var sub []Version
			if err := WindowRanges(c, a, w, func(l int, first, last int64) error {
				for _, v := range stored {
					if s := slot(v.Time, c.Archives[v.Archive]); v.Archive == l && s >= first && s < last {
						sub = append(sub, v)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			k := key{a, slot(w, c.Archives[a])}
			read, readHas := NewVersions(c, sub).SlotValue(a, k.slot)
			loaded[0] += int64(len(sub))
			if strings.HasSuffix(store, "-open") {
				read, readHas, err = OpenSlot(c, a, w, func(l int, first, last int64, _ uint64) ([]Record, error) {
					var out []Record
					defer func() { loaded[1] += int64(len(out)) }()
					for _, r := range storedRecs {
						if r.Archive == l && r.Slot >= first && r.Slot < last {
							out = append(out, r)
						}
					}
					return out, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasSuffix(store, "-lazy") {
				settles := func(_ int, r Record) bool { return true } // any Mat, seal windows too
				switch {
				case strings.Contains(store, "-wd"):
					settles = func(_ int, r Record) bool { return r.Upto > 0 }
				case strings.Contains(store, "+up"):
					// A written-down window settles only while no stored
					// version under it is newer than its Upto: the read's
					// per-part order bound, here over the whole store.
					settles = func(l int, r Record) bool {
						if r.Upto == 0 {
							return false
						}
						end := r.Sample.Time + c.Archives[l].Step
						for _, x := range storedRecs {
							if x.Archive < l && x.Sample.Time >= r.Sample.Time && x.Sample.Time < end && x.Version().Order > r.Upto {
								return false
							}
						}
						return true
					}
				}
				read, readHas, err = openSlot(c, a, w, settles, func(l int, first, last int64) ([]Record, error) {
					var out []Record
					defer func() { loaded[1] += int64(len(out)) }()
					for _, r := range storedRecs {
						if r.Archive == l && r.Slot >= first && r.Slot < last {
							out = append(out, r)
						}
					}
					return out, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			wantS, wantOK := eager[k]
			proj, projHas := projected[k]
			check(si, a, openOutcome{
				want: [2]uint64{uint64(wantS.Time), wantS.Bits}, read: [2]uint64{uint64(read.Time), read.Bits}, project: [2]uint64{uint64(proj.Time), proj.Bits},
				wantOK: wantOK, readHas: readHas, projHas: projHas,
			})
		}
	}
}

// TestOpenWindowMinimize shrinks open-window misses of FN_OPEN_STORE (default
// parts); FN_WORSE=1 keeps only misses where ProjectOpen matches Whisper.
func TestOpenWindowMinimize(t *testing.T) {
	if !testing.Verbose() || os.Getenv("FN_DEBUG") == "" {
		t.Skip("set FN_DEBUG=1 and -v")
	}
	store := cmp.Or(os.Getenv("FN_OPEN_STORE"), "parts")
	bad := func(o openOutcome) bool { return !o.readOK() && (os.Getenv("FN_WORSE") == "" || o.projectOK()) }
	found := 0
	for seed := uint64(1); seed <= 8 && found < fnMinCases; seed++ {
		for _, m := range fnRandom(seed, 60).metrics {
			miss := func(m fnMetric) (hit bool) {
				openWindowRead(t, m, store, new([2]int64), func(_, _ int, o openOutcome) { hit = hit || bad(o) })
				return hit
			}
			if !miss(m) {
				continue
			}
			small := fnMinimize(m, miss)
			var detail []string
			openWindowRead(t, small, store, new([2]int64), func(si, a int, o openOutcome) {
				if bad(o) {
					detail = append(detail, fmt.Sprintf("seal%d a%d whisper%v/%v read%v/%v projectopen%v/%v", si, a, o.want, o.wantOK, o.read, o.readHas, o.project, o.projHas))
				}
			})
			t.Logf("MIN open store=%s seed%d:\n%s  %s", store, seed, fnDescribe(small), strings.Join(detail, "\n  "))
			if found++; found >= fnMinCases {
				break
			}
		}
	}
}

// BenchmarkOpenSlotRead is the read cost of one open coarse slot: the versions
// WindowRanges selects (one per fine slot, seal-computed closed windows as
// materialized records) indexed and evaluated with SlotValue.
func BenchmarkOpenSlotRead(b *testing.B) {
	for _, def := range []string{"10s:6h,1m:7d,10m:5y", "1m:30d,1h:1y,1d:10y", "1s:1d,1m:30d,1h:1y,1d:10y"} {
		c := config(def, Average, .5)
		now := int64(1794700800 + 86400 - 7)
		var all []Version
		order := uint64(1)
		for ts := now - c.Archives[0].Step*c.Archives[0].Count + c.Archives[0].Step; ts <= now; ts += c.Archives[0].Step {
			all = append(all, Version{Archive: 0, Time: align(ts, c.Archives[0].Step), Bits: uint64(ts), Order: Tick(order, 0), Upto: Tick(order, 0)})
			order++
		}
		for a := 1; a < len(c.Archives); a++ {
			open := align(now, c.Archives[a].Step)
			for w := open - c.Archives[a].Step*(c.Archives[a].Count-1); w < open; w += c.Archives[a].Step {
				o := Tick(uint64(max(1, (w+c.Archives[a].Step-1-(now-int64(order)+1)))), a-1)
				all = append(all, Version{Archive: a, Time: w, Bits: uint64(w), Order: o, Upto: o, Mat: true})
			}
		}
		for a := 1; a < len(c.Archives); a++ {
			w := align(now, c.Archives[a].Step)
			var sub []Version
			_ = WindowRanges(c, a, w, func(l int, first, last int64) error {
				for _, v := range all {
					if s := slot(v.Time, c.Archives[v.Archive]); v.Archive == l && s >= first && s < last {
						sub = append(sub, v)
					}
				}
				return nil
			})
			b.Run(fmt.Sprintf("%s/archive%d/versions%d", def, a, len(sub)), func(b *testing.B) {
				for range b.N {
					if _, ok := NewVersions(c, sub).SlotValue(a, slot(w, c.Archives[a])); !ok {
						b.Fatal("open slot empty")
					}
				}
			})
			// The lazy read over the same versions, closed windows written
			// down by a merge: it reads the open child chain only.
			recs := make([]Record, len(all))
			for i, v := range all {
				recs[i] = v.Record(c)
			}
			var loaded int
			load := func(l int, first, last int64, from uint64) ([]Record, error) {
				var out []Record
				for _, r := range recs {
					if r.Archive == l && r.Slot >= first && r.Slot < last && r.Order >= from {
						out = append(out, r)
					}
				}
				loaded += len(out)
				return out, nil
			}
			want, _ := NewVersions(c, sub).SlotValue(a, slot(w, c.Archives[a]))
			if got, ok, _ := OpenSlot(c, a, w, load); !ok || got != want {
				b.Fatalf("lazy read %+v, eager %+v", got, want)
			}
			b.Run(fmt.Sprintf("%s/archive%d/lazy", def, a), func(b *testing.B) {
				loaded = 0
				for range b.N {
					if _, ok, _ := OpenSlot(c, a, w, load); !ok {
						b.Fatal("open slot empty")
					}
				}
				b.ReportMetric(float64(loaded)/float64(b.N), "versions/op")
			})
		}
	}
}

// openSlot is the value of window w of coarse level a, the window holding the
// frontier, computed from stored records by lazy descent. load(l, first, last)
// returns every stored record version of level l in physical slots
// [first, last). The read loads level a-1 under the window first; a finer
// window with a materialized value (a Mat of its epoch) stands for its own
// history, so the read descends only into finer windows without one: the open
// child at every level, which the seal has not closed yet. Work is therefore
// the sum of the step ratios, not every finest slot under the window. Every
// window is computed by Versions, the function seals and merges use.
// settles says which Mat stands for its window's history (see the -lazy
// stores of TestOpenWindowRead). Measurement only, no production caller.
func openSlot(c Config, a int, w int64, settles func(l int, r Record) bool, load func(l int, first, last int64) ([]Record, error)) (Sample, bool, error) {
	var recs []Version
	add := func(l int, first, last int64) (map[int64]bool, error) {
		rs, err := load(l, first, last)
		if err != nil {
			return nil, err
		}
		mats := map[int64]bool{}
		for _, r := range rs {
			v := r.Version()
			if r.Kind == KindMat && settles(l, r) {
				// A seal's closed window stands in for the finer history
				// the read does not load: it competes at its event's tick.
				if r.Upto == 0 {
					v.Order = Tick(r.Order, max(l-1, 0))
					v.Upto = v.Order
				}
				mats[r.Sample.Time] = true
			}
			recs = append(recs, v)
		}
		return mats, nil
	}
	s := slot(w, c.Archives[a])
	if _, err := add(a, s, s+1); err != nil {
		return Sample{}, false, err
	}
	open := []int64{w}
	for l := a - 1; l >= 0 && len(open) > 0; l-- {
		al, span := c.Archives[l], c.Archives[l+1].Step
		var next []int64
		for _, pw := range open {
			n := span / al.Step
			var mats map[int64]bool
			var err error
			if first := slot(pw, al); n >= al.Count {
				mats, err = add(l, 0, al.Count)
			} else if mats, err = add(l, first, min(first+n, al.Count)); err == nil && first+n > al.Count {
				var wrap map[int64]bool
				wrap, err = add(l, 0, first+n-al.Count)
				for t := range wrap {
					mats[t] = true
				}
			}
			if err != nil {
				return Sample{}, false, err
			}
			for t := pw; l > 0 && t < pw+span; t += al.Step {
				if !mats[t] {
					next = append(next, t)
				}
			}
		}
		open = next
	}
	p, ok := NewVersions(c, recs).SlotValue(a, s)
	return p, ok, nil
}

// upgradeClosed is a prefix merge that also writes down every window the seal
// closed (a fallback Mat): recs hold every version at or below the merge's
// newest order, so the window's value as of its newest event is exact. It
// returns the written-down records it adds (one per window whose newest
// written-down Upto is older than the window's newest event).
func upgradeClosed(c Config, recs []Record) []Record {
	vs := make([]Version, len(recs))
	have := map[[2]int64]uint64{}
	var closed [][2]int64
	for i, r := range recs {
		vs[i] = r.Version()
		k := [2]int64{int64(r.Archive), r.Sample.Time}
		if r.Kind == KindMat && r.Upto == 0 {
			closed = append(closed, k)
		}
		if (r.Kind == KindMat || r.Kind == KindNone) && r.Upto > 0 {
			have[k] = max(have[k], r.Upto)
		}
	}
	f := NewVersions(c, vs)
	top := uint64(math.MaxUint64)
	var out []Record
	done := map[[2]int64]bool{}
	for _, k := range closed {
		if done[k] {
			continue
		}
		done[k] = true
		a, w := int(k[0]), k[1]
		v := f.valueAt(a, w, top)
		upto := uint64(0)
		if ev := f.events[a][w]; len(ev) > 0 {
			upto = f.recs[ev[len(ev)-1]].Order
		}
		if d, ok := f.stored(a, w, top); ok {
			upto = max(upto, d.Upto)
		}
		if u, ok := f.absent[k]; ok {
			upto = max(upto, u)
		}
		if v.ok {
			upto = max(upto, v.s.Sequence)
		}
		if upto == 0 || have[k] >= upto {
			continue
		}
		if !v.ok {
			out = append(out, Version{Archive: a, Time: w, Order: upto, Upto: upto, None: true}.Record(c))
			continue
		}
		out = append(out, Version{Archive: a, Time: w, Bits: v.s.Bits, Order: v.s.Sequence, Upto: upto, Mat: true}.Record(c))
	}
	return out
}

// projectOpen is the open-window projection reads used before the version
// read (OpenSlot): each open window aggregated from the resolved finer slots.
// Kept as the comparison baseline of TestOpenWindowRead.
func projectOpen(c Config, maxTime, through int64, records []Record) []Record {
	var samples []Sample
	for a := 1; a < len(c.Archives); a++ {
		coarse := c.Archives[a]
		start := align(maxTime, coarse.Step)
		if start+coarse.Step <= through {
			continue
		}
		samples = samples[:0]
		old := -1
		physical := slot(start, coarse)
		for i, r := range records {
			if r.Archive == a && r.Slot == physical {
				old = i
			}
			if r.Archive == a-1 && r.Sample.Time >= start && r.Sample.Time < start+coarse.Step {
				samples = append(samples, r.Sample)
			}
		}
		slices.SortFunc(samples, func(a, b Sample) int {
			if a.Time < b.Time {
				return -1
			}
			if a.Time > b.Time {
				return 1
			}
			return 0
		})
		if p, ok := AggregateWindow(c, a, start, samples); ok {
			r := Record{Delta: Delta{Archive: a, Slot: physical, Order: p.Sequence, Sample: p}}
			if old < 0 {
				records = append(records, r)
			} else if p.Time > records[old].Sample.Time || p.Time == records[old].Sample.Time && p.Sequence >= records[old].Order {
				records[old] = r
			}
		}
	}
	slices.SortFunc(records, func(a, b Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		if a.Slot < b.Slot {
			return -1
		}
		if a.Slot > b.Slot {
			return 1
		}
		return 0
	})
	return records
}
