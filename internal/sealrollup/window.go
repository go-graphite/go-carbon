package sealrollup

import (
	"math"
	"slices"
	"sort"
)

// Version is one stored record version of one name. Raw versions come from
// seals: a point at the archive its batch's admission clock routes it to,
// aligned to that archive's step. Mat versions are window values written down
// by a merge (or a closed window the seal materialized): Order is the write
// order the value competes for its physical slot with, Upto the newest event
// order of its window the value already accounts for. A Ghost is a point whose
// physical slot a later point of the same batch (another epoch) took: never a
// value, but Whisper still propagated its window in that batch, so it is an
// event there. None is a materialized absence: as of Upto the window had no
// value; it only bounds the event walk and never competes for a slot.
type Version struct {
	Archive          int
	Time             int64
	Bits             uint64
	Order, Upto      uint64
	Mat, Ghost, None bool
}

// Versions evaluates Whisper archives as a pure function of stored versions.
// A coarse window is the value of its newest successful Whisper event (a
// propagation or a direct write), its finer slots resolved as of that event.
// Build one per read over the versions the pinned generation holds for the
// name's ranges; it memoizes by (level, window or slot, as-of order).
type Versions struct {
	c      Config
	recs   []Version
	orders []uint64                   // distinct stored orders, ascending
	bySlot []map[int64][]int          // level -> physical slot -> stored versions
	wins   []map[int64]map[int64]bool // level -> physical slot -> window starts with events/versions
	events []map[int64][]uint64       // level -> window start -> distinct raw orders below the level inside it
	absent map[[2]int64]uint64        // (level, window) -> newest absence marker
	passes map[uint64][]Version       // order -> raw versions (and ghosts) its pass wrote
	memoC  map[[2]uint64]bool         // (order, level) -> the pass wrote at that level
	memoV  map[[3]int64]slotValue     // (level, window, asOf)
	memoS  map[[3]int64]slotValue     // (level, slot, asOf)
	stats  struct{ walks, deepWalks int }
}

type slotValue struct {
	s      Sample // Sequence is the write order
	stored bool
	ok     bool
}

// NewVersions indexes recs. It keeps recs; the caller must not modify them.
func NewVersions(c Config, recs []Version) *Versions {
	v := &Versions{c: c, recs: recs}
	v.index()
	return v
}

// WindowValue is the Whisper value of window w at coarse level a after every
// event with order <= asOf. Sequence is the order the value was written at.
func (f *Versions) WindowValue(a int, w int64, asOf uint64) (Sample, bool) {
	v := f.valueAt(a, w, asOf)
	return v.s, v.ok
}

// SlotValue is the content of physical slot s at coarse level a after every
// stored version: the newest of its stored versions and of the computed values
// of the windows that map to it.
func (f *Versions) SlotValue(a int, s int64) (Sample, bool) {
	v := f.slotAt(a, s, math.MaxUint64)
	return v.s, v.ok
}

// WindowRanges visits the physical slot ranges [first, last) whose versions
// window w of level a reads: its own slot, and at every finer level the slots
// covering [w, w+step) (two ranges where the ring wraps; the whole ring where
// the window spans it).
func WindowRanges(c Config, a int, w int64, visit func(level int, first, last int64) error) error {
	s := slot(w, c.Archives[a])
	if err := visit(a, s, s+1); err != nil {
		return err
	}
	for l := a - 1; l >= 0; l-- {
		al := c.Archives[l]
		n := c.Archives[a].Step / al.Step
		if n >= al.Count {
			if err := visit(l, 0, al.Count); err != nil {
				return err
			}
			continue
		}
		first := slot(w, al)
		if err := visit(l, first, min(first+n, al.Count)); err != nil {
			return err
		}
		if first+n > al.Count {
			if err := visit(l, 0, first+n-al.Count); err != nil {
				return err
			}
		}
	}
	return nil
}

// Version maps a stored record to the version the event walk reads. A Raw or
// Ghost record's order becomes the tick (batch order, target archive), its
// target archive being its own, so parts store no extra order bits. A Mat or
// None record competes at its batch order's tick below its archive. upto 0
// marks the seal's fallback window: the weakest version (order 1), it bounds
// no event walk and is a slot's value only where the stored versions yield
// no event at all. The seal's closed windows carry its known divergences
// from Whisper; with any stronger order they leak into the open windows.
func (r Record) Version(upto uint64) Version {
	v := Version{Archive: r.Archive, Time: r.Sample.Time, Bits: r.Sample.Bits}
	if r.Kind == KindRaw || r.Kind == KindGhost {
		v.Order = Tick(r.Order, r.Archive)
		v.Upto, v.Ghost = v.Order, r.Kind == KindGhost
		return v
	}
	v.Order, v.Upto = Tick(r.Order, max(r.Archive-1, 0)), upto
	if upto == 0 {
		v.Order = 1
	}
	v.Mat, v.None = r.Kind == KindMat, r.Kind == KindNone
	return v
}

// Tick orders the sub-steps of one batch. Whisper applies a batch one target
// archive at a time, ascending: archive j's writes and their propagation see
// direct writes of the same batch to archives <= j only. A version's order is
// therefore (batch order, target archive). Ladders have at most 16 archives.
func Tick(order uint64, archive int) uint64 { return order<<4 | uint64(archive) }

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

func (f *Versions) index() {
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
	f.passes = map[uint64][]Version{}
	f.memoC = map[[2]uint64]bool{}
	for i, r := range f.recs {
		if !r.Mat && !r.None {
			f.passes[r.Order] = append(f.passes[r.Order], r)
		}
		s := slot(r.Time, f.c.Archives[r.Archive])
		if r.None {
			k := [2]int64{int64(r.Archive), r.Time}
			f.absent[k] = max(f.absent[k], r.Upto)
			f.orders = append(f.orders, r.Upto)
			addWin(r.Archive, r.Time)
			continue
		}
		if !r.Ghost {
			f.bySlot[r.Archive][s] = append(f.bySlot[r.Archive][s], i)
			addWin(r.Archive, r.Time)
		}
		f.orders = append(f.orders, r.Order)
		for a := r.Archive + 1; a < n; a++ {
			w := align(r.Time, f.c.Archives[a].Step)
			addWin(a, w)
			if !r.Mat {
				f.events[a][w] = append(f.events[a][w], r.Order)
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
	f.memoV, f.memoS = map[[3]int64]slotValue{}, map[[3]int64]slotValue{}
}

// norm maps asOf to the newest stored order at or below it, so equal stored
// prefixes share memo entries.
func (f *Versions) norm(asOf uint64) uint64 {
	i := sort.Search(len(f.orders), func(i int) bool { return f.orders[i] > asOf })
	if i == 0 {
		return 0
	}
	return f.orders[i-1]
}

// stored returns the newest stored version of window w at level a at or
// before asOf (raw direct write or materialized value).
func (f *Versions) stored(a int, w int64, asOf uint64) (Version, bool) {
	var best Version
	has := false
	for _, i := range f.bySlot[a][slot(w, f.c.Archives[a])] {
		r := f.recs[i]
		if r.Time != w || r.Order > asOf {
			continue
		}
		if !has || r.Order > best.Order || r.Order == best.Order && r.Mat {
			best, has = r, true
		}
	}
	return best, has
}

// valueAt is the newest event with order <= asOf whose visible finer slots
// pass XFF, or the stored version when it was written at/after that event.
// Failed XFF keeps the older value, so the walk continues to older events.
func (f *Versions) valueAt(a int, w int64, asOf uint64) slotValue {
	asOf = f.norm(asOf)
	mk := [3]int64{int64(a), w, int64(asOf)}
	if v, ok := f.memoV[mk]; ok {
		return v
	}
	d, hasD := f.stored(a, w, asOf)
	ev := f.events[a][w]
	result := slotValue{}
	stop := uint64(0)
	if hasD {
		result = slotValue{Sample{d.Time, d.Bits, d.Order}, !d.Mat, true}
		stop = d.Upto
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
			result = slotValue{p, false, true}
			break
		}
	}
	f.memoV[mk] = result
	return result
}

// chain is Whisper's propagation stop: a pass reaches level a only if it wrote
// some window at every level between its target archive and a.
func (f *Versions) chain(a int, tick uint64) bool {
	pass := f.passes[tick]
	if len(pass) == 0 {
		return true
	}
	for l := pass[0].Archive + 1; l < a; l++ {
		mk := [2]uint64{tick, uint64(l)}
		wrote, ok := f.memoC[mk]
		if !ok {
			step := f.c.Archives[l].Step
			for _, r := range pass {
				if v := f.valueAt(l, align(r.Time, step), tick); v.ok && !v.stored && v.s.Sequence == tick {
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
func (f *Versions) inputsAt(l int, w, span int64, o uint64) []Sample {
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
func (f *Versions) slotAt(l int, s int64, o uint64) slotValue {
	o = f.norm(o)
	mk := [3]int64{int64(l), s, int64(o)}
	if v, ok := f.memoS[mk]; ok {
		return v
	}
	best := slotValue{}
	consider := func(v slotValue) {
		if v.ok && (!best.ok || newer(v.s.Sequence, v.stored, v.s.Time, best.s.Sequence, best.stored, best.s.Time)) {
			best = v
		}
	}
	for _, i := range f.bySlot[l][s] {
		// A materialized value competes for its slot as the computed value it
		// stands for; a raw direct write of the same batch was applied later.
		if r := f.recs[i]; r.Order <= o {
			consider(slotValue{Sample{r.Time, r.Bits, r.Order}, !r.Mat, true})
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
