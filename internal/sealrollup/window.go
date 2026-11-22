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
// None record competes at its batch order's tick below its archive and bounds
// the walk at its Upto. Upto 0 marks the seal's fallback window: the weakest
// version (order 1), it bounds no event walk and is a slot's value only where
// the stored versions yield no event at all. The seal's closed windows carry
// its known divergences from Whisper; with any stronger order they leak into
// the open windows.
func (r Record) Version() Version {
	v := Version{Archive: r.Archive, Time: r.Sample.Time, Bits: r.Sample.Bits}
	if r.Kind == KindRaw || r.Kind == KindGhost {
		v.Order = Tick(r.Order, r.Archive)
		v.Upto, v.Ghost = v.Order, r.Kind == KindGhost
		return v
	}
	v.Order, v.Upto = Tick(r.Order, max(r.Archive-1, 0)), r.Upto
	if r.Upto == 0 {
		v.Order = 1
	}
	v.Mat, v.None = r.Kind == KindMat, r.Kind == KindNone
	return v
}

// Record is the stored form of a window WriteDown materialized: Version maps
// it back to v. A Mat's order is the tick of the event (or stored version) its
// value came from; the stored batch order keeps that tick's batch, and the
// archive below the Mat's own stands for the sub-step (see Version).
func (v Version) Record(c Config) Record {
	r := Record{Delta: Delta{Archive: v.Archive, Slot: slot(v.Time, c.Archives[v.Archive]), Order: max(v.Order>>4, 1), Sample: Sample{Time: v.Time, Bits: v.Bits}}, Upto: v.Upto}
	r.Sample.Sequence = r.Order
	switch {
	case v.None:
		r.Kind, r.Sample.Bits = KindNone, 0
	case v.Mat:
		r.Kind = KindMat
	case v.Ghost:
		r.Kind, r.Upto = KindGhost, 0
	default:
		r.Upto = 0
	}
	return r
}

// WriteDown is a merge's rollup write-down. recs must hold every version of
// one name at or below the merge's newest order: the merge holds the owner's
// oldest part, so no older version lies outside it. It drops every superseded
// version (all but each physical slot's winner), every ghost and every
// version gone reports (expired), and before that materializes each window
// whose event walk could read a dropped version: the parents, at every level
// above, of every epoch the dropped version's slot held, and recursively the
// slots of those parents. Each such window becomes a Mat (its value, at the
// order it was written, Upto its newest event) or a None (no value as of
// Upto). Older absence markers of a window go. keep reports the surviving
// inputs; mats are the new versions. Rollups stay a pure function of the
// stored versions: reads over keep + mats equal reads over recs. Seal-closed
// windows are written down too, so reads stop above their finer history while
// no newer event lies under them.
func WriteDown(c Config, recs []Version, gone func(int) bool) (keep []bool, mats []Version) {
	f := NewVersions(c, recs)
	keep = make([]bool, len(recs))
	drop := func(i int) { keep[i] = false }
	for i := range keep {
		keep[i] = true
	}
	dropped := false
	for _, m := range f.bySlot {
		for _, idx := range m {
			win := idx[0]
			for _, i := range idx[1:] {
				r, b := recs[i], recs[win]
				if r.Time == b.Time && r.Order == b.Order {
					if r.Mat && !b.Mat {
						win = i
					}
				} else if newer(r.Order, !r.Mat, r.Time, b.Order, !b.Mat, b.Time) {
					win = i
				}
			}
			for _, i := range idx {
				if i != win {
					drop(i)
					dropped = true
				}
			}
		}
	}
	for i, r := range recs {
		if r.Ghost || gone != nil && gone(i) {
			drop(i)
			dropped = true
		}
	}
	// A prefix merge also writes down every seal-closed window (a fallback
	// Mat) whose newest event is newer than its newest write-down, even when
	// nothing above was superseded. The next merge drops the fallback, which
	// the written-down window outranks; a late point brings a new fallback.
	need := map[[2]int64]bool{}
	var have map[[2]int64]uint64 // newest write-down per window, built on the first fallback
	for _, r := range recs {
		if !r.Mat || r.Upto != 0 {
			continue
		}
		if have == nil {
			have = map[[2]int64]uint64{}
			for _, x := range recs {
				if x.Mat || x.None {
					k := [2]int64{int64(x.Archive), x.Time}
					have[k] = max(have[k], x.Upto)
				}
			}
		}
		k := [2]int64{int64(r.Archive), r.Time}
		if ev := f.events[r.Archive][r.Time]; len(ev) > 0 && ev[len(ev)-1] > have[k] {
			need[k] = true
		}
	}
	if !dropped && len(need) == 0 {
		return keep, nil
	}
	type lslot struct {
		l int
		s int64
	}
	seen := map[lslot]bool{}
	var queue []lslot
	addParents := func(l int, t int64) {
		for a := l + 1; a < len(c.Archives); a++ {
			w := align(t, c.Archives[a].Step)
			need[[2]int64{int64(a), w}] = true
			if k := (lslot{a, slot(w, c.Archives[a])}); !seen[k] {
				seen[k] = true
				queue = append(queue, k)
			}
		}
	}
	for i, r := range recs {
		if keep[i] {
			continue
		}
		addParents(r.Archive, r.Time)
		if k := (lslot{r.Archive, slot(r.Time, c.Archives[r.Archive])}); !r.Ghost && !seen[k] {
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
	for k := range need {
		a, w := int(k[0]), k[1]
		v := f.valueAt(a, w, top)
		upto := uint64(0)
		if ev := f.events[a][w]; len(ev) > 0 {
			upto = ev[len(ev)-1]
		}
		if d, ok := f.stored(a, w, top); ok {
			upto = max(upto, d.Upto)
		}
		if u, ok := f.absent[k]; ok {
			upto = max(upto, u)
		}
		if !v.ok {
			if upto > 0 {
				mats = append(mats, Version{Archive: a, Time: w, Order: upto, Upto: upto, None: true})
			}
			continue
		}
		upto = max(upto, v.s.Sequence)
		mats = append(mats, Version{Archive: a, Time: w, Bits: v.s.Bits, Order: v.s.Sequence, Upto: upto, Mat: true})
	}
	// Map iteration order must not reach the output.
	slices.SortFunc(mats, func(x, y Version) int {
		if x.Archive != y.Archive {
			return x.Archive - y.Archive
		}
		return int(x.Time - y.Time)
	})
	for i, r := range recs {
		if r.None && (need[[2]int64{int64(r.Archive), r.Time}] || r.Upto < f.absent[[2]int64{int64(r.Archive), r.Time}]) {
			drop(i)
		}
	}
	return keep, mats
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

// OpenSlot is the value of window w of coarse level a, computed from stored
// records by lazy descent. load(l, first, last, from) returns every stored
// record version of level l in physical slots [first, last) from the parts
// that may hold a batch order >= from (from 0: every part). The read loads
// level a-1 under the window first and descends only into finer windows that
// are not settled. A window is settled by a window value a merge wrote down
// (Upto > 0) while no stored version under it is newer than that Upto: the
// value then accounts for the window's whole history, so its finer slots are
// never read. The seal's fallback windows settle nothing (they carry the
// seal's divergences). Every window is computed by Versions, the function the
// seal and merges use, so the read equals Versions over every version under
// the window (WindowRanges).
func OpenSlot(c Config, a int, w int64, load func(l int, first, last int64, from uint64) ([]Record, error)) (Sample, bool, error) {
	var recs []Version
	s := slot(w, c.Archives[a])
	rs, err := load(a, s, s+1, 0)
	if err != nil {
		return Sample{}, false, err
	}
	for _, r := range rs {
		recs = append(recs, r.Version())
	}
	// settled reports whether no version under window t of level l (finer
	// levels, from parts that may be newer than upto) is newer than upto.
	settled := func(l int, t int64, upto uint64) (bool, error) {
		quiet := true
		err := WindowRanges(c, l, t, func(j int, first, last int64) error {
			if j == l || !quiet {
				return nil
			}
			rs, err := load(j, first, last, upto>>4)
			for _, r := range rs {
				if r.Sample.Time >= t && r.Sample.Time < t+c.Archives[l].Step && r.Version().Order > upto {
					quiet = false
				}
			}
			return err
		})
		return quiet, err
	}
	open := []int64{w}
	for l := a - 1; l >= 0 && len(open) > 0; l-- {
		al, span := c.Archives[l], c.Archives[l+1].Step
		// One load per distinct physical range of this level.
		var slots []int64
		for _, pw := range open {
			n := min(span/al.Step, al.Count)
			for i := range n {
				slots = append(slots, (slot(pw, al)+i)%al.Count)
			}
		}
		slices.Sort(slots)
		slots = slices.Compact(slots)
		upto := map[int64]uint64{}
		for i := 0; i < len(slots); {
			j := i + 1
			for j < len(slots) && slots[j] == slots[j-1]+1 {
				j++
			}
			rs, err := load(l, slots[i], slots[j-1]+1, 0)
			if err != nil {
				return Sample{}, false, err
			}
			for _, r := range rs {
				if r.Kind == KindMat && r.Upto > 0 {
					upto[r.Sample.Time] = max(upto[r.Sample.Time], r.Upto)
				}
				recs = append(recs, r.Version())
			}
			i = j
		}
		var next []int64
		for _, pw := range open {
			for t := pw; l > 0 && t < pw+span; t += al.Step {
				if u, ok := upto[t]; ok {
					quiet, err := settled(l, t, u)
					if err != nil {
						return Sample{}, false, err
					}
					if quiet {
						continue
					}
				}
				next = append(next, t)
			}
		}
		open = next
	}
	p, ok := NewVersions(c, recs).SlotValue(a, s)
	return p, ok, nil
}
