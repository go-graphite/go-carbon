package v2core

import (
	"slices"
	"sort"
)

// PendingView is an owner's published pending set (name-once S5a-2): an
// immutable prefix of pointer-free row segments plus, for applications rows
// cannot represent exactly, cold coreBatch segments in arrival order.
//
// Positions number rows in row segments and applications in cold segments, so
// one application can own several consecutive positions. Owners only append
// between seals, so a later publication extends an earlier one's positions.
type PendingView struct {
	segs   []pendingSegment
	n      int
	lookup func(uint64) *ApplicationPolicy
	// rows is the owner's catalog, set on owner views: it maps head row IDs
	// to durable IDs by page arithmetic (durable). Nil views use lookup.
	rows *PolicyCatalog
	// fresh lists, once per head, each row application's ID whose name had no
	// sealed metadata when the owner published it, with the name's hash.
	fresh []pendingFresh
	// runs index every row position by ID: immutable in-memory parts, one per
	// publication, merged as they accumulate. Cold positions are not in runs.
	runs []pendingRun
}

// pendingRun is an immutable ID-sorted part over head row positions:
// keys are ID<<32|position, ascending, so positions ascend within an ID
// (arrival order). A view position is a head position plus shift. Rows with
// IDs at or above 2^32 are row escapes, which Find reads from the segments.
type pendingRun struct {
	keys  []uint64
	lo    int // retained head positions start at lo (Suffix preserves backing keys)
	hi    int // head positions below hi
	shift int
}

// pendingFresh is a name with no sealed metadata: namefp.Hash and its ID.
type pendingFresh struct{ hash, id uint64 }

// pendingSegment is immutable once published. A row segment covers packets
// packets[0..] whose rows are rows[packets[0].FirstRow:]; side arrays keep the
// absolute indices the packet headers carry.
type pendingSegment struct {
	rows    []pendingRow
	packets []pendingPacket
	deltas  []uint32
	seqs    []uint64
	escapes []pendingEscape
	cold    []coreBatch
	start   int // first position
	count   int // positions
}

// PendingApplication describes one application without materializing points.
type PendingApplication struct {
	ID     uint64
	Order  uint64
	Clock  int64
	Points int
	Policy []byte

	registered *ApplicationPolicy
	lookup     func(uint64) *ApplicationPolicy
}

// Registered is the application's catalog handle. Row applications resolve
// it on demand, so scans that only need IDs never touch the catalog.
func (a *PendingApplication) Registered() *ApplicationPolicy {
	if a.lookup != nil {
		return a.lookup(a.ID)
	}
	return a.registered
}

// PendingBatches wraps detached batches as a cold view (tests, older callers).
func PendingBatches(batches []Batch) PendingView {
	if len(batches) == 0 {
		return PendingView{}
	}
	return PendingView{segs: []pendingSegment{{cold: batches[:len(batches):len(batches)], count: len(batches)}}, n: len(batches)}
}

// Len is the number of positions.
func (v PendingView) Len() int { return v.n }

func (s *pendingSegment) firstRow() uint32 { return s.packets[0].FirstRow }

// packetAt finds the packet holding row. The seal calls it once per
// application in ID order, so it is a plain loop rather than sort.Search.
func (s *pendingSegment) packetAt(row uint32) int {
	lo, hi := 0, len(s.packets)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if pk := &s.packets[m]; pk.FirstRow+pk.Rows > row {
			hi = m
		} else {
			lo = m + 1
		}
	}
	return lo
}

func (s *pendingSegment) delta(pk *pendingPacket, row uint32) uint32 {
	if pk.Flags&pendingMulti != 0 {
		return s.deltas[pk.Deltas+row-pk.FirstRow]
	}
	return row - pk.FirstRow
}

func (s *pendingSegment) escape(row uint32) (pendingEscape, bool) {
	i := sort.Search(len(s.escapes), func(i int) bool { return s.escapes[i].Row >= row })
	if i < len(s.escapes) && s.escapes[i].Row == row {
		return s.escapes[i], true
	}
	return pendingEscape{}, false
}

func (s *pendingSegment) idTime(pk *pendingPacket, row uint32) (uint64, int64) {
	r := s.rows[row]
	if e, ok := s.escape(row); ok {
		return e.ID, e.Time
	}
	return uint64(r.ID), pk.Base + int64(r.DT)
}

// seg finds the segment holding position pos.
func (v PendingView) seg(pos int) *pendingSegment {
	lo, hi := 0, len(v.segs)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if s := &v.segs[m]; s.start+s.count > pos {
			hi = m
		} else {
			lo = m + 1
		}
	}
	return &v.segs[lo]
}

// Ref returns the application at pos: its ID and its name source.
func (v PendingView) Ref(pos int) (id uint64, registered *ApplicationPolicy, policy []byte) {
	s := v.seg(pos)
	if s.cold != nil {
		b := &s.cold[pos-s.start]
		return b.ID, b.Registered, b.Policy
	}
	row := s.firstRow() + uint32(pos-s.start)
	pk := &s.packets[s.packetAt(row)]
	id, _ = s.idTime(pk, row)
	h := v.lookup(id)
	return v.durable(id), h, nil
}

// durable maps a head row ID to the durable ID readers and L0 carry.
func (v *PendingView) durable(id uint64) uint64 {
	if v.rows != nil {
		return v.rows.durableRow(id)
	}
	return durableID(id, v.lookup)
}

// Application decodes the application holding pos and returns the position
// range it covers. Points are a fresh arena with cap == len.
func (v PendingView) Application(pos int) (Batch, int, int) {
	s := v.seg(pos)
	if s.cold != nil {
		return s.cold[pos-s.start], pos, pos + 1
	}
	base := s.firstRow()
	row := base + uint32(pos-s.start)
	pk := &s.packets[s.packetAt(row)]
	d := s.delta(pk, row)
	lo, hi := row, row+1
	for lo > pk.FirstRow && s.delta(pk, lo-1) == d {
		lo--
	}
	for hi < pk.FirstRow+pk.Rows && s.delta(pk, hi) == d {
		hi++
	}
	b := s.decode(pk, lo, hi, v.lookup)
	return b, s.start + int(lo-base), s.start + int(hi-base)
}

func (s *pendingSegment) decode(pk *pendingPacket, lo, hi uint32, lookup func(uint64) *ApplicationPolicy) Batch {
	order := pk.FirstOrder + uint64(s.delta(pk, lo))
	points := make([]row, hi-lo)
	for k := lo; k < hi; k++ {
		id, t := s.idTime(pk, k)
		id = durableID(id, lookup)
		seq := order
		if pk.Flags&pendingSeq != 0 {
			seq = s.seqs[pk.Seqs+k-pk.FirstRow]
		}
		points[k-lo] = row{ID: id, Time: t, Bits: s.rows[k].Bits, Seq: seq}
	}
	id := points[0].ID
	return Batch{ID: id, Order: order, Clock: pk.Clock, Points: points, Registered: lookup(id)}
}

// Each visits applications in arrival order without allocating; fn returns
// false to stop.
func (v PendingView) Each(fn func(*PendingApplication) bool) {
	var a PendingApplication
	for si := range v.segs {
		s := &v.segs[si]
		if s.cold != nil {
			for i := range s.cold {
				b := &s.cold[i]
				a = PendingApplication{ID: b.ID, Order: b.Order, Clock: b.Clock, Points: len(b.Points), registered: b.Registered, Policy: b.Policy}
				if !fn(&a) {
					return
				}
			}
			continue
		}
		for pi := range s.packets {
			pk := &s.packets[pi]
			end := pk.FirstRow + pk.Rows
			for lo := pk.FirstRow; lo < end; {
				d := s.delta(pk, lo)
				hi := lo + 1
				for hi < end && s.delta(pk, hi) == d {
					hi++
				}
				id, _ := s.idTime(pk, lo)
				a = PendingApplication{ID: v.durable(id), Order: pk.FirstOrder + uint64(d), Clock: pk.Clock, Points: int(hi - lo), lookup: v.lookup}
				if !fn(&a) {
					return
				}
				lo = hi
			}
		}
	}
}

// Batches decodes every application, in order. Owners and readers never keep
// the result; it serves seal planning and callers on the older []Batch hooks.
func (v PendingView) Batches() []Batch {
	if len(v.segs) == 1 && v.segs[0].cold != nil {
		return v.segs[0].cold
	}
	var dst []Batch
	for si := range v.segs {
		s := &v.segs[si]
		if s.cold != nil {
			dst = append(dst, s.cold...)
			continue
		}
		rs := pendingRows{rows: s.rows, packets: s.packets, deltas: s.deltas, seqs: s.seqs, escapes: s.escapes}
		dst = append(dst, rs.decodeFrom(v.lookup)...)
	}
	// Like the cold form, the result has cap == len: appends never share it.
	return dst[:len(dst):len(dst)]
}

// Suffix returns the positions at and after pos, which must be an
// application boundary inside the view (publication boundaries are).
func (v PendingView) Suffix(pos int) PendingView {
	if pos <= 0 {
		return v
	}
	if pos >= v.n {
		return PendingView{lookup: v.lookup, rows: v.rows}
	}
	i := sort.Search(len(v.segs), func(i int) bool { return v.segs[i].start+v.segs[i].count > pos })
	segs := make([]pendingSegment, 0, len(v.segs)-i)
	for _, s := range v.segs[i:] {
		if cut := pos - s.start; cut > 0 {
			if s.cold != nil {
				s.cold = s.cold[cut:]
			} else {
				row := s.firstRow() + uint32(cut)
				s.packets = s.packets[s.packetAt(row):]
				if s.packets[0].FirstRow != row {
					panic("pending suffix inside a packet")
				}
			}
			s.count -= cut
			s.start = pos
		}
		s.start -= pos
		segs = append(segs, s)
	}
	runs := make([]pendingRun, 0, len(v.runs))
	for _, r := range v.runs {
		if r.hi+r.shift > pos {
			r.lo = max(r.lo, pos-r.shift)
			r.shift -= pos
			runs = append(runs, r)
		}
	}
	return PendingView{segs: segs, n: v.n - pos, lookup: v.lookup, rows: v.rows, fresh: v.fresh, runs: runs}
}

// pendingHead is the owner's append-only pending set for one head. Closed
// segments and published prefixes of the open one are never written again.
type pendingHead struct {
	segs     []pendingSegment
	rows     pendingRows
	cold     []coreBatch
	coldOpen bool
	n        int
	// scanned is how many positions the owner's metadata scan has cleared.
	// Installed metrics, the catalog and the expiry policy only ever add, so
	// a cleared position stays clear until the head resets.
	scanned int
	// unresolved keeps plain rows with no catalog handle on the cold scan path.
	unresolved bool
	// fresh is the head's published new-name list (PendingView.NewNames);
	// freshScanned the positions it covers, freshIDs its members.
	fresh        []pendingFresh
	freshScanned int
	freshIDs     map[uint64]struct{}
	// runs cover positions [0, runEnd); view adds one for the rest.
	runs   []pendingRun
	runEnd int
	// Identifies this head's outstanding merge. Resetting the head drops it;
	// an owner may still finish that old merge, but cannot install its result.
	merging <-chan []pendingRun
}

func (h *pendingHead) add(batches []coreBatch, lookup func(uint64) *ApplicationPolicy) {
	if len(batches) == 0 {
		return
	}
	if pendingRowsFit(batches, lookup) {
		if h.coldOpen {
			h.closeOpen()
		}
		before := len(h.rows.rows)
		h.rows.append(batches)
		h.n += len(h.rows.rows) - before
		return
	}
	if !h.coldOpen {
		h.closeOpen()
		h.coldOpen = true
	}
	h.cold = append(h.cold, batches...)
	h.n += len(batches)
}

// pendingBlockRows caps one head block. A head's blocks start at 1,024 rows
// and double up to this cap; every application packet fits one block.
const pendingBlockRows = pendingPacketRows

// addRows copies applications a producer encoded as rows (Outbox) into fixed
// blocks. Whole packets go into the open block; one that does not fit closes
// it and starts the next. So each row is written once, a published block is
// never reallocated, and a head allocates its rows plus at most one block.
func (h *pendingHead) addRows(src *pendingRows) {
	if len(src.rows) == 0 {
		return
	}
	if h.coldOpen {
		h.closeOpen()
	}
	for lo := 0; lo < len(src.packets); {
		r := &h.rows
		hi, free := lo, cap(r.rows)-len(r.rows)
		for ; hi < len(src.packets) && int(src.packets[hi].Rows) <= free; hi++ {
			free -= int(src.packets[hi].Rows)
		}
		if hi == lo {
			if len(r.rows) > 0 {
				h.closeOpen()
			}
			size := min(pendingBlockRows, 1024<<min(len(h.segs), 5))
			h.rows.rows = make([]pendingRow, 0, max(size, int(src.packets[lo].Rows)))
			continue
		}
		r.appendPackets(src, lo, hi)
		lo = hi
	}
	h.rows.lastOrder = src.lastOrder
	h.n += len(src.rows)
}

func (h *pendingHead) openSegment(start int) (pendingSegment, bool) {
	if h.coldOpen {
		if len(h.cold) == 0 {
			return pendingSegment{}, false
		}
		return pendingSegment{cold: h.cold[:len(h.cold):len(h.cold)], start: start, count: len(h.cold)}, true
	}
	r := &h.rows
	if len(r.rows) == 0 {
		return pendingSegment{}, false
	}
	return pendingSegment{rows: r.rows[:len(r.rows):len(r.rows)], packets: r.packets[:len(r.packets):len(r.packets)],
		deltas: r.deltas[:len(r.deltas):len(r.deltas)], seqs: r.seqs[:len(r.seqs):len(r.seqs)],
		escapes: r.escapes[:len(r.escapes):len(r.escapes)], start: start, count: len(r.rows)}, true
}

// closeOpen freezes the open segment; the next one starts fresh slices so it
// never writes into a published backing array.
func (h *pendingHead) closeOpen() {
	start := 0
	if k := len(h.segs); k > 0 {
		start = h.segs[k-1].start + h.segs[k-1].count
	}
	if s, ok := h.openSegment(start); ok {
		h.segs = append(h.segs, s)
	}
	h.rows, h.cold, h.coldOpen = pendingRows{}, nil, false
}

// view publishes the current immutable prefix with every row position in an
// ID-sorted run.
func (h *pendingHead) view(lookup func(uint64) *ApplicationPolicy) PendingView {
	v := h.rawView(lookup)
	if h.runEnd < h.n {
		h.addRun(v)
	}
	v.runs = h.runs[:len(h.runs):len(h.runs)]
	return v
}

// addRun sorts only the row positions appended since the last publication.
// Merging older runs belongs to pendingCompaction, outside publication.
func (h *pendingHead) addRun(v PendingView) {
	if h.n >= 1<<32 {
		panic("pending head beyond 2^32 positions")
	}
	r := pendingRun{hi: h.n}
	v.IDs(h.runEnd, h.n, func(pos int, id uint64, cold *coreBatch) bool {
		if cold == nil && id < 1<<32 {
			r.keys = append(r.keys, id<<32|uint64(pos))
		}
		return true
	})
	h.runEnd = h.n
	if len(r.keys) == 0 {
		return // cold applications and escapes only: Find reads them directly
	}
	r.keys = sortByID(r.keys)
	h.runs = append(h.runs, r)
}

// pendingCompaction permits one merge flight per owner, including across
// seals. Inputs and outputs are immutable; only the owner installs a result.
// A slow flight leaves new publications searchable as separate runs.
type pendingCompaction struct {
	done chan []pendingRun
	n    int // input prefix length; later publications form an untouched tail
}

func (c *pendingCompaction) install(h *pendingHead, runs []pendingRun) {
	if h.merging == c.done {
		h.runs = append(runs[:len(runs):len(runs)], h.runs[c.n:]...)
		h.merging = nil
	}
	c.done = nil
}

func (c *pendingCompaction) poll(h *pendingHead) {
	if c.done != nil {
		select {
		case runs := <-c.done:
			c.install(h, runs)
		default:
		}
	}
}

func (c *pendingCompaction) wait(h *pendingHead) {
	if c.done != nil {
		c.install(h, <-c.done)
	}
}

func (c *pendingCompaction) start(h *pendingHead) {
	if c.done != nil {
		return
	}
	for i := 1; i < len(h.runs); i++ {
		if len(h.runs[i-1].keys) > len(h.runs[i].keys) {
			continue
		}
		inputs := h.runs[:len(h.runs):len(h.runs)]
		done := make(chan []pendingRun, 1)
		c.done, c.n, h.merging = done, len(inputs), done
		go func() {
			runs := make([]pendingRun, 0, len(inputs))
			for _, r := range inputs {
				for k := len(runs); k > 0 && len(runs[k-1].keys) <= len(r.keys); k-- {
					r = mergeRuns(&runs[k-1], &r)
					runs = runs[:k-1]
				}
				runs = append(runs, r)
			}
			done <- runs
		}()
		return
	}
}

// sortByID orders keys that arrive in position order by their ID half with a
// stable LSD radix (bytes that never vary are skipped), which leaves them in
// full key order. It returns the slice holding the result.
func sortByID(keys []uint64) []uint64 {
	if len(keys) < 256 {
		slices.Sort(keys)
		return keys
	}
	src, dst := keys, make([]uint64, len(keys))
	for shift := 32; shift < 64; shift += 8 {
		var count [256]int
		for _, k := range src {
			count[byte(k>>shift)]++
		}
		if count[byte(src[0]>>shift)] == len(src) {
			continue
		}
		sum := 0
		for i, c := range count {
			count[i], sum = sum, sum+c
		}
		for _, k := range src {
			b := byte(k >> shift)
			dst[count[b]] = k
			count[b]++
		}
		src, dst = dst, src
	}
	return src
}

// mergeRuns joins an earlier run a and a later run b into new arrays.
func mergeRuns(a, b *pendingRun) pendingRun {
	m := pendingRun{hi: b.hi, keys: make([]uint64, 0, len(a.keys)+len(b.keys))}
	i, j := 0, 0
	for i < len(a.keys) && j < len(b.keys) {
		if a.keys[i] < b.keys[j] {
			m.keys = append(m.keys, a.keys[i])
			i++
		} else {
			m.keys = append(m.keys, b.keys[j])
			j++
		}
	}
	m.keys = append(append(m.keys, a.keys[i:]...), b.keys[j:]...)
	return m
}

// Find returns, ascending, the positions of row applications whose ID is in
// ids and of the cold applications cold accepts. Row positions come from a
// binary search of each run, so the cost grows with the requested IDs and the
// number of runs, not with the pending rows. Rows with wider IDs, which are
// rare, come from the segments' escape lists.
func (v PendingView) Find(ids []uint64, cold func(*Batch) (bool, error)) ([]int, error) {
	var out []int
	for _, id := range ids {
		// Runs index head row IDs: a packed identity's slot. A NumericID
		// is its own row ID, so its query never reads the catalog.
		if IsPackedID(id) && v.lookup != nil {
			if h := v.lookup(id); h != nil && h.slot != 0 {
				id = uint64(h.slot)
			}
		}
		for ri := range v.runs {
			r := &v.runs[ri]
			k, _ := slices.BinarySearch(r.keys, id<<32)
			for ; id < 1<<32 && k < len(r.keys) && r.keys[k]>>32 == id; k++ {
				if pos := int(uint32(r.keys[k])); pos >= r.lo && pos+r.shift >= 0 && pos+r.shift < v.n {
					out = append(out, pos+r.shift)
				}
			}
		}
	}
	for si := range v.segs {
		s := &v.segs[si]
		for _, e := range s.escapes {
			if e.ID >= 1<<32 && slices.Contains(ids, e.ID) && e.Row >= s.firstRow() && int(e.Row-s.firstRow()) < s.count {
				out = append(out, s.start+int(e.Row-s.firstRow()))
			}
		}
		for i := range s.cold {
			ok, err := cold(&s.cold[i])
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, s.start+i)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

// rawView is the current immutable prefix without runs, for owner scans.
func (h *pendingHead) rawView(lookup func(uint64) *ApplicationPolicy) PendingView {
	start := 0
	if k := len(h.segs); k > 0 {
		start = h.segs[k-1].start + h.segs[k-1].count
	}
	s, ok := h.openSegment(start)
	segs := h.segs[:len(h.segs):len(h.segs)]
	if ok {
		segs = append(segs, s)
	}
	return PendingView{segs: segs, n: h.n, lookup: lookup, fresh: h.fresh[:len(h.fresh):len(h.fresh)]}
}

// concat joins a frozen head's view and the next head's view.
func (v PendingView) concat(w PendingView) PendingView {
	if v.n == 0 {
		return w
	}
	if w.n == 0 {
		return v
	}
	segs := make([]pendingSegment, 0, len(v.segs)+len(w.segs))
	segs = append(segs, v.segs...)
	for _, s := range w.segs {
		s.start += v.n
		segs = append(segs, s)
	}
	runs := v.runs[:len(v.runs):len(v.runs)]
	runs = append(runs, w.runs...)
	for i := len(v.runs); i < len(runs); i++ {
		runs[i].shift += v.n
	}
	fresh := v.fresh[:len(v.fresh):len(v.fresh)]
	fresh = append(fresh, w.fresh...)
	return PendingView{segs: segs, n: v.n + w.n, lookup: v.lookup, rows: v.rows, fresh: fresh, runs: runs}
}

// NewNames visits the name hash and ID of each row application whose name had
// no sealed metadata when the owner published it. A reader resolves its
// requested names to IDs through it and the sealed metadata, so it never looks
// a pending position up in the catalog. Cold applications are not listed:
// they carry their identity inline.
func (v PendingView) NewNames(fn func(hash, id uint64)) {
	for _, f := range v.fresh {
		fn(f.hash, f.id)
	}
}

// IDs visits positions [lo, hi) in order with each position's ID and, for
// cold applications, their descriptor. It never touches the catalog.
func (v PendingView) IDs(lo, hi int, fn func(pos int, id uint64, cold *Batch) bool) {
	for si := range v.segs {
		s := &v.segs[si]
		from, to := max(lo, s.start), min(hi, s.start+s.count)
		if from >= to {
			continue
		}
		if s.cold != nil {
			for pos := from; pos < to; pos++ {
				b := &s.cold[pos-s.start]
				if !fn(pos, b.ID, b) {
					return
				}
			}
			continue
		}
		base := s.firstRow()
		row := base + uint32(from-s.start)
		esc := sort.Search(len(s.escapes), func(i int) bool { return s.escapes[i].Row >= row })
		for pos := from; pos < to; pos, row = pos+1, row+1 {
			id := uint64(s.rows[row].ID)
			if esc < len(s.escapes) && s.escapes[esc].Row == row {
				id = s.escapes[esc].ID
				esc++
			}
			if !fn(pos, id, nil) {
				return
			}
		}
	}
}

// maxClock is the latest arrival clock among the view's applications, or c.
// Every application in a packet shares its header's clock.
func (v PendingView) maxClock(c int64) int64 {
	for si := range v.segs {
		s := &v.segs[si]
		for i := range s.cold {
			c = max(c, s.cold[i].Clock)
		}
		for i := range s.packets {
			c = max(c, s.packets[i].Clock)
		}
	}
	return c
}
