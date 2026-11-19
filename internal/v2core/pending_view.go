package v2core

import (
	"sort"
	"sync/atomic"
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
}

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
	start   int    // first position
	count   int    // positions
	id      uint64 // owner segment identity; 0 for PendingBatches wrappers
}

// pendingSegmentIDs names owner segments. A segment never rewrites a
// published position, even when its slices grow into new arrays, so equal
// IDs mean equal content at every position both views hold.
var pendingSegmentIDs atomic.Uint64

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

func (s *pendingSegment) packetAt(row uint32) int {
	return sort.Search(len(s.packets), func(i int) bool { return s.packets[i].FirstRow+s.packets[i].Rows > row })
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
	i := sort.Search(len(v.segs), func(i int) bool { return v.segs[i].start+v.segs[i].count > pos })
	return &v.segs[i]
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
	return id, v.lookup(id), nil
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
				a = PendingApplication{ID: id, Order: pk.FirstOrder + uint64(d), Clock: pk.Clock, Points: int(hi - lo), lookup: v.lookup}
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
	return dst
}

// Suffix returns the positions at and after pos, which must be an
// application boundary inside the view (publication boundaries are).
func (v PendingView) Suffix(pos int) PendingView {
	if pos <= 0 {
		return v
	}
	if pos >= v.n {
		return PendingView{lookup: v.lookup}
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
	return PendingView{segs: segs, n: v.n - pos, lookup: v.lookup}
}

// Extends reports whether v starts with all of old: same immutable segments
// and backing arrays. Readers use it to keep indexes of old's positions.
func (v PendingView) Extends(old PendingView) bool {
	if old.n == 0 || old.n > v.n || len(old.segs) > len(v.segs) {
		return false
	}
	for i := range old.segs {
		a, b := &old.segs[i], &v.segs[i]
		if a.start != b.start || a.count > b.count || (a.count != b.count && i != len(old.segs)-1) || (a.cold == nil) != (b.cold == nil) {
			return false
		}
		if a.id != b.id {
			return false
		}
		if a.id == 0 {
			// PendingBatches wrappers may be re-sliced copies; compare the
			// first and last descriptors by their immutable arenas.
			if a.cold == nil || !sameColdBatch(&a.cold[0], &b.cold[0]) || !sameColdBatch(&a.cold[a.count-1], &b.cold[a.count-1]) {
				return false
			}
		}
	}
	return true
}

// pendingHead is the owner's append-only pending set for one head. Closed
// segments and published prefixes of the open one are never written again.
type pendingHead struct {
	segs     []pendingSegment
	rows     pendingRows
	cold     []coreBatch
	coldOpen bool
	openID   uint64
	n        int
}

func (h *pendingHead) add(batches []coreBatch, lookup func(uint64) *ApplicationPolicy) {
	if len(batches) == 0 {
		return
	}
	defer func() {
		if h.openID == 0 {
			h.openID = pendingSegmentIDs.Add(1)
		}
	}()
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

// addRows is add for applications the router already encoded as rows.
func (h *pendingHead) addRows(src *pendingRows) {
	if len(src.rows) == 0 {
		return
	}
	if h.coldOpen {
		h.closeOpen()
	}
	h.rows.appendRows(src)
	h.n += len(src.rows)
	if h.openID == 0 {
		h.openID = pendingSegmentIDs.Add(1)
	}
}

func (h *pendingHead) openSegment(start int) (pendingSegment, bool) {
	if h.coldOpen {
		if len(h.cold) == 0 {
			return pendingSegment{}, false
		}
		return pendingSegment{cold: h.cold[:len(h.cold):len(h.cold)], start: start, count: len(h.cold), id: h.openID}, true
	}
	r := &h.rows
	if len(r.rows) == 0 {
		return pendingSegment{}, false
	}
	return pendingSegment{rows: r.rows[:len(r.rows):len(r.rows)], packets: r.packets[:len(r.packets):len(r.packets)],
		deltas: r.deltas[:len(r.deltas):len(r.deltas)], seqs: r.seqs[:len(r.seqs):len(r.seqs)],
		escapes: r.escapes[:len(r.escapes):len(r.escapes)], start: start, count: len(r.rows), id: h.openID}, true
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
	h.rows, h.cold, h.coldOpen, h.openID = pendingRows{}, nil, false, 0
}

// view publishes the current immutable prefix.
func (h *pendingHead) view(lookup func(uint64) *ApplicationPolicy) PendingView {
	start := 0
	if k := len(h.segs); k > 0 {
		start = h.segs[k-1].start + h.segs[k-1].count
	}
	s, ok := h.openSegment(start)
	segs := h.segs[:len(h.segs):len(h.segs)]
	if ok {
		segs = append(segs, s)
	}
	return PendingView{segs: segs, n: h.n, lookup: lookup}
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
	return PendingView{segs: segs, n: v.n + w.n, lookup: v.lookup}
}

func sameColdBatch(a, b *coreBatch) bool {
	if a.ID != b.ID || a.Order != b.Order || a.Clock != b.Clock || a.Registered != b.Registered || len(a.Points) != len(b.Points) || len(a.Policy) != len(b.Policy) {
		return false
	}
	if len(a.Points) > 0 && &a.Points[0] != &b.Points[0] {
		return false
	}
	return len(a.Policy) == 0 || &a.Policy[0] == &b.Policy[0]
}

// Refs visits positions [lo, hi) in order with each position's application
// ID and name source; fn returns false to stop. Rows resolve their identity
// through the owner's catalog.
func (v PendingView) Refs(lo, hi int, fn func(pos int, id uint64, registered *ApplicationPolicy, policy []byte) bool) {
	for si := range v.segs {
		s := &v.segs[si]
		from, to := max(lo, s.start), min(hi, s.start+s.count)
		if from >= to {
			continue
		}
		if s.cold != nil {
			for pos := from; pos < to; pos++ {
				b := &s.cold[pos-s.start]
				if !fn(pos, b.ID, b.Registered, b.Policy) {
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
			if !fn(pos, id, v.lookup(id), nil) {
				return
			}
		}
	}
}
