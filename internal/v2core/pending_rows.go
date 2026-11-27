package v2core

import (
	"errors"
	"math"
	"slices"
	"sort"
	"unsafe"
)

// pendingRows is the pointer-free resident form of pending original
// applications (name-once S5a). One point costs one 16-byte pendingRow; one
// packet header covers up to pendingPacketRows rows. Nothing here holds a
// pointer, so the GC never scans pending points.
//
// Row r of packet p belongs to the application with order
// p.FirstOrder+delta(r), where delta(r) is r-p.FirstRow for single-point
// packets and deltas[p.Deltas+r-p.FirstRow] for multi-point ones. Orders
// strictly increase within a packet, so equal deltas mark one application.
// Seq equals the order unless pendingSeq is set. IDs at or above 2^32 and
// times outside base±2^31 move to escapes, sorted by row.
type pendingRows struct {
	rows    []pendingRow
	packets []pendingPacket
	deltas  []uint32
	seqs    []uint64
	escapes []pendingEscape

	open      bool
	lastOrder uint64
	// oldest is the earliest row time; it is valid while rows is nonempty.
	oldest int64
}

type pendingRow struct {
	ID   uint32
	DT   int32
	Bits uint64
}

type pendingPacket struct {
	FirstOrder uint64
	Clock      int64
	Base       int64
	FirstRow   uint32
	Rows       uint32
	Deltas     uint32
	Seqs       uint32
	Flags      uint32
	_          uint32
}

type pendingEscape struct {
	Row  uint32
	_    uint32
	ID   uint64
	Time int64
}

const (
	pendingPacketRows = 32768
	pendingMulti      = 1 << 0
	pendingSeq        = 1 << 1
)

// pendingRowsFit reports whether append can represent batches exactly.
// Policy bytes (non-rollup mode), empty applications, handles that differ
// from lookup and applications above one packet keep the coreBatch form.
func pendingRowsFit(batches []coreBatch, lookup func(uint64) *ApplicationPolicy) bool {
	for i := range batches {
		b := &batches[i]
		if len(b.Points) == 0 || len(b.Points) > pendingPacketRows || b.Policy != nil || b.Registered != lookup(b.ID) {
			return false
		}
		for j := range b.Points {
			if b.Points[j].ID != b.ID {
				return false
			}
		}
	}
	return true
}

// appendRows copies closed rows encoded by append, startApplication and
// addRow, rebasing their row and side-array offsets. The result equals
// append over the same applications.
func (p *pendingRows) appendRows(src *pendingRows) {
	if len(src.packets) > 0 {
		p.appendPackets(src, 0, len(src.packets))
	}
	p.lastOrder = src.lastOrder
}

// appendPackets copies src's closed packets [lo, hi) with their rows, side
// arrays and escapes. Side arrays take the capacity of rows on first use, so a
// head block sized for its rows never reallocates them.
func (p *pendingRows) appendPackets(src *pendingRows, lo, hi int) {
	p.closePacket()
	rowLo, shift := src.packets[lo].FirstRow, uint32(len(p.rows))-src.packets[lo].FirstRow
	for _, pk := range src.packets[lo:hi] {
		if pk.Flags&pendingMulti != 0 {
			d := src.deltas[pk.Deltas : pk.Deltas+pk.Rows]
			pk.Deltas = uint32(len(p.deltas))
			p.deltas = append(reserve(p.deltas, len(d), cap(p.rows)), d...)
		}
		if pk.Flags&pendingSeq != 0 {
			q := src.seqs[pk.Seqs : pk.Seqs+pk.Rows]
			pk.Seqs = uint32(len(p.seqs))
			p.seqs = append(reserve(p.seqs, len(q), cap(p.rows)), q...)
		}
		p.rows = append(p.rows, src.rows[pk.FirstRow:pk.FirstRow+pk.Rows]...)
		pk.FirstRow += shift
		p.packets = append(p.packets, pk)
	}
	rowHi := uint32(len(p.rows)) - shift
	for i := sort.Search(len(src.escapes), func(i int) bool { return src.escapes[i].Row >= rowLo }); i < len(src.escapes) && src.escapes[i].Row < rowHi; i++ {
		e := src.escapes[i]
		e.Row += shift
		p.escapes = append(p.escapes, e)
	}
}

// reserve returns s with room for n more elements, taking capacity c at once.
func reserve[T any](s []T, n, c int) []T {
	if cap(s)-len(s) >= n {
		return s
	}
	return slices.Grow(s, max(n, c-len(s)))
}

// size is the packet's queue charge: its rows, headers and side arrays.
func (p *pendingRows) size() uint64 {
	return uint64(len(p.rows))*uint64(unsafe.Sizeof(pendingRow{})) + uint64(len(p.packets))*uint64(unsafe.Sizeof(pendingPacket{})) +
		uint64(len(p.deltas))*4 + uint64(len(p.seqs))*8 + uint64(len(p.escapes))*uint64(unsafe.Sizeof(pendingEscape{}))
}

// append encodes one submitted packet's applications. It starts a new packet
// header and never splits an application. The caller checks pendingRowsFit.
func (p *pendingRows) append(batches []coreBatch) {
	p.closePacket()
	for i := range batches {
		b := &batches[i]
		p.startApplication(b.Order, b.Clock, b.Points[0].Time, len(b.Points))
		id := b.rowID()
		for j := range b.Points {
			p.addRow(id, b.Points[j].Time, b.Points[j].Bits, b.Points[j].Seq)
		}
	}
	p.closePacket()
}

// startApplication opens the header for one application of n points, closing
// the open header on a clock change, a non-increasing order, an order delta
// above 2^32 or a full packet. Producers call it and addRow per point instead
// of building a coreBatch.
func (p *pendingRows) startApplication(order uint64, clock, base int64, n int) {
	if p.open {
		pk := &p.packets[len(p.packets)-1]
		if clock != pk.Clock || order <= p.lastOrder || order-pk.FirstOrder > math.MaxUint32 || int(pk.Rows)+n > pendingPacketRows {
			p.closePacket()
		}
	}
	if !p.open {
		// The open packet writes its side arrays in place; closePacket drops
		// the ones the packet does not need.
		p.packets = append(p.packets, pendingPacket{FirstOrder: order, Clock: clock, Base: base, FirstRow: uint32(len(p.rows)), Deltas: uint32(len(p.deltas)), Seqs: uint32(len(p.seqs))})
		p.open = true
	}
	p.lastOrder = order
}

// addRow appends one point of the application startApplication opened.
func (p *pendingRows) addRow(id uint64, t int64, bits, seq uint64) {
	if len(p.rows) == 0 || t < p.oldest {
		p.oldest = t
	}
	pk := &p.packets[len(p.packets)-1]
	dt := t - pk.Base
	if id > math.MaxUint32 || dt < math.MinInt32 || dt > math.MaxInt32 {
		p.escapes = append(p.escapes, pendingEscape{Row: uint32(len(p.rows)), ID: id, Time: t})
		p.rows = append(p.rows, pendingRow{Bits: bits})
	} else {
		p.rows = append(p.rows, pendingRow{ID: uint32(id), DT: int32(dt), Bits: bits})
	}
	p.deltas = append(p.deltas, uint32(p.lastOrder-pk.FirstOrder))
	p.seqs = append(p.seqs, seq)
	pk.Rows++
}

// closePacket keeps side arrays only for packets that need them.
func (p *pendingRows) closePacket() {
	if !p.open {
		return
	}
	p.open = false
	pk := &p.packets[len(p.packets)-1]
	deltas, seqs := p.deltas[pk.Deltas:], p.seqs[pk.Seqs:]
	for i, d := range deltas {
		if d != uint32(i) {
			pk.Flags |= pendingMulti
		}
		if seqs[i] != pk.FirstOrder+uint64(d) {
			pk.Flags |= pendingSeq
		}
	}
	if pk.Flags&pendingSeq == 0 {
		p.seqs, pk.Seqs = p.seqs[:pk.Seqs], 0
	}
	if pk.Flags&pendingMulti == 0 {
		p.deltas, pk.Deltas = p.deltas[:pk.Deltas], 0
	}
}

func (p *pendingRows) reset() {
	// A slow admission or journal sync can grow a producer packet far beyond
	// steady demand. Released dispatch leases keep those arrays across GC;
	// retain two packets of headroom, rather than every warm-up high-water mark.
	keep := 2 * max(1024, len(p.rows))
	if cap(p.rows) > keep {
		p.rows = nil
	}
	if cap(p.deltas) > keep {
		p.deltas = nil
	}
	if cap(p.seqs) > keep {
		p.seqs = nil
	}
	p.rows, p.packets, p.deltas, p.seqs, p.escapes = p.rows[:0], p.packets[:0], p.deltas[:0], p.seqs[:0], p.escapes[:0]
	p.open, p.lastOrder = false, 0
}

// Outbox is one producer's applications encoded straight into the owners'
// pointer-free head rows, one pendingRows per owner, between journal syncs.
// Submitting it hands those rows to the owners, which copy them into their
// heads, and gives the outbox the arrays the owners released last, so steady
// intake reuses the same memory and builds no intermediate batch.
type Outbox struct {
	catalog *PolicyCatalog
	owners  []pendingRows
	open    *pendingRows
	id      uint64
}

// NewOutbox returns an empty outbox for one producer of e.
func (e *coreEngine) NewOutbox() *Outbox {
	return &Outbox{catalog: e.identities, owners: make([]pendingRows, e.cfg.Shards)}
}

// Start opens one original application of n points (1..32768) whose first
// point is at time first. h is a handle this engine registered. Point then
// adds the application's points in order.
func (b *Outbox) Start(h *ApplicationPolicy, order uint64, clock, first int64, n int) error {
	if h == nil || h.catalog != b.catalog || n < 1 || n > pendingPacketRows {
		return errors.New("outbox application requires an engine policy and 1..32768 points")
	}
	b.open, b.id = &b.owners[int(h.vshard)%len(b.owners)], h.rowID()
	b.open.startApplication(order, clock, first, n)
	return nil
}

// Point adds one point of the application Start opened.
func (b *Outbox) Point(t int64, bits, seq uint64) { b.open.addRow(b.id, t, bits, seq) }

func (b *Outbox) reset() {
	for i := range b.owners {
		b.owners[i].reset()
	}
	b.open = nil
}

// decodeFrom decodes p.packets, which may start after row 0 (a published
// suffix); rows and side arrays keep their absolute indices.
func (p *pendingRows) decodeFrom(lookup func(uint64) *ApplicationPolicy) []coreBatch {
	if len(p.packets) == 0 {
		return nil
	}
	first := p.packets[0].FirstRow
	last := &p.packets[len(p.packets)-1]
	var dst []coreBatch
	arena := make([]row, last.FirstRow+last.Rows-first)
	var lastRaw, id uint64
	escape := sort.Search(len(p.escapes), func(i int) bool { return p.escapes[i].Row >= first })
	for pi := range p.packets {
		pk := &p.packets[pi]
		start := -1
		var prev uint32
		for k := uint32(0); k < pk.Rows; k++ {
			ri := pk.FirstRow + k
			delta := k
			if pk.Flags&pendingMulti != 0 {
				delta = p.deltas[pk.Deltas+k]
			}
			order := pk.FirstOrder + uint64(delta)
			seq := order
			if pk.Flags&pendingSeq != 0 {
				seq = p.seqs[pk.Seqs+k]
			}
			src := p.rows[ri]
			raw, t := uint64(src.ID), pk.Base+int64(src.DT)
			if escape < len(p.escapes) && p.escapes[escape].Row == ri {
				raw, t = p.escapes[escape].ID, p.escapes[escape].Time
				escape++
			}
			if raw != lastRaw || ri == first {
				lastRaw, id = raw, durableID(raw, lookup)
			}
			arena[ri-first] = row{ID: id, Time: t, Bits: src.Bits, Seq: seq}
			if start >= 0 && delta == prev {
				continue
			}
			if start >= 0 {
				dst[len(dst)-1].Points = arena[start-int(first) : ri-first : ri-first]
			}
			dst = append(dst, coreBatch{ID: id, Order: order, Clock: pk.Clock, Registered: lookup(id)})
			start, prev = int(ri), delta
		}
		if start >= 0 {
			end := int(pk.FirstRow + pk.Rows - first)
			dst[len(dst)-1].Points = arena[start-int(first) : end : end]
		}
	}
	return dst
}
