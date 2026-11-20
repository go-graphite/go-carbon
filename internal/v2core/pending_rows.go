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

	// Open-packet scratch, reused across packets.
	openDeltas []uint32
	openSeqs   []uint64
	open       bool
	lastOrder  uint64
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

// pendingBatchesFit reports whether rollup batches, whose IDs core has
// interned, fit pendingRows: every application has 1..pendingPacketRows points.
func pendingBatchesFit(batches []coreBatch) bool {
	for i := range batches {
		if n := len(batches[i].Points); n == 0 || n > pendingPacketRows {
			return false
		}
	}
	return len(batches) > 0
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

// appendPackets copies src's closed packets [lo, hi) and their rows, side
// arrays and escapes. Side arrays get the capacity of rows on first use, so a
// head block sized for its rows never reallocates them.
func (p *pendingRows) appendPackets(src *pendingRows, lo, hi int) {
	p.closePacket()
	rowLo := src.packets[lo].FirstRow
	rowHi := src.packets[hi-1].FirstRow + src.packets[hi-1].Rows
	// Packets close their side arrays in order, so the ones in [lo, hi) are
	// contiguous ranges of src.deltas and src.seqs.
	deltaLo, deltaHi, seqLo, seqHi := uint32(0), uint32(0), uint32(0), uint32(0)
	for i := hi - 1; i >= lo; i-- {
		pk := &src.packets[i]
		if pk.Flags&pendingMulti != 0 {
			deltaLo = pk.Deltas
			if deltaHi == 0 {
				deltaHi = pk.Deltas + pk.Rows
			}
		}
		if pk.Flags&pendingSeq != 0 {
			seqLo = pk.Seqs
			if seqHi == 0 {
				seqHi = pk.Seqs + pk.Rows
			}
		}
	}
	row, delta, seq := uint32(len(p.rows)), uint32(len(p.deltas)), uint32(len(p.seqs))
	for _, pk := range src.packets[lo:hi] {
		pk.FirstRow = pk.FirstRow - rowLo + row
		if pk.Flags&pendingMulti != 0 {
			pk.Deltas = pk.Deltas - deltaLo + delta
		}
		if pk.Flags&pendingSeq != 0 {
			pk.Seqs = pk.Seqs - seqLo + seq
		}
		p.packets = append(p.packets, pk)
	}
	for i := sort.Search(len(src.escapes), func(i int) bool { return src.escapes[i].Row >= rowLo }); i < len(src.escapes) && src.escapes[i].Row < rowHi; i++ {
		e := src.escapes[i]
		e.Row = e.Row - rowLo + row
		p.escapes = append(p.escapes, e)
	}
	p.rows = append(p.rows, src.rows[rowLo:rowHi]...)
	if deltaHi > deltaLo {
		p.deltas = append(reserve(p.deltas, int(deltaHi-deltaLo), cap(p.rows)), src.deltas[deltaLo:deltaHi]...)
	}
	if seqHi > seqLo {
		p.seqs = append(reserve(p.seqs, int(seqHi-seqLo), cap(p.rows)), src.seqs[seqLo:seqHi]...)
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
		for j := range b.Points {
			p.addRow(b.ID, b.Points[j].Time, b.Points[j].Bits, b.Points[j].Seq)
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
		p.packets = append(p.packets, pendingPacket{FirstOrder: order, Clock: clock, Base: base, FirstRow: uint32(len(p.rows))})
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
	p.openDeltas = append(p.openDeltas, uint32(p.lastOrder-pk.FirstOrder))
	p.openSeqs = append(p.openSeqs, seq)
	pk.Rows++
}

// closePacket keeps side arrays only for packets that need them.
func (p *pendingRows) closePacket() {
	if !p.open {
		return
	}
	p.open = false
	pk := &p.packets[len(p.packets)-1]
	for i, d := range p.openDeltas {
		if d != uint32(i) {
			pk.Flags |= pendingMulti
			pk.Deltas = uint32(len(p.deltas))
			p.deltas = append(p.deltas, p.openDeltas...)
			break
		}
	}
	for i, s := range p.openSeqs {
		if s != pk.FirstOrder+uint64(p.openDeltas[i]) {
			pk.Flags |= pendingSeq
			pk.Seqs = uint32(len(p.seqs))
			p.seqs = append(p.seqs, p.openSeqs...)
			break
		}
	}
	p.openDeltas = p.openDeltas[:0]
	p.openSeqs = p.openSeqs[:0]
}

func (p *pendingRows) len() int { return len(p.rows) }

func (p *pendingRows) reset() {
	p.rows, p.packets, p.deltas, p.seqs, p.escapes = p.rows[:0], p.packets[:0], p.deltas[:0], p.seqs[:0], p.escapes[:0]
	p.openDeltas, p.openSeqs, p.open, p.lastOrder = p.openDeltas[:0], p.openSeqs[:0], false, 0
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
	points  int
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
	b.open, b.id = &b.owners[int(h.vshard)%len(b.owners)], h.id
	b.open.startApplication(order, clock, first, n)
	b.points += n
	return nil
}

// Point adds one point of the application Start opened.
func (b *Outbox) Point(t int64, bits, seq uint64) { b.open.addRow(b.id, t, bits, seq) }

// Len is the number of points added since the last submission.
func (b *Outbox) Len() int { return b.points }

func (b *Outbox) reset() {
	for i := range b.owners {
		b.owners[i].reset()
	}
	b.open, b.points = nil, 0
}

// batches rebuilds the original applications exactly, with their
// points in one arena so each batch's Points has cap == len, as submission
// produces them.
func (p *pendingRows) batches(lookup func(uint64) *ApplicationPolicy) []coreBatch {
	return p.decodeFrom(lookup)
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
			id, t := uint64(src.ID), pk.Base+int64(src.DT)
			if escape < len(p.escapes) && p.escapes[escape].Row == ri {
				id, t = p.escapes[escape].ID, p.escapes[escape].Time
				escape++
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
