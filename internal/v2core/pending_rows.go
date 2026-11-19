package v2core

import (
	"math"
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
	p.closePacket()
	row, delta, seq := uint32(len(p.rows)), uint32(len(p.deltas)), uint32(len(p.seqs))
	for _, pk := range src.packets {
		pk.FirstRow += row
		if pk.Flags&pendingMulti != 0 {
			pk.Deltas += delta
		}
		if pk.Flags&pendingSeq != 0 {
			pk.Seqs += seq
		}
		p.packets = append(p.packets, pk)
	}
	for _, e := range src.escapes {
		e.Row += row
		p.escapes = append(p.escapes, e)
	}
	p.rows = append(p.rows, src.rows...)
	p.deltas = append(p.deltas, src.deltas...)
	p.seqs = append(p.seqs, src.seqs...)
	p.lastOrder = src.lastOrder
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
