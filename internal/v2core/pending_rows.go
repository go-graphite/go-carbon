package v2core

import "math"

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

// append encodes one submitted packet's applications. It starts a new packet
// header and never splits an application. The caller checks pendingRowsFit.
func (p *pendingRows) append(batches []coreBatch) {
	p.closePacket()
	for i := range batches {
		b := &batches[i]
		if p.open {
			pk := &p.packets[len(p.packets)-1]
			if b.Clock != pk.Clock || b.Order <= p.lastOrder || b.Order-pk.FirstOrder > math.MaxUint32 ||
				int(pk.Rows)+len(b.Points) > pendingPacketRows {
				p.closePacket()
			}
		}
		if !p.open {
			p.packets = append(p.packets, pendingPacket{FirstOrder: b.Order, Clock: b.Clock, Base: b.Points[0].Time, FirstRow: uint32(len(p.rows))})
			p.open = true
		}
		pk := &p.packets[len(p.packets)-1]
		delta := uint32(b.Order - pk.FirstOrder)
		for j := range b.Points {
			r := &b.Points[j]
			dt := r.Time - pk.Base
			if b.ID > math.MaxUint32 || dt < math.MinInt32 || dt > math.MaxInt32 {
				p.escapes = append(p.escapes, pendingEscape{Row: uint32(len(p.rows)), ID: b.ID, Time: r.Time})
				p.rows = append(p.rows, pendingRow{Bits: r.Bits})
			} else {
				p.rows = append(p.rows, pendingRow{ID: uint32(b.ID), DT: int32(dt), Bits: r.Bits})
			}
			p.openDeltas = append(p.openDeltas, delta)
			p.openSeqs = append(p.openSeqs, r.Seq)
		}
		pk.Rows += uint32(len(b.Points))
		p.lastOrder = b.Order
	}
	p.closePacket()
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
	var dst []coreBatch
	arena := make([]row, len(p.rows))
	escape := 0
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
			arena[ri] = row{ID: id, Time: t, Bits: src.Bits, Seq: seq}
			if start >= 0 && delta == prev {
				continue
			}
			if start >= 0 {
				dst[len(dst)-1].Points = arena[start:ri:ri]
			}
			dst = append(dst, coreBatch{ID: id, Order: order, Clock: pk.Clock, Registered: lookup(id)})
			start, prev = int(ri), delta
		}
		if start >= 0 {
			end := int(pk.FirstRow + pk.Rows)
			dst[len(dst)-1].Points = arena[start:end:end]
		}
	}
	return dst
}
