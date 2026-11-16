package main

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/golang/snappy"
)

// compactRollupHead belongs to one producer. It preserves original apply
// boundaries and float bits; it does not aggregate, reorder or drop samples.
// The arena has no per-point pointers. Values use byte-aligned XOR; arbitrary
// timestamps and sequences retain a lossless varint fallback.
// This is an offline numeric adapter, not a durable journal codec.
type compactRollupHead struct {
	submissionEngine   *coreEngine
	submissionProducer int
	data               []byte
	scratch            []byte
	batches            []compactRollupBatch
	policies           [][]byte
	policyIDs          map[string]uint32
	points             int
	submission         []compactRollupSubmission
}
type compactRollupBatch struct {
	id, order         uint64
	clock             int64
	start, end, count int
	policy            uint32
	compressed        bool
}

func (h *compactRollupHead) append(b coreBatch) error {
	return h.appendValues(b, len(b.Points), func(i int) row { return b.Points[i] })
}
func (h *compactRollupHead) appendSamples(b coreBatch, points []sealrollup.Sample) error {
	return h.appendValues(b, len(points), func(i int) row { p := points[i]; return row{ID: b.ID, Time: p.Time, Bits: p.Bits, Seq: p.Sequence} })
}
func (h *compactRollupHead) appendValues(b coreBatch, count int, valueAt func(int) row) error {
	regularTime, regularSequence := true, true
	var timeStep int64
	var sequenceStep uint64
	if count > 1 {
		timeStep = valueAt(1).Time - valueAt(0).Time
		sequenceStep = valueAt(1).Seq - valueAt(0).Seq
	}
	for i := range count {
		p := valueAt(i)
		if i > 0 {
			regularTime = regularTime && p.Time-valueAt(i-1).Time == timeStep
			regularSequence = regularSequence && p.Seq-valueAt(i-1).Seq == sequenceStep
		}
		if p.ID != b.ID {
			return errors.New("mixed metric compact batch")
		}
	}
	if h.policyIDs == nil {
		h.policyIDs = make(map[string]uint32)
	}
	policy, ok := h.policyIDs[string(b.Policy)]
	if !ok {
		policy = uint32(len(h.policies))
		h.policies = append(h.policies, slices.Clone(b.Policy))
		h.policyIDs[string(b.Policy)] = policy
	}
	meta := compactRollupBatch{id: b.ID, order: b.Order, clock: b.Clock, start: len(h.data), count: count, policy: policy}
	timestamp, sequence, value := b.Clock, uint64(0), uint64(0)
	flags := byte(0)
	if regularTime {
		flags |= 1
	}
	if regularSequence {
		flags |= 2
	}
	h.data = append(h.data, flags)
	if count > 0 {
		if regularTime {
			h.data = binary.AppendVarint(h.data, valueAt(0).Time-b.Clock)
			h.data = binary.AppendVarint(h.data, timeStep)
		}
		if regularSequence {
			h.data = binary.AppendUvarint(h.data, valueAt(0).Seq)
			h.data = binary.AppendUvarint(h.data, sequenceStep)
		}
	}
	for i := range count {
		p := valueAt(i)
		if !regularTime {
			h.data = binary.AppendVarint(h.data, p.Time-timestamp)
		}
		if !regularSequence {
			h.data = binary.AppendUvarint(h.data, p.Seq-sequence)
		}
		xor := p.Bits ^ value
		trailing, width := 0, 0
		if xor != 0 {
			trailing = bits.TrailingZeros64(xor) / 8
			width = 8 - bits.LeadingZeros64(xor)/8 - trailing
		}
		h.data = append(h.data, byte(trailing<<4|width))
		xor >>= uint(trailing * 8)
		for range width {
			h.data = append(h.data, byte(xor))
			xor >>= 8
		}
		timestamp, sequence, value = p.Time, p.Seq, p.Bits
	}
	// Compress a complete original batch only when it saves owned bytes. The
	// bounded reusable scratch is one batch, independent of the head's point count.
	h.scratch = snappy.Encode(h.scratch[:cap(h.scratch)], h.data[meta.start:])
	if len(h.scratch) < len(h.data)-meta.start {
		h.data = append(h.data[:meta.start], h.scratch...)
		meta.compressed = true
	}
	meta.end = len(h.data)
	h.batches = append(h.batches, meta)
	h.points += count
	return nil
}

// decodeBatch writes straight into reducer scratch. No expanded row arena is
// retained on admission or allocated for a whole frozen head.
func (h *compactRollupHead) decodeBatch(m compactRollupBatch, dst []sealrollup.Sample) (sealrollup.Batch, error) {
	if m.start < 0 || m.end < m.start || m.end > len(h.data) || m.count < 0 || m.count > len(dst) || int(m.policy) >= len(h.policies) {
		return sealrollup.Batch{}, errors.New("invalid compact batch")
	}
	data := h.data[m.start:m.end]
	if m.compressed {
		var err error
		n, e := snappy.DecodedLen(data)
		if e != nil || n > 30*m.count+64 {
			return sealrollup.Batch{}, errors.New("invalid compact decoded size")
		}
		h.scratch, err = snappy.Decode(h.scratch[:cap(h.scratch)], data)
		data = h.scratch
		if err != nil {
			return sealrollup.Batch{}, err
		}
	}
	points := dst[:m.count]
	timestamp, sequence, value := m.clock, uint64(0), uint64(0)
	if len(data) == 0 {
		return sealrollup.Batch{}, errors.New("missing compact flags")
	}
	flags := data[0]
	data = data[1:]
	if flags > 3 {
		return sealrollup.Batch{}, errors.New("invalid compact flags")
	}
	var timeStep int64
	var sequenceStep uint64
	if m.count > 0 {
		if flags&1 != 0 {
			dt, n := binary.Varint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact base time")
			}
			data = data[n:]
			timestamp += dt
			timeStep, n = binary.Varint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact time stride")
			}
			data = data[n:]
		}
		if flags&2 != 0 {
			var n int
			sequence, n = binary.Uvarint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact base sequence")
			}
			data = data[n:]
			sequenceStep, n = binary.Uvarint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact sequence stride")
			}
			data = data[n:]
		}
	}
	for i := range points {
		if flags&1 == 0 {
			dt, n := binary.Varint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact timestamp")
			}
			data = data[n:]
			timestamp += dt
		} else if i > 0 {
			timestamp += timeStep
		}
		if flags&2 == 0 {
			ds, n := binary.Uvarint(data)
			if n <= 0 {
				return sealrollup.Batch{}, errors.New("invalid compact sequence")
			}
			data = data[n:]
			sequence += ds
		} else if i > 0 {
			sequence += sequenceStep
		}
		if len(data) == 0 {
			return sealrollup.Batch{}, errors.New("missing compact value")
		}
		tag := data[0]
		data = data[1:]
		width, trailing := int(tag&15), int(tag>>4)
		if width > 8 || trailing+width > 8 || len(data) < width {
			return sealrollup.Batch{}, errors.New("invalid compact value")
		}
		xor := uint64(0)
		for j := 0; j < width; j++ {
			xor |= uint64(data[j]) << uint((j+trailing)*8)
		}
		data = data[width:]
		value ^= xor
		points[i] = sealrollup.Sample{Time: timestamp, Sequence: sequence, Bits: value}
	}
	if len(data) != 0 {
		return sealrollup.Batch{}, errors.New("trailing compact samples")
	}
	return sealrollup.Batch{Order: m.order, Now: m.clock, Points: points}, nil
}

// decode is the diagnostic expanded adapter. The compact engine never calls it.
func (h *compactRollupHead) decode() ([]coreBatch, error) {
	rows := make([]row, h.points)
	batches := make([]coreBatch, 0, len(h.batches))
	var scratch []sealrollup.Sample
	for _, m := range h.batches {
		if m.count < 0 || m.count > len(rows) {
			return nil, errors.New("invalid compact count")
		}
		if cap(scratch) < m.count {
			scratch = make([]sealrollup.Sample, m.count)
		}
		b, err := h.decodeBatch(m, scratch[:m.count])
		if err != nil {
			return nil, err
		}
		points := rows[:m.count:m.count]
		rows = rows[m.count:]
		for i, p := range b.Points {
			points[i] = row{ID: m.id, Time: p.Time, Bits: p.Bits, Seq: p.Sequence}
		}
		batches = append(batches, coreBatch{ID: m.id, Order: m.order, Clock: m.clock, Policy: h.policies[m.policy], Points: points})
	}
	return batches, nil
}
func (h *compactRollupHead) reset() {
	h.data = h.data[:0]
	clear(h.batches)
	h.batches = h.batches[:0]
	h.points = 0
}
func (h *compactRollupHead) residentBytes() int {
	n := int(unsafe.Sizeof(*h)) + cap(h.data) + cap(h.scratch) + cap(h.batches)*int(unsafe.Sizeof(compactRollupBatch{})) + cap(h.policies)*int(unsafe.Sizeof([]byte{}))
	n += cap(h.submission) * int(unsafe.Sizeof(compactRollupSubmission{}))
	for i := range h.submission {
		n += cap(h.submission[i].selection.batches) * int(unsafe.Sizeof(compactRollupBatch{}))
	}
	// Policy map accounting is conservative for the small distinct-policy set;
	// held-heap measurements also include runtime map backing and allocator classes.
	for k, v := range h.policyIDs {
		n += len(k) + int(unsafe.Sizeof(v)) + 64
	}
	for _, p := range h.policies {
		n += cap(p)
	}
	return n
}

// submitRollupGroup sends only to this producer's disjoint owner group. All
// producers must finish before the next seal begins. One packet/ACK per owner
// means one seal barrier per owner, with no empty all-owner producer fanout.
// Ownership transfers until every ACK arrives; callers can then reuse arenas.
func (e *coreEngine) submitRollupGroup(producer int, batches []coreBatch, cursor uint64) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return errors.New("invalid rollup group")
	}
	byOwner := make([][]coreBatch, len(e.owners))
	for _, b := range batches {
		owner := coreShard(b.ID, len(e.owners))
		if owner%e.cfg.Producers != producer {
			return errors.New("metric outside producer owner group")
		}
		byOwner[owner] = append(byOwner[owner], b)
	}
	type waiting struct {
		owner *coreOwner
		ack   chan error
	}
	var waits []waiting
	var packets []*corePacket
	for owner := producer; owner < len(e.owners); owner += e.cfg.Producers {
		o := e.owners[owner]
		count := 0
		for _, b := range byOwner[owner] {
			count += len(b.Points)
		}
		if count > e.cfg.MaxRows {
			return errors.New("owner group exceeds head bound")
		}
		rows := make([]row, 0, count)
		for _, b := range byOwner[owner] {
			for _, r := range b.Points {
				if r.ID != b.ID {
					return errors.New("mixed metric group batch")
				}
			}
			rows = append(rows, b.Points...)
		}
		ack := make(chan error, 1)
		packet := &corePacket{rows: rows, batches: byOwner[owner], cursor: cursor, seal: true, ack: ack}
		packets = append(packets, packet)
		waits = append(waits, waiting{o, ack})
	}
	// Validate the complete group before any owner can publish a partial submit.
	for i, p := range packets {
		o := waits[i].owner
		if err := o.queues[producer].push(p, o.dead); err != nil {
			return err
		}
	}
	for _, w := range waits {
		select {
		case err := <-w.ack:
			if err != nil {
				return err
			}
		case <-w.owner.dead:
			return errors.New("owner stopped during group seal")
		}
	}
	return nil
}
