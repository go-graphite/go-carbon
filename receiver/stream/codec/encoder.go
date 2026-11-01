package codec

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/klauspost/compress/zstd"
)

type point struct {
	ts    int64
	bits  uint64
	valid bool
}
type row struct {
	id   uint32
	ts   int64
	bits uint64
}

// Encoder keeps a connection dictionary and reusable column buffers. Flush
// before sending data, and send AppendPreamble once when opening the connection.
// Add may queue complete frames internally; callers should Flush when Ready.
type Encoder struct {
	opts         Options
	ids          map[string]uint32
	names        []string
	last         []point
	defined      int
	rows, tmp    []row
	cols         [3][]byte
	raw, pending []byte
	estimate     int
	z            *zstd.Encoder
}

func NewEncoder(o Options) (*Encoder, error) {
	o, err := options(o)
	if err != nil {
		return nil, err
	}
	e := &Encoder{opts: o, ids: make(map[string]uint32)}
	if o.Flags&FlagZstd != 0 {
		e.z, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	}
	return e, err
}

// Close releases compression resources; it does not flush buffered points.
func (e *Encoder) Close() {
	if e.z != nil {
		_ = e.z.Close()
	}
}

// Add copies name only on its first occurrence. Known names allocate nothing in
// steady state. Reaching MaxNames automatically flushes and resets the dictionary
// before assigning a new id. Empty and oversized names are rejected.
func (e *Encoder) Add(name []byte, value float64, ts int64) error {
	if len(name) == 0 || len(name) > e.opts.MaxNameSize {
		return ErrLimit
	}
	id, ok := e.ids[string(name)]
	cost := 24
	if !ok {
		cost += len(name) + 10
	}
	if cost+32 > e.opts.MaxFrameSize {
		return ErrLimit
	}
	if len(e.rows) > 0 && (len(e.rows) >= e.opts.MaxRows || e.estimate+cost+32 > e.opts.MaxFrameSize) {
		if err := e.flushPending(); err != nil {
			return err
		}
	}
	if !ok && len(e.names) >= e.opts.MaxNames {
		if err := e.Reset(); err != nil {
			return err
		}
	}
	if !ok {
		id = uint32(len(e.names))
		s := string(name)
		e.ids[s] = id
		e.names = append(e.names, s)
		e.last = append(e.last, point{})
	}
	e.rows = append(e.rows, row{id: id, ts: ts, bits: math.Float64bits(value)})
	e.estimate += cost
	return nil
}

// Ready reports that the target rows/bytes were reached or a frame was queued.
// A timer may also call Flush to bound latency on quiet connections.
func (e *Encoder) Ready() bool {
	return len(e.pending) > 0 || len(e.rows) >= e.opts.MaxRows || e.estimate >= e.opts.FrameSize
}

// Flush appends all pending frames to caller-owned dst. It emits nothing for an
// empty batch, preserves dictionary state, and reuses encoder scratch memory.
func (e *Encoder) Flush(dst []byte) ([]byte, error) {
	dst = append(dst, e.pending...)
	e.pending = e.pending[:0]
	return e.flush(dst)
}
func (e *Encoder) flushPending() error {
	var err error
	e.pending, err = e.flush(e.pending)
	return err
}

// Reset queues buffered data followed by a reset frame, clearing every id.
func (e *Encoder) Reset() error {
	if err := e.flushPending(); err != nil {
		return err
	}
	var err error
	e.pending, err = appendFrame(e.pending, nil, ResetFrame, 0, nil, e.opts.MaxFrameSize)
	if err != nil {
		return err
	}
	clear(e.ids)
	clear(e.names)
	e.names = e.names[:0]
	e.last = e.last[:0]
	e.defined = 0
	return nil
}

func (e *Encoder) flush(dst []byte) ([]byte, error) {
	if len(e.rows) == 0 {
		return dst, nil
	}
	e.rows, e.tmp = sortRows(e.rows, e.tmp, uint32(len(e.names)-1))
	raw := e.raw[:0]
	raw = binary.AppendUvarint(raw, uint64(e.defined))
	raw = binary.AppendUvarint(raw, uint64(len(e.names)-e.defined))
	for _, name := range e.names[e.defined:] {
		raw = binary.AppendUvarint(raw, uint64(len(name)))
		raw = append(raw, name...)
	}
	ids, stamps, values := e.cols[0][:0], e.cols[1][:0], e.cols[2][:0]
	prev := row{}
	for _, r := range e.rows {
		ids = binary.AppendUvarint(ids, uint64(r.id-prev.id))
		want := e.last[r.id]
		if !want.valid {
			want.ts = prev.ts
		}
		stamps = binary.AppendVarint(stamps, r.ts-want.ts)
		values = appendXor(values, r.bits^want.bits)
		e.last[r.id] = point{ts: r.ts, bits: r.bits, valid: true}
		prev = r
	}
	raw = binary.AppendUvarint(raw, uint64(len(ids)))
	raw = binary.AppendUvarint(raw, uint64(len(stamps)))
	raw = append(raw, ids...)
	raw = append(raw, stamps...)
	raw = append(raw, values...)
	e.raw = raw
	e.cols[0], e.cols[1], e.cols[2] = ids, stamps, values
	var err error
	dst, err = appendFrame(dst, raw, DataFrame, len(e.rows), e.z, e.opts.MaxFrameSize)
	if err != nil {
		return dst, err
	}
	e.defined = len(e.names)
	e.rows = e.rows[:0]
	e.estimate = 0
	return dst, nil
}

func appendXor(dst []byte, x uint64) []byte {
	if x == 0 {
		return append(dst, 0)
	}
	lead, trail := bits.LeadingZeros64(x)/8, bits.TrailingZeros64(x)/8
	n := 8 - lead - trail
	dst = append(dst, byte(1+lead*8+n-1))
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, byte(x>>(8*(trail+i))))
	}
	return dst
}

// Stable radix ordering keeps repeated ids in their original arrival order.
func sortRows(rows, tmp []row, maxID uint32) ([]row, []row) {
	sorted := true
	for i := 1; i < len(rows) && sorted; i++ {
		sorted = rows[i-1].id <= rows[i].id
	}
	if sorted {
		return rows, tmp
	}
	const digit = 11
	for shift := 0; shift == 0 || maxID>>shift > 0; shift += digit {
		var counts [1<<digit + 1]int
		for _, r := range rows {
			counts[(r.id>>shift)&(1<<digit-1)+1]++
		}
		for i := 1; i < len(counts); i++ {
			counts[i] += counts[i-1]
		}
		if cap(tmp) < len(rows) {
			tmp = make([]row, len(rows))
		}
		tmp = tmp[:len(rows)]
		for _, r := range rows {
			d := (r.id >> shift) & (1<<digit - 1)
			tmp[counts[d]] = r
			counts[d]++
		}
		rows, tmp = tmp, rows
	}
	return rows, tmp
}
