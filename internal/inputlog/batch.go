package inputlog

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/go-graphite/go-carbon/points"
)

// Batch owns an immutable staging window. Refs/Points is the original
// columnar representation. Packed batches instead retain one Ref and end
// index per consecutive reference run, plus lossless timestamp deltas and
// value XORs. The two four-bit lengths in each descriptor select 0..8 bytes
// for each integer. Only this in-memory representation changes: streamEncoder
// still emits the same durable frames, in the same arrival order.
// A consumed batch must be Reset before returning it to its source.
type Batch struct {
	Refs      []*StreamRef
	Points    []points.Point
	Ends      []uint32
	Packed    []byte
	n         int
	ts, value uint64
}

func (b *Batch) Len() int {
	if b.n != 0 {
		return b.n
	}
	return len(b.Points)
}
func (b *Batch) Bytes() int64 {
	return int64(cap(b.Refs))*8 + int64(cap(b.Points))*16 + int64(cap(b.Ends))*4 + int64(cap(b.Packed))
}

// Reset releases item references even when the backing arrays are reused.
func (b *Batch) Reset() {
	clear(b.Refs)
	b.Refs = b.Refs[:0]
	b.Points = b.Points[:0]
	b.Ends = b.Ends[:0]
	b.Packed = b.Packed[:0]
	b.n = 0
	b.ts = 0
	b.value = 0
}

// Append reports backing-array growth so the cache need not total all four
// column capacities before and after every accepted point.
func (b *Batch) Append(ref *StreamRef, value float64, timestamp int64) bool {
	grew := false
	if len(b.Refs) == 0 || b.Refs[len(b.Refs)-1] != ref {
		grew = len(b.Refs) == cap(b.Refs) || len(b.Ends) == cap(b.Ends)
		b.Refs = append(b.Refs, ref)
		b.Ends = append(b.Ends, 0)
	}

	d := int64(uint64(timestamp) - b.ts)
	t := uint64(d<<1) ^ uint64(d>>63)
	v := math.Float64bits(value) ^ b.value
	nt, nv := uint((bits.Len64(t)+7)/8), uint((bits.Len64(v)+7)/8)
	at := len(b.Packed)
	// Reserve a whole worst-case row. The decoder can then load each field
	// with one ordinary 64-bit load, including the last short field. Padding
	// is never part of Len or emitted bytes; no unsafe or overread is used.
	if cap(b.Packed)-at < 17 {
		grew = true
		b.Packed = append(b.Packed, make([]byte, 17)...)
		b.Packed = b.Packed[:at]
	}
	out := b.Packed[:at+17]
	out[at] = byte(nt | nv<<4)
	binary.LittleEndian.PutUint64(out[at+1:], t)
	binary.LittleEndian.PutUint64(out[at+1+int(nt):], v)
	b.Packed = out[:at+1+int(nt+nv)]
	b.n++
	b.ts = uint64(timestamp)
	b.value = math.Float64bits(value)

	b.Ends[len(b.Ends)-1] = uint32(b.n)
	return grew
}

func (b *Batch) AppendPoints(ref *StreamRef, data []points.Point) {
	if len(data) == 0 {
		return
	}
	if len(b.Refs) == 0 || b.Refs[len(b.Refs)-1] != ref {
		b.Refs = append(b.Refs, ref)
		b.Ends = append(b.Ends, 0)
	}
	// Reserve once for the entire batch and keep the predictor in registers.
	// The retained stream contains only the used prefix, never these pad bytes.
	at := len(b.Packed)
	if cap(b.Packed)-at < 17*len(data) {
		b.Packed = append(b.Packed, make([]byte, 17*len(data))...)
		b.Packed = b.Packed[:at]
	}
	out := b.Packed[:cap(b.Packed)]
	ts, value := b.ts, b.value
	for _, p := range data {
		d := int64(uint64(p.Timestamp) - ts)
		t := uint64(d<<1) ^ uint64(d>>63)
		current := math.Float64bits(p.Value)
		v := current ^ value
		nt, nv := uint((bits.Len64(t)+7)/8), uint((bits.Len64(v)+7)/8)
		out[at] = byte(nt | nv<<4)
		binary.LittleEndian.PutUint64(out[at+1:], t)
		binary.LittleEndian.PutUint64(out[at+1+int(nt):], v)
		at += 1 + int(nt+nv)
		ts, value = uint64(p.Timestamp), current
	}
	b.Packed = out[:at]
	b.ts, b.value = ts, value
	b.n += len(data)
	b.Ends[len(b.Ends)-1] = uint32(b.n)
}

type batchReader struct {
	data      []byte
	at        int
	ts, value uint64
}

func (r *batchReader) next() (int64, uint64) {
	descriptor := r.data[r.at]
	nt, nv := uint(descriptor&15), uint(descriptor>>4)
	t := binary.LittleEndian.Uint64(r.data[r.at+1:]) & (^uint64(0) >> ((8 - nt) * 8))
	v := binary.LittleEndian.Uint64(r.data[r.at+1+int(nt):]) & (^uint64(0) >> ((8 - nv) * 8))
	r.at += 1 + int(nt+nv)
	r.ts += uint64(int64(t>>1) ^ -int64(t&1))
	r.value ^= v
	return int64(r.ts), r.value
}

// Entries expands staging for legacy Source consumers and correctness oracles.
// The log's hot path consumes the packed fields directly into its row scratch.
func (b *Batch) Entries(dst []Entry) []Entry {
	if b.n == 0 {
		for i, p := range b.Points {
			dst = append(dst, b.Refs[i].Entry(p.Value, p.Timestamp))
		}
		return dst
	}
	r := batchReader{data: b.Packed[:cap(b.Packed)]}
	n := 0
	for i, ref := range b.Refs {
		for ; n < int(b.Ends[i]); n++ {
			ts, v := r.next()
			dst = append(dst, ref.Entry(math.Float64frombits(v), ts))
		}
	}
	return dst
}

func (s *streamEncoder) addBatch(w *streamScratch, b *Batch) {
	if b.n == 0 {
		for i, p := range b.Points {
			s.add(w, b.Refs[i].Entry(p.Value, p.Timestamp))
		}
		return
	}
	data := b.Packed[:cap(b.Packed)]
	at := 0
	var timestamp, value uint64
	n := 0
	for i, ref := range b.Refs {
		cached := ref.state.Load()
		id := uint32(cached)
		if uint32(cached>>32) != s.epoch {
			id = s.lookup(w, ref)
		}
		count := int(b.Ends[i]) - n
		start := len(w.rows)
		w.rows = slices.Grow(w.rows, count)
		w.rows = w.rows[:start+count]
		// Decode directly into the final row scratch, keeping the predictors in
		// registers. The Entry byte oracle covers this bulk consumption path.
		for j := range w.rows[start:] {
			descriptor := data[at]
			nt, nv := uint(descriptor&15), uint(descriptor>>4)
			t := binary.LittleEndian.Uint64(data[at+1:]) & (^uint64(0) >> ((8 - nt) * 8))
			v := binary.LittleEndian.Uint64(data[at+1+int(nt):]) & (^uint64(0) >> ((8 - nv) * 8))
			at += 1 + int(nt+nv)
			timestamp += uint64(int64(t>>1) ^ -int64(t&1))
			value ^= v
			w.rows[start+j] = row{id: id, ts: int64(timestamp), bits: value}
		}
		n += count
	}
}

// BatchSource has the same generation and spare ownership contract as Source.
// This is an in-memory interface; the log's durable codecs are unchanged.
type BatchSource interface {
	TakeLogBatch(shard int, gen uint64, spare Batch) Batch
}
