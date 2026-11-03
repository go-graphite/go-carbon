package inputlog

import (
	"math"
	"slices"

	"github.com/go-graphite/go-carbon/points"
)

// Batch stages accepted points in two immutable columns (24 bytes per point).
// Keeping raw points avoids a second compression pass during ingestion and a
// matching decode before the durable encoder. Consecutive references are
// resolved once per run by the consumer instead of tracked by the producer.
type Batch struct {
	Refs   []*StreamRef
	Points []points.Point
}

func (b *Batch) Len() int     { return len(b.Points) }
func (b *Batch) Bytes() int64 { return int64(cap(b.Refs))*8 + int64(cap(b.Points))*16 }

// Reset releases item references while keeping the arrays available for reuse.
func (b *Batch) Reset() {
	clear(b.Refs)
	b.Refs = b.Refs[:0]
	b.Points = b.Points[:0]
}

// Append reports capacity changes for cache byte accounting.
func (b *Batch) Append(ref *StreamRef, value float64, timestamp int64) bool {
	grew := len(b.Refs) == cap(b.Refs) || len(b.Points) == cap(b.Points)
	if grew {
		b.Refs = growStaging(b.Refs, len(b.Refs)+1)
		b.Points = growStaging(b.Points, len(b.Points)+1)
	}
	b.Refs = append(b.Refs, ref)
	b.Points = append(b.Points, points.Point{Value: value, Timestamp: timestamp})
	return grew
}

func (b *Batch) AppendPoints(ref *StreamRef, data []points.Point) {
	n := len(b.Points)
	b.Refs = growStaging(b.Refs, n+len(data))
	b.Points = growStaging(b.Points, n+len(data))
	b.Refs = b.Refs[:n+len(data)]
	for i := n; i < len(b.Refs); i++ {
		b.Refs[i] = ref
	}
	b.Points = append(b.Points, data...)
}

func growStaging[T any](old []T, required int) []T {
	if cap(old) >= required {
		return old
	}
	capacity := max(required, cap(old)+max(cap(old)/16, 8))
	next := append([]T(nil), make([]T, capacity)...)
	copy(next, old)
	return next[:len(old)]
}

// Recycle releases excess capacity after a burst; ordinary windows reuse it.
func (b *Batch) Recycle() {
	n := b.Len()
	b.Reset()
	if cap(b.Refs) > max(4*n, 64) {
		b.Refs = make([]*StreamRef, 0, 2*n)
	}
	if cap(b.Points) > max(4*n, 64) {
		b.Points = make([]points.Point, 0, 2*n)
	}
}

// Entries preserves the original Source contract and the durable byte oracle.
func (b *Batch) Entries(dst []Entry) []Entry {
	for i, p := range b.Points {
		dst = append(dst, b.Refs[i].Entry(p.Value, p.Timestamp))
	}
	return dst
}

func (s *streamEncoder) addBatch(w *streamScratch, b *Batch) {
	start := len(w.rows)
	w.rows = slices.Grow(w.rows, b.Len())
	w.rows = w.rows[:start+b.Len()]
	var previous *StreamRef
	var id uint32
	for i, p := range b.Points {
		ref := b.Refs[i]
		if ref != previous {
			cached := ref.state.Load()
			id = uint32(cached)
			if uint32(cached>>32) != s.epoch {
				id = s.lookup(w, ref)
			}
			previous = ref
		}
		w.rows[start+i] = row{id: id, ts: p.Timestamp, bits: math.Float64bits(p.Value)}
	}
}

// BatchSource has the same generation and spare ownership contract as Source.
// This is an in-memory interface; the log's durable codecs are unchanged.
type BatchSource interface {
	TakeLogBatch(shard int, gen uint64, spare Batch) Batch
}
