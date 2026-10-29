package chunkstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"

	"github.com/cockroachdb/pebble"
)

func chunkPrefixArchive(id, generation uint64, archive int) []byte {
	k := make([]byte, 19)
	k[0] = 'c'
	binary.BigEndian.PutUint64(k[1:], id)
	binary.BigEndian.PutUint64(k[9:], generation)
	binary.BigEndian.PutUint16(k[17:], uint16(archive))
	return k
}
func chunkKey(m Metadata, archive, index int) []byte {
	k := make([]byte, 23)
	copy(k, chunkPrefixArchive(m.ID, m.Generation, archive))
	binary.BigEndian.PutUint32(k[19:], uint32(index))
	return k
}
func prefixEnd(k []byte) []byte {
	end := append([]byte(nil), k...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil
}

type chunkAddress struct{ archive, index int }

// Reads materialize pending operands too. Keep the write-side chain shorter
// than the format limit to bound that repeated work under sustained updates.
const deltasBeforeSet = 4

type writeChunk struct {
	current        chunk
	changed        [2]uint64
	missing, dirty bool
	forceSet       bool
}

// Keep one copy of the values while a write is pending. The merge operand owns
// only the slots written by this batch, including their last duplicate value.
func (c *writeChunk) mergeDelta() chunk {
	delta := chunk{Deltas: 1, Present: c.changed}
	for word, changed := range c.changed {
		for changed != 0 {
			slot := word*64 + bits.TrailingZeros64(changed)
			delta.Points[slot] = c.current.Points[slot]
			changed &= changed - 1
		}
	}
	return delta
}

type chunkWriter struct {
	store                  *Store
	metadata               Metadata
	chunks                 map[chunkAddress]*writeChunk
	materialized, operands uint64
	dirty                  bool
}

var chunkWriters = sync.Pool{New: func() any { return new(chunkWriter) }}
var writeChunks = sync.Pool{New: func() any { return new(writeChunk) }}

func newChunkWriter(s *Store, m Metadata) *chunkWriter {
	w := chunkWriters.Get().(*chunkWriter)
	w.store, w.metadata = s, m
	if w.chunks == nil {
		w.chunks = make(map[chunkAddress]*writeChunk)
	}
	return w
}

func (w *chunkWriter) release() {
	for _, c := range w.chunks {
		writeChunks.Put(c)
	}
	// Discard an unusually large batch's map instead of keeping its capacity
	// behind an otherwise small, inactive writer. Drop catalog/store references.
	if len(w.chunks) > 64 {
		w.chunks = nil
	} else {
		clear(w.chunks)
	}
	w.store, w.metadata = nil, Metadata{}
	w.materialized, w.operands, w.dirty = 0, 0, false
	chunkWriters.Put(w)
}
func (w *chunkWriter) load(archive, slot int) (*writeChunk, error) {
	a := chunkAddress{archive, slot / chunkSlots}
	if c := w.chunks[a]; c != nil {
		return c, nil
	}
	c := writeChunks.Get().(*writeChunk)
	*c = writeChunk{}
	w.chunks[a] = c
	v, closer, err := w.store.db.Get(chunkKey(w.metadata, archive, a.index))
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		c.missing = true
	case err != nil:
		return nil, err
	default:
		err = decodeChunk(v, &c.current)
		closer.Close()
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}
func (w *chunkWriter) setPoint(archive, timestamp int, value float64) error {
	r := w.metadata.Retentions[archive]
	slot := (timestamp / r.Step) % r.Count
	if slot < 0 {
		return errors.New("negative archive slot")
	}
	c, err := w.load(archive, slot)
	if err != nil {
		return err
	}
	p := Point{Timestamp: int64(timestamp), Value: value}
	chunkSlot := slot % chunkSlots
	c.current.set(chunkSlot, p)
	c.changed[chunkSlot/64] |= uint64(1) << uint(chunkSlot%64)
	c.dirty = true
	w.dirty = true
	return nil
}
func (w *chunkWriter) getPoint(archive, timestamp int) (float64, bool, error) {
	r := w.metadata.Retentions[archive]
	slot := (timestamp / r.Step) % r.Count
	if slot < 0 {
		return 0, false, nil
	}
	c, err := w.load(archive, slot)
	if err != nil {
		return 0, false, err
	}
	p := c.current.Points[slot%chunkSlots]
	return p.Value, c.current.has(slot%chunkSlots) && p.Timestamp == int64(timestamp), nil
}
func (w *chunkWriter) commit(b *pebble.Batch) error {
	for a, c := range w.chunks {
		if !c.dirty {
			continue
		}
		k := chunkKey(w.metadata, a.archive, a.index)
		if c.missing || c.forceSet || c.current.Deltas+1 >= deltasBeforeSet {
			c.current.Deltas = 0
			w.materialized++
			if err := b.Set(k, w.store.encodeChunk(&c.current), nil); err != nil {
				return err
			}
		} else {
			delta := c.mergeDelta()
			w.operands++
			if err := b.Merge(k, w.store.encodeChunk(&delta), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *chunkWriter) changed() bool {
	return w.dirty
}

func getRange(reader pebble.Reader, m Metadata, archive, from, until int) ([]float64, error) {
	r := m.Retentions[archive]
	step, slots := r.Step, r.Count
	values := make([]float64, (until-from)/step)
	for i := range values {
		values[i] = math.NaN()
	}
	remaining := len(values)
	slot := (from / step) % slots
	if slot < 0 {
		slot += slots
	}
	zeroInRange := from <= 0 && 0 < until
	zeroSlotPresent := false
	for remaining > 0 {
		count := min(remaining, slots-slot)
		present, err := readChunkRange(reader, m, archive, slot, count, from, until, step, values, zeroInRange)
		if err != nil {
			return nil, err
		}
		zeroSlotPresent = zeroSlotPresent || present
		remaining -= count
		slot = 0
	}
	if err := applyEpochZeroSentinel(reader, m, archive, from, step, values, zeroInRange, zeroSlotPresent); err != nil {
		return nil, err
	}
	return values, nil
}

func readChunkRange(reader pebble.Reader, m Metadata, archive, slot, count, from, until, step int, values []float64, zeroInRange bool) (bool, error) {
	lower := chunkKey(m, archive, slot/chunkSlots)
	upper := chunkKey(m, archive, (slot+count-1)/chunkSlots+1)
	it, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, fmt.Errorf("iterate chunks: %w", err)
	}
	var c chunk
	zeroSlotPresent := false
	for it.First(); it.Valid(); it.Next() {
		if err := decodeChunk(it.Value(), &c); err != nil {
			it.Close()
			return false, err
		}
		if zeroInRange && binary.BigEndian.Uint32(it.Key()[19:]) == 0 && c.has(0) {
			zeroSlotPresent = true
		}
		setRangeValues(&c, from, until, step, values)
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return false, fmt.Errorf("read chunks: %w", err)
	}
	return zeroSlotPresent, nil
}

func setRangeValues(c *chunk, from, until, step int, values []float64) {
	for slot := range c.Points {
		point := &c.Points[slot]
		if !c.has(slot) || point.Timestamp < int64(from) || point.Timestamp >= int64(until) {
			continue
		}
		index := (point.Timestamp - int64(from)) / int64(step)
		if index >= 0 && index < int64(len(values)) {
			values[index] = point.Value
		}
	}
}

func applyEpochZeroSentinel(reader pebble.Reader, m Metadata, archive, from, step int, values []float64, zeroInRange, zeroSlotPresent bool) error {
	if !zeroInRange || zeroSlotPresent {
		return nil
	}
	// Classic exposes the empty-slot timestamp zero on a query crossing the
	// Unix epoch, but only when the archive contains another point.
	hasPoints, err := hasArchivePoint(reader, m, archive)
	if err != nil {
		return err
	}
	if hasPoints {
		values[-from/step] = 0
	}
	return nil
}
