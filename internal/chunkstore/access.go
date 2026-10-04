package chunkstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

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
	current, delta chunk
	missing, dirty bool
}
type chunkWriter struct {
	store                  *Store
	metadata               Metadata
	chunks                 map[chunkAddress]*writeChunk
	materialized, operands uint64
}

func newChunkWriter(s *Store, m Metadata) *chunkWriter {
	return &chunkWriter{store: s, metadata: m, chunks: make(map[chunkAddress]*writeChunk)}
}
func (w *chunkWriter) load(archive, slot int) (*writeChunk, error) {
	a := chunkAddress{archive, slot / chunkSlots}
	if c := w.chunks[a]; c != nil {
		return c, nil
	}
	c := new(writeChunk)
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
	w.chunks[a] = c
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
	c.current.set(slot%chunkSlots, p)
	c.delta.set(slot%chunkSlots, p)
	c.dirty = true
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
		if c.missing || c.current.Deltas+1 >= deltasBeforeSet {
			c.current.Deltas = 0
			w.materialized++
			if err := b.Set(k, encodeChunk(&c.current), nil); err != nil {
				return err
			}
		} else {
			c.delta.Deltas = 1
			w.operands++
			if err := b.Merge(k, encodeChunk(&c.delta), nil); err != nil {
				return err
			}
		}
	}
	return nil
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
		lower := chunkKey(m, archive, slot/chunkSlots)
		upper := chunkKey(m, archive, (slot+count-1)/chunkSlots+1)
		it, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, fmt.Errorf("iterate chunks: %w", err)
		}
		var c chunk
		for it.First(); it.Valid(); it.Next() {
			if err := decodeChunk(it.Value(), &c); err != nil {
				it.Close()
				return nil, err
			}
			if zeroInRange && binary.BigEndian.Uint32(it.Key()[19:]) == 0 && c.has(0) {
				zeroSlotPresent = true
			}
			for i, p := range c.Points {
				if !c.has(i) || p.Timestamp < int64(from) || p.Timestamp >= int64(until) {
					continue
				}
				index := (p.Timestamp - int64(from)) / int64(step)
				if index >= 0 && index < int64(len(values)) {
					values[index] = p.Value
				}
			}
		}
		if err := errors.Join(it.Error(), it.Close()); err != nil {
			return nil, fmt.Errorf("read chunks: %w", err)
		}
		remaining -= count
		slot = 0
	}
	if zeroInRange && !zeroSlotPresent {
		// Classic exposes the empty-slot timestamp zero on a query crossing
		// the Unix epoch, but only when the archive contains another point.
		hasPoints, err := hasArchivePoint(reader, m, archive)
		if err != nil {
			return nil, err
		}
		if hasPoints {
			values[-from/step] = 0
		}
	}
	return values, nil
}
