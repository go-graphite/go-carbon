package chunkstore

import (
	"context"
	"errors"
	"fmt"
)

// ArchiveValue is the authoritative visible value of one aligned archive slot.
// Present=false removes only that timestamp, preserving a newer circular owner.
type ArchiveValue struct {
	Archive   int
	Timestamp int64
	Value     float64
	Present   bool
}

// UpdateManyWithArchiveValues atomically publishes an ordinary batch and the
// original writer's bounded visible-window corrections. It is used only by the
// opt-in rollback mirror; shared-only writes keep their existing update path.
func (s *Store) UpdateManyWithArchiveValues(ctx context.Context, name string, input []Point, values []ArchiveValue) error {
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if err != nil {
		return err
	}
	if err := s.checkFileOwnership(m); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.Continuation {
		if len(values) != 0 {
			return errors.New("archive-window corrections cannot replace writer continuation state")
		}
		return s.updateContinuation(ctx, m, input, -1, nil)
	}
	for _, value := range values {
		if value.Archive < 0 || value.Archive >= len(m.Retentions) {
			return errors.New("invalid archive correction index")
		}
		ts, err := storageTimestamp(value.Timestamp)
		if err != nil || ts%m.Retentions[value.Archive].Step != 0 {
			return fmt.Errorf("invalid archive correction timestamp: %d", value.Timestamp)
		}
	}
	now, err := storageTimestamp(s.now().Unix())
	if err != nil {
		return err
	}
	if index, ok := singleArchiveCorrections(m, input, values); s.exactBlocks && ok {
		return s.updateCanonicalChunk(m, index, values)
	}
	return s.updateArchiveValues(m, input, values, now)
}

func (s *Store) updateArchiveValues(m Metadata, input []Point, values []ArchiveValue, now int) error {
	writer := newChunkWriter(s, m)
	defer writer.release()
	if err := updateAutomatic(writer, m, input, now); err != nil {
		return err
	}
	for _, value := range values {
		ts := int(value.Timestamp)
		var err error
		if value.Present {
			err = writer.setPoint(value.Archive, ts, value.Value)
		} else {
			err = writer.removePoint(value.Archive, ts)
		}
		if err != nil {
			return err
		}
	}
	if !writer.changed() {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	return s.commitUpdate(batch, writer, m)
}

func (w *chunkWriter) removePoint(archive, timestamp int) error {
	r := w.metadata.Retentions[archive]
	slot := (timestamp / r.Step) % r.Count
	if slot < 0 {
		return nil
	}
	c, err := w.load(archive, slot)
	if err != nil {
		return err
	}
	offset := slot % chunkSlots
	if !c.current.has(offset) || c.current.Points[offset].Timestamp != int64(timestamp) {
		return nil
	}
	c.current.Present[offset/64] &^= uint64(1) << uint(offset%64)
	c.current.Points[offset] = Point{}
	// The merge format has no tombstone operand. Materialize this chunk in the
	// same batch so older operands cannot resurrect a removed virtual rollup.
	c.forceSet = true
	c.dirty = true
	w.dirty = true
	return nil
}
