package chunkstore

import (
	"time"
)

// A complete single-archive correction overwrites every automatic input write.
// Restrict this path to one chunk and present values: rollups, conditional
// removals and incomplete corrections still need the ordinary read/modify path.
func singleArchiveCorrections(m Metadata, input []Point, values []ArchiveValue) (int, bool) {
	if len(m.Retentions) != 1 || len(values) == 0 {
		return 0, false
	}
	r := m.Retentions[0]
	index := -1
	var owners [chunkSlots]int64
	var present [2]uint64
	for _, value := range values {
		if value.Archive != 0 || !value.Present || value.Timestamp <= 0 || value.Timestamp%int64(r.Step) != 0 {
			return 0, false
		}
		slot := int(value.Timestamp/int64(r.Step)) % r.Count
		if index < 0 {
			index = slot / chunkSlots
		} else if index != slot/chunkSlots {
			return 0, false
		}
		offset := slot % chunkSlots
		owners[offset] = value.Timestamp
		present[offset/64] |= uint64(1) << uint(offset%64)
	}
	for _, point := range input {
		timestamp, err := storageTimestamp(point.Timestamp)
		if err != nil || timestamp <= 0 {
			return 0, false
		}
		timestamp = align(timestamp, r.Step)
		slot := (timestamp / r.Step) % r.Count
		offset := slot % chunkSlots
		if slot/chunkSlots != index || present[offset/64]&(uint64(1)<<uint(offset%64)) == 0 || owners[offset] != int64(timestamp) {
			return 0, false
		}
	}
	return index, true
}

// A run permission belongs to this process and the last successful canonical
// batch only. Dense IDs cost one bit each; 64 independently locked shards are
// capped at 16 pages each (8 MiB total). Eviction and restart cause a safe miss.
func (s *Store) canonicalRun(id uint64) *IDSet {
	return &s.canonicalRuns[(id>>16)%uint64(len(s.canonicalRuns))]
}

func sameCanonicalHead(previous, observed time.Time, r Retention) bool {
	if previous.IsZero() || previous.After(observed) || previous.Unix() < 0 {
		return false
	}
	a, b := previous.Unix()/int64(r.Step), observed.Unix()/int64(r.Step)
	return a/int64(r.Count) == b/int64(r.Count) && (a%int64(r.Count))/chunkSlots == (b%int64(r.Count))/chunkSlots
}

func (s *Store) updateCanonicalChunk(m Metadata, index int, values []ArchiveValue) error {
	run := s.canonicalRun(m.ID)
	known := run.Contains(m.ID)
	// A failed write must never leave a reusable permission.
	run.Clear(m.ID)
	observed := s.now()
	now, err := storageTimestamp(observed.Unix())
	if err != nil {
		return err
	}
	r := m.Retentions[0]
	head := ((now / r.Step) % r.Count) / chunkSlots
	activity := nextActivityAt(m.LastUpdate, observed)
	track := index == head && activity.Equal(time.Unix(0, observed.UnixNano()))
	// Other writes clear the run. Therefore every fourth metric revision really
	// is a checkpoint within this uninterrupted run, rather than a revision
	// consumed by a different chunk or a reconciliation mutation.
	blind := track && known && sameCanonicalHead(m.LastUpdate, observed, r) && (m.Revision+1)%deltasBeforeSet != 0
	w := newChunkWriter(s, m)
	defer w.release()
	if blind {
		c := writeChunks.Get().(*writeChunk)
		*c = writeChunk{}
		w.chunks[chunkAddress{archive: 0, index: index}] = c
	} else {
		c, err := w.load(0, index*chunkSlots)
		if err != nil {
			return err
		}
		if track {
			c.forceSet = true
		}
	}
	for _, value := range values {
		if err := w.setPoint(0, int(value.Timestamp), value.Value); err != nil {
			return err
		}
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := s.commitUpdateAt(b, w, m, activity); err != nil {
		return err
	}
	if track {
		run.markBounded(m.ID, 16)
	}
	s.mirrorCanonical.Add(1)
	if blind {
		s.mirrorBlind.Add(1)
	}
	return nil
}
