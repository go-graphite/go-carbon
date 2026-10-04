package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/pebble"
)

func (s *Store) UpdateMany(ctx context.Context, name string, input []Point) error {
	return s.updateByName(ctx, name, input, -1)
}

// UpdateManyForArchive writes directly into a selected archive. It deliberately
// does not propagate, which permits archive-preserving imports.
func (s *Store) UpdateManyForArchive(ctx context.Context, name string, input []Point, targetRetention int) error {
	return s.updateByName(ctx, name, input, targetRetention)
}

func (s *Store) updateByName(ctx context.Context, name string, input []Point, targetRetention int) error {
	unlock := s.lockMetric(name)
	defer unlock()
	m, err := metadataFrom(s.db, name)
	if err != nil {
		return err
	}
	return s.update(ctx, m, input, targetRetention)
}

func (s *Store) update(ctx context.Context, m Metadata, input []Point, targetRetention int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	w := newChunkWriter(s, m)
	now, err := storageTimestamp(s.now().Unix())
	if err != nil {
		return fmt.Errorf("clock timestamp: %w", err)
	}
	if targetRetention >= 0 {
		archive := targetArchive(m.Retentions, 0, targetRetention)
		if archive < 0 {
			return fmt.Errorf("target retention %d not found", targetRetention)
		}
		for _, p := range input {
			timestamp, err := storageTimestamp(p.Timestamp)
			if err != nil {
				return err
			}
			if err := w.setPoint(archive, align(timestamp, m.Retentions[archive].SecondsPerPoint()), p.Value); err != nil {
				return err
			}
		}
	} else {
		// This preserves classic UpdateMany's newest-first routing and exact
		// retention-boundary behaviour.
		remaining := append([]Point(nil), input...)
		for i := 0; i < len(remaining)/2; i++ {
			remaining[i], remaining[len(remaining)-i-1] = remaining[len(remaining)-i-1], remaining[i]
		}
		sort.SliceStable(remaining, func(i, j int) bool { return remaining[i].Timestamp > remaining[j].Timestamp })
		for archive := range m.Retentions {
			current, next := extractPoints(remaining, now, m.Retentions[archive].MaxRetention())
			remaining = next
			for i := 0; i < len(current)/2; i++ {
				current[i], current[len(current)-i-1] = current[len(current)-i-1], current[i]
			}
			changed := make(map[int]struct{})
			for _, p := range current {
				timestamp, err := storageTimestamp(p.Timestamp)
				if err != nil {
					return err
				}
				interval := align(timestamp, m.Retentions[archive].SecondsPerPoint())
				if err := w.setPoint(archive, interval, p.Value); err != nil {
					return err
				}
				changed[interval] = struct{}{}
			}
			if err := s.propagate(w, m, archive, changed); err != nil {
				return err
			}
			if len(remaining) == 0 {
				break
			}
		}
	}
	if m.Revision == math.MaxUint64 {
		return errors.New("metric revision exhausted")
	}
	if err := w.commit(b); err != nil {
		return err
	}
	if err := b.Set(revisionKey(m), uint64Bytes(m.Revision+1), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("sync update %s: %w", m.Name, err)
	}
	s.materializations.Add(w.materialized)
	s.operands.Add(w.operands)
	return nil
}

func extractPoints(input []Point, now, retention int) ([]Point, []Point) {
	maxAge := now - retention
	for i, point := range input {
		if point.Timestamp < int64(maxAge) {
			return input[:i], input[i:]
		}
	}
	return input, nil
}

func (s *Store) propagate(reader *chunkWriter, m Metadata, start int, changed map[int]struct{}) error {
	// Keep every original interval eligible at each lower archive. A finer
	// rollup can fail XFF while its lower-resolution bucket is already complete.
	original := make([]int, 0, len(changed))
	for timestamp := range changed {
		original = append(original, timestamp)
	}
	sort.Ints(original)
	for archive := start + 1; archive < len(m.Retentions) && len(original) > 0; archive++ {
		seen := make(map[int]struct{})
		propagated := false
		for _, timestamp := range original {
			interval := align(timestamp, m.Retentions[archive].SecondsPerPoint())
			if _, ok := seen[interval]; ok {
				continue
			}
			seen[interval] = struct{}{}
			wrote, err := s.rollup(reader, m, archive, interval)
			if err != nil {
				return err
			}
			if wrote {
				propagated = true
			}
		}
		if !propagated {
			break
		}
	}
	return nil
}

func (s *Store) rollup(reader *chunkWriter, m Metadata, archive, interval int) (bool, error) {
	higher := m.Retentions[archive-1]
	lower := m.Retentions[archive]
	need := lower.SecondsPerPoint() / higher.SecondsPerPoint()
	values := make([]float64, 0, need)
	for t := interval; t < interval+lower.SecondsPerPoint(); t += higher.SecondsPerPoint() {
		value, ok, err := reader.getPoint(archive-1, t)
		if err != nil {
			return false, err
		}
		if ok {
			values = append(values, value)
		}
	}
	if float32(len(values))/float32(need) < m.XFilesFactor || len(values) == 0 {
		return false, nil
	}
	if err := reader.setPoint(archive, interval, aggregate(m.AggregationMethod, values)); err != nil {
		return false, err
	}
	return true, nil
}
