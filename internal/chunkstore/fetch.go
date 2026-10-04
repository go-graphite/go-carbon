package chunkstore

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
)

func (s *Store) Fetch(ctx context.Context, name string, fromTime, untilTime int) (*Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromTime > untilTime {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	m, err := metadataFrom(snapshot, name)
	if err != nil {
		return nil, err
	}
	now, err := storageTimestamp(s.now().Unix())
	if err != nil {
		return nil, fmt.Errorf("clock timestamp: %w", err)
	}
	oldest := now - maxRetention(m.Retentions)
	if fromTime > now || untilTime < oldest {
		return nil, nil
	}
	if fromTime < oldest {
		fromTime = oldest
	}
	if untilTime > now {
		untilTime = now
	}
	archive := targetArchive(m.Retentions, now-fromTime, -1)
	if archive < 0 {
		return nil, nil
	}
	step := m.Retentions[archive].SecondsPerPoint()
	from := interval(fromTime, step)
	until := interval(untilTime, step)
	if from == until {
		hasPoints, err := hasArchivePoint(snapshot, m, archive)
		if err != nil {
			return nil, err
		}
		if hasPoints {
			until += step
		}
	}
	values, err := getRange(snapshot, m, archive, from, until)
	if err != nil {
		return nil, err
	}
	return &Series{Metadata: m, FromTime: from, UntilTime: until, Step: step, Values: values}, nil
}

func hasArchivePoint(reader pebble.Reader, m Metadata, archive int) (bool, error) {
	prefix := chunkPrefixArchive(m.ID, m.Generation, archive)
	it, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return false, fmt.Errorf("iterate archive: %w", err)
	}
	defer it.Close()
	return it.First() && it.Valid(), it.Error()
}
