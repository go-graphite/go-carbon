package chunkstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

func (s *Store) Fetch(ctx context.Context, name string, fromTime, untilTime int) (*Series, error) {
	if err := s.fileOwnershipError(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromTime > untilTime {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	// A complete catalog view can route original-only and missing metrics
	// without opening a Pebble snapshot or reading its catalog tables.
	if !s.catalogMembership.mayContain(name) {
		return nil, ErrNotFound
	}
	// The metadata cache answers only for state committed before the view
	// opens, judged by the epoch captured first (see stateCache).
	cache := s.stateCache
	var stripe, epoch uint64
	if cache != nil {
		stripe = s.metricStripe(name)
		epoch = cache.epoch(stripe)
	}
	// A read view rather than a snapshot: opening and closing a snapshot take
	// Pebble's DB mutex, which flushes, compactions and version changes hold
	// for milliseconds, and every concurrent fetch queued behind them.
	snapshot := s.db.NewReadView()
	defer snapshot.Close()
	var state *metricState
	var m Metadata
	var err error
	if cache != nil {
		state = cache.get(stripe, name, epoch)
	}
	if state != nil {
		m = state.meta
		m.MetricConfig = cloneConfig(m.MetricConfig)
	} else if m, err = metadataFrom(snapshot, name); err != nil {
		return nil, err
	}
	if err := s.checkFileOwnership(m); err != nil {
		return nil, err
	}
	if m.Continuation {
		if s.continuation == nil {
			return nil, ErrFormat
		}
		io := s.newContinuationIO(ctx, snapshot, nil, m)
		io.state = s.newStateView(state, m)
		if s.fetchSharedIterator {
			io.withIterator()
		}
		series, err := s.continuation.Fetch(ctx, io, m, fromTime, untilTime, s.now)
		err = errors.Join(err, io.Close())
		if err == nil && state == nil && cache != nil {
			if io.state != nil {
				cache.fill(stripe, name, epoch, io.state.final(m))
			} else {
				cache.fill(stripe, name, epoch, newMetricState(m).seal())
			}
		}
		return series, err
	}
	if state == nil && cache != nil {
		cache.fill(stripe, name, epoch, newMetricState(m).seal())
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

// Closing a snapshot takes Pebble's manifest lock to reconsider compactions, so
// an inline close waits behind every flush, compaction and ingestion syncing
// the manifest. Fetches hand snapshots to background closers instead; a full
// queue falls back to closing inline.
const snapshotReleaseQueue = 1 << 16

func (s *Store) startSnapshotRelease() {
	s.snapshotRelease = make(chan *pebble.Snapshot, snapshotReleaseQueue)
	s.snapshotReleased = make(chan struct{})
	go func() {
		defer close(s.snapshotReleased)
		for snapshot := range s.snapshotRelease {
			_ = snapshot.Close()
		}
	}()
}

func (s *Store) releaseSnapshot(snapshot *pebble.Snapshot) {
	select {
	case s.snapshotRelease <- snapshot:
	default:
		_ = snapshot.Close()
	}
}

// stopSnapshotRelease closes every queued snapshot. Readers must be stopped.
func (s *Store) stopSnapshotRelease() {
	if s.snapshotRelease != nil {
		close(s.snapshotRelease)
		<-s.snapshotReleased
	}
}
