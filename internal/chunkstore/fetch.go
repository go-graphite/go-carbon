package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// An iterator pins native SST index blocks even when they exceed a cache shard's
// capacity and cannot be cached. Per-request render fan-out is not a store-wide
// bound: thousands of cold requests used to allocate a private 158 MiB index
// each. Admit before opening a view and retain the slot until all iterators and
// value handles close. Queued callers hold no Pebble buffers. Block loading is
// separately bounded for writeout and background readers as well.
const (
	maxConcurrentStoreReads = 32
	maxConcurrentBlockLoads = 16
)

// readAdmission is a store read slot taken by AdmitRead and carried by its
// context. Fetch and Metadata calls with that context use it instead of
// waiting for another slot until the caller releases it.
type readAdmission struct {
	store    *Store
	released atomic.Bool
}

type readAdmissionKey struct{}

// AdmitRead takes a store read slot for name before the caller takes
// OriginalReadGuard. A reader queued for a slot then holds no guard stripe, so
// a read queue cannot stall persister writes (SharedWriteGuard) on the same
// stripes: in one production stall 5,576 queued readers held stripes and
// blocked 93 persister workers. Fetch and Metadata with the returned context
// reuse the slot. The release function is idempotent and must always be
// called; callers release as soon as the store reads are done. Names the
// catalog rules out take no slot, because Fetch answers them without one.
func (s *Store) AdmitRead(ctx context.Context, name string) (context.Context, func(), error) {
	if !s.catalogMembership.mayContain(name) {
		return ctx, func() {}, nil
	}
	if err := s.readSlots.Acquire(ctx, 1); err != nil {
		return ctx, func() {}, err
	}
	a := &readAdmission{store: s}
	var once sync.Once
	release := func() {
		once.Do(func() {
			a.released.Store(true)
			s.readSlots.Release(1)
		})
	}
	return context.WithValue(ctx, readAdmissionKey{}, a), release, nil
}

// acquireReadSlot uses a held admission from ctx, or waits for a slot.
func (s *Store) acquireReadSlot(ctx context.Context) (func(), error) {
	if a, ok := ctx.Value(readAdmissionKey{}).(*readAdmission); ok && a.store == s && !a.released.Load() {
		return func() {}, nil
	}
	if err := s.readSlots.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { s.readSlots.Release(1) }, nil
}

// Now is the clock Fetch chooses archives and clamps ranges by.
func (s *Store) Now() time.Time { return s.now() }

func (s *Store) Fetch(ctx context.Context, name string, fromTime, untilTime int) (*Series, error) {
	return s.fetch(ctx, name, fromTime, untilTime, nil)
}

func (s *Store) fetch(ctx context.Context, name string, fromTime, untilTime int, prefix *fetchPrefix) (*Series, error) {
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
	releaseSlot, err := s.acquireReadSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseSlot()
	// The metadata cache answers only for state committed before the view
	// opens, judged by the epoch captured first (see stateCache).
	cache := s.stateCache
	var stripe, epoch uint64
	if cache != nil {
		stripe = s.metricStripe(name)
		epoch = cache.epoch(stripe)
	}
	// Opening a read view avoids the DB mutex used by registered snapshots.
	// Its final release can still wait when it retires an old read state.
	started := time.Now()
	snapshot := s.db.NewReadView()
	defer func() {
		closing := time.Now()
		_ = snapshot.Close()
		closed := time.Now()
		s.fetchTimings.body.add(closing.Sub(started))
		s.fetchTimings.readViewClose.add(closed.Sub(closing))
	}()
	var state *metricState
	var m Metadata
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
	if prefix != nil && prefix.plan(m, fromTime, untilTime, s.now().Unix(), s.ReadCacheEpoch()) {
		if prefix.end == prefix.until {
			return prefix.join(m, nil), nil
		}
		fromTime = prefix.until - prefix.series.Step
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
		if io.iter != nil {
			closing := time.Now()
			err = errors.Join(err, io.Close())
			s.fetchTimings.iteratorClose.add(time.Since(closing))
		} else {
			err = errors.Join(err, io.Close())
		}
		if err == nil && state == nil && cache != nil {
			if io.state != nil {
				cache.fill(stripe, name, epoch, io.state.final(m))
			} else {
				cache.fill(stripe, name, epoch, newMetricState(m).seal())
			}
		}
		if err == nil && prefix != nil && prefix.count > 0 {
			return prefix.join(m, series), nil
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
	series := &Series{Metadata: m, FromTime: from, UntilTime: until, Step: step, Values: values}
	if prefix != nil && prefix.count > 0 {
		return prefix.join(m, series), nil
	}
	return series, nil
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
