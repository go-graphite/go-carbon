package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

// Relayout moves the v6 blocks of metrics converted before format v7 into the
// history bucket, so their cold history stops sitting beside hot keys. Metrics
// are processed in ID order: each group's copies form one table covering only
// the group's ID range in the history bucket, disjoint from other groups and
// from v7 conversions (whose IDs are higher), so it ingests into the bottom
// level. Each block then switches under its metric lock only if its descriptor
// is unchanged; otherwise its copies are deleted. A crash between ingestion and
// the switch leaves copies that the resumed run writes again.
const (
	relayoutTargetBytes = 64 << 20
	relayoutMaxMetrics  = 4096
)

func relayoutCursorKey() []byte { return []byte("z/relayout") }

// RelayoutStats reports progress of the current process.
type RelayoutStats struct {
	Metrics, Blocks, Skipped, Bytes uint64
	Cursor                          uint64
	// Ceiling is the highest metric ID the run covers; Cursor/Ceiling is its
	// progress through the ID-ordered catalog.
	Ceiling uint64
	Done    bool
	// Rate is the current pacing in bytes per second.
	Rate int64
	// Error is why a background run stopped, if it did.
	Error string
}

type relayoutState struct {
	metrics, blocks, skipped, bytes, cursor atomic.Uint64
	ceiling                                 atomic.Uint64
	done                                    atomic.Bool
	err                                     atomic.Pointer[string]
	// rate is the live pacing in bytes per second; zero copies unpaced.
	rate atomic.Int64
	// running admits one relayout at a time: two runs planning the same
	// metric would let the second delete the copies the first made live.
	running atomic.Bool
}

// SetRelayoutRate changes the pacing of a running relayout. A configuration
// reload uses it to slow, pause (a tiny rate) or speed up the background move
// without restarting.
func (s *Store) SetRelayoutRate(bytesPerSecond int64) error {
	if bytesPerSecond <= 0 {
		return errors.New("relayout rate must be positive")
	}
	s.relayout.rate.Store(bytesPerSecond)
	return nil
}

// paceRelayout sleeps until the bytes copied since the previous call fit the
// current rate. Rechecking the rate at least every second lets a reload take
// effect promptly, and pacing per interval keeps a stalled period from
// earning a burst afterwards.
func (s *Store) paceRelayout(ctx context.Context, paced *uint64, pacedAt *time.Time) error {
	for {
		rate := s.relayout.rate.Load()
		copied := s.relayout.bytes.Load() - *paced
		ahead := time.Duration(float64(copied)/float64(rate)*float64(time.Second)) - time.Since(*pacedAt)
		if rate <= 0 || ahead <= 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(ahead, time.Second)):
		}
	}
	*paced, *pacedAt = s.relayout.bytes.Load(), time.Now()
	return nil
}

type relayoutItem struct {
	m    Metadata
	plan RelayoutPlan
}

func (s *Store) RelayoutStats() RelayoutStats {
	r := &s.relayout
	stats := RelayoutStats{Metrics: r.metrics.Load(), Blocks: r.blocks.Load(), Skipped: r.skipped.Load(), Bytes: r.bytes.Load(), Cursor: r.cursor.Load(), Ceiling: r.ceiling.Load(), Done: r.done.Load(), Rate: r.rate.Load()}
	if e := r.err.Load(); e != nil {
		stats.Error = *e
	}
	return stats
}

// RelayoutContinuation runs until every v6 block of metrics existing at start
// is moved, ctx ends or an error occurs. bytesPerSecond > 0 paces the copies;
// SetRelayoutRate changes the pacing while it runs.
func (s *Store) RelayoutContinuation(ctx context.Context, bytesPerSecond int64) error {
	if !s.relayout.running.CompareAndSwap(false, true) {
		return errors.New("relayout already running")
	}
	defer s.relayout.running.Store(false)
	s.relayout.rate.Store(bytesPerSecond)
	if !s.timeBuckets {
		return fmt.Errorf("%w: relayout requires time buckets", ErrFormat)
	}
	planner, ok := s.continuation.(ContinuationRelayouter)
	if !ok {
		return fmt.Errorf("%w: continuation codec cannot relayout", ErrFormat)
	}
	ceiling, err := s.CatalogHighWatermark(ctx)
	if err != nil {
		return err
	}
	s.relayout.ceiling.Store(ceiling)
	cursor, done, err := s.relayoutCursor()
	if err != nil || done {
		s.relayout.done.Store(done)
		return err
	}
	s.relayout.cursor.Store(cursor)
	candidates, err := s.relayoutCandidates(ctx, cursor, ceiling)
	if err != nil {
		return err
	}
	paced, pacedAt := s.relayout.bytes.Load(), time.Now()
	var group []relayoutItem
	groupBytes := 0
	flush := func(last uint64) error {
		if len(group) > 0 {
			if err := s.relayoutGroup(ctx, group); err != nil {
				return err
			}
		}
		group, groupBytes = group[:0], 0
		if err := s.setRelayoutCursor(last, false); err != nil {
			return err
		}
		s.relayout.cursor.Store(last)
		return s.paceRelayout(ctx, &paced, &pacedAt)
	}
	for i, c := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		snap := s.db.NewSnapshot()
		m, err := metadataFrom(snap, c.name)
		var plan RelayoutPlan
		if err == nil && m.ID == c.id && m.Continuation {
			plan, err = planner.RelayoutPlan(ctx, s.newContinuationIO(ctx, snap, nil, m))
		} else if errors.Is(err, ErrNotFound) || err == nil {
			err = nil // deleted or replaced since the scan
		}
		snap.Close()
		if err != nil {
			return fmt.Errorf("relayout plan %s: %w", c.name, err)
		}
		if len(plan.Blocks) > 0 {
			group = append(group, relayoutItem{m: m, plan: plan})
			groupBytes += plan.Bytes
		}
		if groupBytes >= relayoutTargetBytes || len(group) >= relayoutMaxMetrics || i == len(candidates)-1 {
			if err := flush(c.id); err != nil {
				return err
			}
		}
	}
	if err := s.setRelayoutCursor(ceiling, true); err != nil {
		return err
	}
	s.relayout.done.Store(true)
	return nil
}

type relayoutCandidate struct {
	id   uint64
	name string
}

// relayoutCandidates lists continuation metrics with IDs in (after, ceiling],
// in ID order. The catalog is keyed by name, so this scans it once.
func (s *Store) relayoutCandidates(ctx context.Context, after, ceiling uint64) ([]relayoutCandidate, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		return nil, err
	}
	var out []relayoutCandidate
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, it.Close())
		}
		name := string(it.Key()[2:])
		m, err := decodeCatalog(name, it.Value())
		if err != nil {
			return nil, errors.Join(fmt.Errorf("decode catalog %s: %w", name, err), it.Close())
		}
		if m.Continuation && m.ID > after && m.ID <= ceiling {
			out = append(out, relayoutCandidate{id: m.ID, name: name})
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b relayoutCandidate) int {
		switch {
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	})
	return out, nil
}

// relayoutGroup ingests the group's copies as one table, then switches each
// block whose descriptor is unchanged and deletes the copies of the rest.
func (s *Store) relayoutGroup(ctx context.Context, items []relayoutItem) error {
	path := s.fs.PathJoin(s.dir, fileIngestDir, fmt.Sprintf("relayout-%020d.sst", items[0].m.ID))
	if err := s.fs.MkdirAll(s.fs.PathDir(path), 0o755); err != nil {
		return err
	}
	f, err := s.fs.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), s.pebbleOptions.MakeWriterOptions(numLevels-1, s.db.FormatMajorVersion().MaxTableFormat()))
	var previous []byte
	for _, item := range items {
		prefix := continuationPrefix(item.m.ID, item.m.Generation)
		for _, b := range item.plan.Blocks {
			for _, c := range b.Copies {
				key := appendContinuationKey(nil, prefix, c.Key)
				if previous != nil && bytes.Compare(previous, key) >= 0 {
					_ = w.Close()
					_ = s.fs.Remove(path)
					return fmt.Errorf("relayout copies out of order at metric %d", item.m.ID)
				}
				if err := w.Set(key, c.Value); err != nil {
					_ = w.Close()
					_ = s.fs.Remove(path)
					return err
				}
				previous = key
			}
		}
	}
	if err := w.Close(); err != nil {
		_ = s.fs.Remove(path)
		return err
	}
	_, err = s.db.IngestWithStats([]string{path})
	_ = s.fs.Remove(path)
	if err != nil {
		return fmt.Errorf("ingest relayout copies: %w", err)
	}
	for _, item := range items {
		if err := s.relayoutApply(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) relayoutApply(ctx context.Context, item relayoutItem) error {
	unlock := s.lockMetricMutation(item.m.Name)
	defer unlock()
	current, err := s.metadataLocked(item.m.Name)
	stale := errors.Is(err, ErrNotFound) || err == nil && (current.ID != item.m.ID || current.Generation != item.m.Generation)
	if err != nil && !stale {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	io := s.newContinuationIO(ctx, s.db, batch, item.m)
	moved := 0
	for _, b := range item.plan.Blocks {
		keep := false
		if !stale {
			value, err := io.Get(b.Descriptor)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			keep = err == nil && bytes.Equal(value, b.Old)
		}
		if keep {
			if err := io.Set(b.Descriptor, b.New); err != nil {
				return err
			}
			for _, key := range b.Removed {
				if err := io.Delete(key); err != nil {
					return err
				}
			}
			moved++
			continue
		}
		// The block changed after planning (or the generation is gone): its
		// new state does not reference these copies.
		for _, c := range b.Copies {
			if err := io.Delete(c.Key); err != nil {
				return err
			}
		}
		s.relayout.skipped.Add(1)
	}
	if err := s.commit(batch); err != nil {
		return err
	}
	if moved > 0 {
		s.relayout.metrics.Add(1)
		s.relayout.blocks.Add(uint64(moved))
	}
	s.relayout.bytes.Add(uint64(item.plan.Bytes))
	return nil
}

func (s *Store) relayoutCursor() (uint64, bool, error) {
	value, closer, err := s.db.Get(relayoutCursorKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(value) != 9 {
		return 0, false, errors.New("invalid relayout cursor")
	}
	return binary.BigEndian.Uint64(value[:8]), value[8] == 1, nil
}

func (s *Store) setRelayoutCursor(id uint64, done bool) error {
	value := binary.BigEndian.AppendUint64(nil, id)
	if done {
		value = append(value, 1)
	} else {
		value = append(value, 0)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(relayoutCursorKey(), value, nil); err != nil {
		return err
	}
	return s.commit(batch)
}
