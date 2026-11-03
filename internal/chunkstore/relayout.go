package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"golang.org/x/sync/errgroup"
)

// Relayout moves the v6 blocks of metrics converted before format v7 into the
// history bucket, so their cold history stops sitting beside hot keys. Metrics
// are grouped in ID order: each group's copies form one table covering only
// the group's ID range in the history bucket, disjoint from other groups and
// from v7 conversions (whose IDs are higher), so it ingests into the bottom
// level. Each block then switches under its metric lock only if its descriptor
// is unchanged; otherwise its copies are deleted. A crash between ingestion and
// the switch leaves copies that the resumed run writes again.
const (
	relayoutTargetBytes = 64 << 20
	relayoutMaxMetrics  = 4096
)

func relayoutCursorKey() []byte { return []byte("R/relayout/cursor") }

// RelayoutStats reports progress of the current process.
type RelayoutStats struct {
	Metrics, Blocks, Skipped, Bytes uint64
	Cursor                          uint64
	// Ceiling is the highest metric ID the run covers; Cursor/Ceiling is its
	// progress through the ID-ordered catalog.
	Ceiling uint64
	Done    bool
	// Rate is the current pacing in bytes per second.
	Rate                                                     int64
	Paused                                                   bool
	PauseReason                                              string
	Pauses                                                   map[string]uint64
	MinFreeBytes, MaxCompactionDebt                          uint64
	ReclaimBytes, UnreclaimedBytes, Reclaims, ReclaimedBytes uint64
	// Error is why a background run stopped, if it did.
	Error string
}

type relayoutState struct {
	guard                                   relayoutGuard
	reclaim                                 relayoutReclaimState
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
		if s.db != nil {
			if err := s.reclaimRelayout(ctx, false, 0); err != nil {
				return err
			}
		}
		rate := s.relayout.rate.Load()
		copiedThrough := s.relayout.bytes.Load()
		copied := copiedThrough - *paced
		ahead := time.Duration(float64(copied)/float64(rate)*float64(time.Second)) - time.Since(*pacedAt)
		if rate <= 0 || ahead <= 0 {
			// Other workers may finish copies while we wait. Only acknowledge
			// the bytes checked above, so none escape the shared budget.
			*paced, *pacedAt = copiedThrough, time.Now()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(ahead, time.Second)):
		}
	}
}

type relayoutItem struct {
	m    Metadata
	plan RelayoutPlan
}

func (s *Store) RelayoutStats() RelayoutStats {
	r := &s.relayout
	stats := RelayoutStats{Metrics: r.metrics.Load(), Blocks: r.blocks.Load(), Skipped: r.skipped.Load(), Bytes: r.bytes.Load(), Cursor: r.cursor.Load(), Ceiling: r.ceiling.Load(), Done: r.done.Load(), Rate: r.rate.Load()}
	r.guard.stats(&stats)
	r.reclaim.stats(&stats)
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
	defer s.relayout.guard.clear()
	s.relayout.rate.Store(bytesPerSecond)
	if !s.timeBuckets {
		return fmt.Errorf("%w: relayout requires time buckets", ErrFormat)
	}
	planner, ok := s.continuation.(ContinuationRelayouter)
	if !ok {
		return fmt.Errorf("%w: continuation codec cannot relayout", ErrFormat)
	}
	if err := s.recoverRelayoutReclaim(ctx); err != nil {
		return err
	}
	ceiling, err := s.CatalogHighWatermark(ctx)
	if err != nil {
		return err
	}
	s.relayout.ceiling.Store(ceiling)
	cursor, done, err := s.relayoutCursor()
	s.relayout.cursor.Store(cursor)
	if err != nil || done {
		s.relayout.done.Store(done)
		return err
	}
	candidates, err := s.relayoutCandidates(ctx, cursor, ceiling)
	if err != nil {
		return err
	}
	paced, pacedAt := s.relayout.bytes.Load(), time.Now()
	var paceMu sync.Mutex
	pace := func(ctx context.Context) error {
		paceMu.Lock()
		defer paceMu.Unlock()
		return s.paceRelayout(ctx, &paced, &pacedAt)
	}
	workers := min(max(1, s.relayoutWorkers), len(candidates))
	if workers == 1 {
		// Preserve the serial grouping, cursor and pacing boundaries, on the
		// caller's goroutine. Parallel scheduling must not change K=1.
		err = s.relayoutRange(ctx, planner, candidates, func(i int) error {
			last := candidates[i-1].id
			if err := s.setRelayoutCursor(last, false); err != nil {
				return err
			}
			s.relayout.cursor.Store(last)
			return nil
		}, pace)
	} else if workers > 1 {
		chunkSize := relayoutChunkSize(len(candidates), workers)
		chunks := (len(candidates) + chunkSize - 1) / chunkSize
		var next atomic.Uint64
		// A chunk is complete only after all its apply batches commit. Later
		// chunks may finish first, but cannot take the cursor across a hole.
		completed := make([]bool, chunks)
		frontier := 0
		var cursorMu sync.Mutex
		complete := func(chunk int) error {
			cursorMu.Lock()
			defer cursorMu.Unlock()
			completed[chunk] = true
			through := frontier
			for through < chunks && completed[through] {
				through++
			}
			if through == frontier {
				return nil
			}
			last := candidates[min(through*chunkSize, len(candidates))-1].id
			if err := s.setRelayoutCursor(last, false); err != nil {
				return err
			}
			frontier = through
			s.relayout.cursor.Store(last)
			return nil
		}
		group, workerCtx := errgroup.WithContext(ctx)
		for range workers {
			group.Go(func() (err error) {
				// A maintenance panic must cancel and join the other workers,
				// rather than crash the store.
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("panic: %v", r)
					}
				}()
				for {
					if err := workerCtx.Err(); err != nil {
						return err
					}
					chunk := int(next.Add(1) - 1)
					if chunk >= chunks {
						return nil
					}
					start, end := chunk*chunkSize, min((chunk+1)*chunkSize, len(candidates))
					publish := func(i int) error {
						if i == end-start {
							return complete(chunk)
						}
						return nil
					}
					if err := s.relayoutRange(workerCtx, planner, candidates[start:end], publish, pace); err != nil {
						return err
					}
				}
			})
		}
		err = group.Wait()
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.reclaimRelayout(ctx, true, 0); err != nil {
		return err
	}
	if err := s.setRelayoutCursor(ceiling, true); err != nil {
		return err
	}
	s.relayout.cursor.Store(ceiling)
	s.relayout.done.Store(true)
	return nil
}

// Aim for 8 chunks per worker, capped at the existing metric group bound.
// This amortizes table ingestion while sharing skew concentrated at low IDs.
// Even when the oldest tenth of IDs holds most history, it is shared across
// multiple workers. Small catalogs use single-candidate chunks. A
// chunk can still flush multiple times at the 64 MiB byte bound; one oversized
// metric remains indivisible. Chunk boundaries add a final group flush.
func relayoutChunkSize(candidates, workers int) int {
	return min(relayoutMaxMetrics, max(1, candidates/(workers*8)))
}

// relayoutRange owns a contiguous chunk of metric IDs. Each flush completes
// all candidates through its exclusive index before reporting progress.
func (s *Store) relayoutRange(ctx context.Context, planner ContinuationRelayouter, candidates []relayoutCandidate, complete func(int) error, pace func(context.Context) error) error {
	var group []relayoutItem
	groupBytes := 0
	flush := func(position int) error {
		if len(group) > 0 {
			if err := s.relayoutGroup(ctx, group); err != nil {
				return err
			}
		}
		group, groupBytes = group[:0], 0
		if err := complete(position); err != nil {
			return err
		}
		return pace(ctx)
	}
	for i, c := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.waitRelayout(ctx); err != nil {
			return err
		}
		m, plan, err := s.planRelayout(ctx, planner, c)
		if err != nil {
			return fmt.Errorf("relayout plan %s: %w", c.name, err)
		}
		if len(plan.Blocks) > 0 {
			group = append(group, relayoutItem{m: m, plan: plan})
			groupBytes += plan.Bytes
		}
		if groupBytes >= relayoutTargetBytes || len(group) >= relayoutMaxMetrics || i == len(candidates)-1 {
			if err := flush(i + 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) planRelayout(ctx context.Context, planner ContinuationRelayouter, c relayoutCandidate) (Metadata, RelayoutPlan, error) {
	snap := s.db.NewSnapshot()
	defer snap.Close() // Also release it if the background worker panics.
	m, err := metadataFrom(snap, c.name)
	var plan RelayoutPlan
	if err == nil && m.ID == c.id && m.Continuation {
		plan, err = planner.RelayoutPlan(ctx, s.newContinuationIO(ctx, snap, nil, m))
	} else if errors.Is(err, ErrNotFound) || err == nil {
		err = nil // deleted or replaced since the scan
	}
	return m, plan, err
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
	g, err := s.beginRelayoutGroup(ctx, items)
	if err != nil {
		return err
	}
	defer g.finish()
	if err := s.ingestRelayoutGroup(ctx, items, g); err != nil {
		return err
	}
	g.finish()
	return s.reclaimRelayout(ctx, false, 0)
}

func (s *Store) ingestRelayoutGroup(ctx context.Context, items []relayoutItem, g *relayoutReclaimGroup) error {
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
		if err := s.relayoutApply(ctx, item, g); err != nil {
			return err
		}
	}
	return nil
}

type relayoutSpan struct{ start, end []byte }

func addRelayoutSpan(spans map[byte]*relayoutSpan, key []byte) {
	span := spans[key[0]]
	if span == nil {
		spans[key[0]] = &relayoutSpan{start: key, end: key}
		return
	}
	if bytes.Compare(key, span.start) < 0 {
		span.start = key
	}
	if bytes.Compare(key, span.end) > 0 {
		span.end = key
	}
}

func (s *Store) relayoutApply(ctx context.Context, item relayoutItem, g *relayoutReclaimGroup) error {
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
				addRelayoutSpan(g.spans, io.key(key))
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
			addRelayoutSpan(g.spans, io.key(c.Key))
			if err := io.Delete(c.Key); err != nil {
				return err
			}
		}
		s.relayout.skipped.Add(1)
	}
	if moved > 0 {
		// Physical moves must fence optimistic readers too. Maintenance does
		// not refresh activity or keep an otherwise idle metric alive.
		if current.Revision == math.MaxUint64 {
			return errors.New("metric revision exhausted")
		}
		current.Revision++
		if err := batch.Set(revisionKey(current), revisionBytes(current), nil); err != nil {
			return err
		}
	}
	// The journal and descriptor/delete mutations share a WAL batch. A
	// cursor committed later cannot outlive the obligation after recovery.
	if err := batch.Set(g.key, encodeRelayoutReclaim(g.bytes+uint64(item.plan.Bytes), g.spans), nil); err != nil {
		return err
	}
	if err := s.commit(batch); err != nil {
		return err
	}
	g.committed(uint64(item.plan.Bytes))
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
		value, closer, err = s.db.Get([]byte("z/relayout"))
	}
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
