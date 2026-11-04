package chunkstore

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Two stock parallel Compacts can use the shared Pebble worker budget without
// queuing an entire journal. Group up to 48 bottom-level target files, capped
// at 3 GiB per call: 24 files at the usual 128 MiB SST target, or about 86
// spans for 256 GiB of overlapping SST bytes.
// Two calls admit at most 6 GiB of estimated inputs: 4.6-9.2s at 1.4-0.7 GB/s.
// Scale down with the configured bottom-level target for smaller stores/tests.
const manualCompactionMaxBytes = 3 << 30

// Share admission across reclaim, compact-once, purge and explicit Compact.
// Otherwise two maintenance callers could each queue two full-sized calls.
type manualCompactionState struct {
	mu      sync.Mutex
	active  int
	changed chan struct{}
}

func (s *Store) withManualCompactionSlot(ctx context.Context, compact func() error) error {
	m := &s.manualCompaction
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		if m.active < min(2, max(1, int(s.compactionWorkers.Load()))) {
			m.active++
			m.mu.Unlock()
			break
		}
		if m.changed == nil {
			m.changed = make(chan struct{})
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	defer func() {
		m.mu.Lock()
		m.active--
		if m.changed != nil {
			close(m.changed)
			m.changed = nil
		}
		m.mu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	return compact()
}

type manualCompactionSpan struct {
	start, end []byte
	bytes      uint64
}

// File endpoints, sorted independently, let a range estimate count the full
// bytes of EVERY overlapping file in every level in O(log N), including broad
// L0 files. Table properties/IO are unnecessary. An indivisible bottom file or
// broad upper file may exceed the target; live writes and LSM changes can also
// enlarge an entered Compact. This is an admission bound, not an IO deadline.
type compactionFileEdge struct {
	key   []byte
	bytes uint64 // cumulative bytes after sorting
}

func planManualCompaction(tables [][]pebble.SSTableInfo, start, end []byte, target uint64) []manualCompactionSpan {
	var first, last []compactionFileEdge
	var bounds [][]byte
	for level := len(tables) - 1; level >= 0; level-- {
		for _, table := range tables[level] {
			first = append(first, compactionFileEdge{table.Smallest.UserKey, table.Size})
			last = append(last, compactionFileEdge{table.Largest.UserKey, table.Size})
		}
		if len(bounds) == 0 && len(tables[level]) != 0 {
			for _, table := range tables[level] {
				key := append(bytes.Clone(table.Largest.UserKey), 0)
				if bytes.Compare(key, start) > 0 && bytes.Compare(key, end) < 0 {
					bounds = append(bounds, key)
				}
			}
			// Even if this level has no interior boundary, don't split on
			// upper levels and repeatedly rewrite its single broad file.
			bounds = append(bounds, bytes.Clone(end))
		}
	}
	if len(bounds) == 0 {
		bounds = append(bounds, bytes.Clone(end))
	}
	for _, edges := range [][]compactionFileEdge{first, last} {
		slices.SortFunc(edges, func(a, b compactionFileEdge) int { return bytes.Compare(a.key, b.key) })
		for i := 1; i < len(edges); i++ {
			edges[i].bytes += edges[i-1].bytes
		}
	}
	prefix := func(edges []compactionFileEdge, key []byte, inclusive bool) uint64 {
		i := sort.Search(len(edges), func(i int) bool {
			c := bytes.Compare(edges[i].key, key)
			return c > 0 || !inclusive && c == 0
		})
		if i == 0 {
			return 0
		}
		return edges[i-1].bytes
	}
	estimate := func(lo, hi []byte) uint64 { return prefix(first, hi, true) - prefix(last, lo, false) }
	slices.SortFunc(bounds, bytes.Compare)
	bounds = slices.CompactFunc(bounds, bytes.Equal)
	var spans []manualCompactionSpan
	lo, previous := bytes.Clone(start), start
	for _, hi := range bounds {
		if bytes.Compare(lo, previous) < 0 && estimate(lo, hi) > target && estimate(lo, hi) > estimate(lo, previous) {
			spans = append(spans, manualCompactionSpan{lo, previous, estimate(lo, previous)})
			lo = previous
		}
		previous = hi
	}
	if bytes.Compare(lo, end) < 0 {
		spans = append(spans, manualCompactionSpan{lo, bytes.Clone(end), estimate(lo, end)})
	}
	return spans
}

func (s *Store) compactSpans(ctx context.Context, start, end []byte, before func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tables, err := s.db.SSTables(pebble.WithKeyRangeFilter(start, end))
	if err != nil {
		return err
	}
	target := uint64(manualCompactionMaxBytes)
	if s.pebbleOptions != nil {
		target = min(target, uint64(s.pebbleOptions.Level(numLevels-1).TargetFileSize)*48)
	}
	spans := planManualCompaction(tables, start, end, target)
	parallel := min(2, max(1, int(s.compactionWorkers.Load())))
	return runManualCompaction(ctx, spans, parallel, nil, func(span manualCompactionSpan) error {
		return s.withManualCompactionSlot(ctx, func() error {
			if before != nil {
				if err := before(); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			// Inclusive bounds share only the key immediately after an SST's last
			// key, preserving coverage without rewriting that bottom file twice.
			return s.db.Compact(span.start, span.end, true)
		})
	})
}

// One coordinator admits work only after a slot is free, then checks policy and
// cancellation again. On cancel/error, join only already admitted calls. Every
// Compact retains Pebble's internal parallelism and shared output pacer. Coarse
// byte groups also bound forced memtable flushes to O(spans), rather than O(SSTs).
func runManualCompaction(ctx context.Context, spans []manualCompactionSpan, parallel int, before func() error, compact func(manualCompactionSpan) error) error {
	// Most small reclaim batches fit one group. Keep their existing synchronous
	// scheduling (and avoid a worker/channel allocation) on this common path.
	if len(spans) == 1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if before != nil {
			if err := before(); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err := compact(spans[0])
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	type completion struct {
		err        error
		panicValue any
	}
	done := make(chan completion, parallel)
	inflight := 0
	var result error
	var panicValue any
	join := func() {
		completed := <-done
		inflight--
		if result == nil {
			result = completed.err
		}
		if panicValue == nil {
			panicValue = completed.panicValue
		}
	}
	for _, span := range spans {
		if inflight == parallel {
			join()
		}
		if result != nil || panicValue != nil || ctx.Err() != nil {
			break
		}
		if before != nil {
			if result = before(); result != nil {
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		inflight++
		go func() {
			var completed completion
			defer func() {
				completed.panicValue = recover()
				done <- completed
			}()
			completed.err = compact(span)
		}()
	}
	for inflight > 0 {
		join()
	}
	if panicValue != nil {
		panic(panicValue) // retain the caller's maintenance panic handling
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return result
}
