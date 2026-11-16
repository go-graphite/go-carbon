package downsize

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Read failures are isolated at the backend boundary. Splitting verifyBatch
// instead would repeat every successful read from all the other stores/windows.
// Only failure reasons survive a read; no point arrays or digests are cached.
type syncReadFailures struct {
	mu    sync.Mutex
	names map[string]string
}
type syncReadFailuresKey struct{}

func splitVerifyRead[T any](ctx context.Context, op, store string, names []string, read func([]string) (map[string]T, error)) (map[string]T, error) {
	failures, ok := ctx.Value(syncReadFailuresKey{}).(*syncReadFailures)
	if !ok {
		return read(names)
	}
	failures.mu.Lock()
	pending := make([]string, 0, len(names))
	for _, n := range names {
		if _, failed := failures.names[n]; !failed {
			pending = append(pending, n)
		}
	}
	failures.mu.Unlock()
	if len(pending) == 0 {
		return map[string]T{}, nil
	}
	got, err := read(pending)
	if err == nil || ctx.Err() != nil || errors.Is(err, errSyncStoreDown) || errors.Is(err, errSyncNoDigest) {
		return got, err
	}
	if len(pending) == 1 {
		reason := fmt.Sprintf("%s %s: %v", store, op, err)
		failures.mu.Lock()
		failures.names[pending[0]] = reason
		failures.mu.Unlock()
		return map[string]T{}, nil
	}
	event("sync_read_split", map[string]any{"operation": op, "store": store, "first": pending[0], "names": len(pending), "error": err.Error()})
	left, err := splitVerifyRead(ctx, op, store, pending[:len(pending)/2], read)
	if err != nil {
		return nil, err
	}
	right, err := splitVerifyRead(ctx, op, store, pending[len(pending)/2:], read)
	if err != nil {
		return nil, err
	}
	for n, v := range right {
		left[n] = v
	}
	return left, nil
}

func (s *syncer) verifyInfo(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	return splitVerifyRead(ctx, "info", store, names, func(ns []string) (map[string][]syncArchive, error) { return s.b.Info(ctx, store, ns) })
}

func finishVerifyReads(ctx context.Context, row *syncRow) {
	failures := ctx.Value(syncReadFailuresKey{}).(*syncReadFailures)
	failures.mu.Lock()
	defer failures.mu.Unlock()
	if len(failures.names) == 0 {
		return
	}
	holes := row.Holes[:0]
	for _, h := range row.Holes {
		if _, failed := failures.names[h.Name]; !failed {
			holes = append(holes, h)
		}
	}
	row.Holes = holes
	for _, items := range []*[]string{&row.Unindexed, &row.Short} {
		kept := (*items)[:0]
		for _, n := range *items {
			if _, failed := failures.names[n]; !failed {
				kept = append(kept, n)
			}
		}
		*items = kept
	}
	for _, items := range []*[]syncSkip{&row.Skipped, &row.Resolved} {
		kept := (*items)[:0]
		for _, x := range *items {
			if _, failed := failures.names[x.Name]; !failed {
				kept = append(kept, x)
			}
		}
		*items = kept
	}
	names := make([]string, 0, len(failures.names))
	for n := range failures.names {
		if n < row.First || n > row.Last {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		row.Skipped = append(row.Skipped, syncSkip{n, failures.names[n]})
		event("sync_name_skipped", map[string]any{"level": "warn", "round": row.Round, "metric": n, "reason": failures.names[n]})
	}
}
