package downsize

import (
	"context"
	"sync"
)

// syncDigestMemo keeps the serving-archive digests one verification batch has
// already received. Absence probes digest stale names over each archive's
// serving window, and full-history verification then asks the same store for
// the same window. Both requests return the same bytes, so the second one is
// answered from this batch's memo instead of reading the window again.
// The memo never outlives the batch, whose mutation proofs bracket every read.
type syncDigestMemo struct {
	mu  sync.Mutex
	got map[syncDigestMemoKey]map[string][]syncDigest // nil slice: requested, not found
}

type syncDigestMemoKey struct {
	store       string
	from, until int64
}

type syncDigestMemoContextKey struct{}

func withSyncDigestMemo(ctx context.Context) context.Context {
	return context.WithValue(ctx, syncDigestMemoContextKey{}, &syncDigestMemo{got: map[syncDigestMemoKey]map[string][]syncDigest{}})
}

func syncDigestMemoFrom(ctx context.Context) *syncDigestMemo {
	m, _ := ctx.Value(syncDigestMemoContextKey{}).(*syncDigestMemo)
	return m
}

// split returns a fresh result map holding the names already answered and the
// names that still need a backend request, in their original order.
func (m *syncDigestMemo) split(store string, w syncRange, names []string) (map[string][]syncDigest, []string) {
	out := map[string][]syncDigest{}
	if m == nil {
		return out, names
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	known := m.got[syncDigestMemoKey{store, w.From, w.Until}]
	if len(known) == 0 {
		return out, names
	}
	pending := make([]string, 0, len(names))
	for _, n := range names {
		d, ok := known[n]
		switch {
		case !ok:
			pending = append(pending, n)
		case d != nil:
			out[n] = d
		}
	}
	return out, pending
}

// remember records a successful response. Names whose read failed stay out of
// the memo, so they keep the existing failure handling.
func (m *syncDigestMemo) remember(ctx context.Context, store string, w syncRange, names []string, got map[string][]syncDigest) {
	if m == nil {
		return
	}
	failures, _ := ctx.Value(syncReadFailuresKey{}).(*syncReadFailures)
	if failures != nil {
		failures.mu.Lock()
		defer failures.mu.Unlock()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := syncDigestMemoKey{store, w.From, w.Until}
	known := m.got[key]
	if known == nil {
		known = make(map[string][]syncDigest, len(names))
		m.got[key] = known
	}
	for _, n := range names {
		if d, ok := got[n]; ok {
			if d == nil {
				d = []syncDigest{}
			}
			known[n] = d
			continue
		}
		if failures != nil {
			if _, failed := failures.names[n]; failed {
				continue
			}
		}
		known[n] = nil
	}
}
