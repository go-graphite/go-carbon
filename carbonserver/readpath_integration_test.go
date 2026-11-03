package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// Exercise the real cold packer and render path together, including a narrow
// query inside a multi-group v9 block and cached historical prefixes. The
// uncached render is the byte oracle; no durable data changes on toggle.
func TestReadPathPackedV9RenderOracle(t *testing.T) {
	l, _, _, first, count := newSealedRenderFixture(t)
	l.initializeCaches()
	targets := map[timeRange][]target{{int32(first + count/2), int32(first + count)}: {
		{"sealed.render.m1", "sealed.render.m1"},
		{"sealed.render.m2", "sealed.render.m2"},
	}}
	render := func() []byte {
		t.Helper()
		result, _, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{})
		if err != nil {
			t.Fatal(err)
		}
		if result.metricsFetched != 2 {
			t.Fatalf("rendered %d metrics", result.metricsFetched)
		}
		return result.data
	}
	want := render()
	for _, mode := range []struct{ canonical, prefix bool }{
		{false, false}, {true, false}, {false, true}, {true, true},
	} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			l.SetQueryCacheCanonicalKeys(mode.canonical)
			if err := l.SetQueryRangeCache(mode.prefix, 1, 2*time.Hour); err != nil {
				t.Fatal(err)
			}
			if mode.prefix {
				l.queryRangeCache.Load().now = func() time.Time { return time.Unix(1700100000, 0) }
			}
			for range 2 {
				if !bytes.Equal(want, render()) {
					t.Fatal("render differs from flags-off oracle")
				}
			}
		})
	}
	if atomic.LoadUint64(&l.metrics.RangeCacheHit) == 0 {
		t.Fatal("oracle did not exercise prefix hits")
	}
	// The canonical exact-response namespace also works over packed data.
	l.SetQueryCacheCanonicalKeys(true)
	l.SetQueryCacheEnabled(true)
	for i := range 2 {
		result, hit, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{})
		if err != nil || hit != (i == 1) || !bytes.Equal(want, result.data) {
			t.Fatalf("exact cache oracle hit=%v err=%v", hit, err)
		}
		ts := targets[timeRange{int32(first + count/2), int32(first + count)}]
		ts[0], ts[1] = ts[1], ts[0]
	}
}

// Publish policy while readers build keys and use the old/new prefix namespaces.
func TestReadPathConcurrentPolicyToggle(t *testing.T) {
	l, _, _, first, count := newSealedRenderFixture(t)
	targets := map[timeRange][]target{{int32(first + count/2), int32(first + count)}: {{"sealed.render.m1", "sealed.render.m1"}}}
	// Prepare an immutable warmed namespace before publishing it to readers.
	// The fixture clock is historical, so default time.Now would bypass all
	// prefix reuse and leave the concurrent cache path untested.
	if err := l.SetQueryRangeCache(true, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	warmed := l.queryRangeCache.Load()
	warmed.now = func() time.Time { return time.Unix(1700100000, 0) }
	for range 2 {
		if _, _, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{}); err != nil {
			t.Fatal(err)
		}
	}
	before := atomic.LoadUint64(&l.metrics.RangeCacheHit)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for i := range 20 {
				l.SetQueryCacheCanonicalKeys(i%2 == 0)
				if i%2 == 0 {
					l.queryRangeCache.Store(warmed)
				} else {
					l.queryRangeCache.Store(nil)
				}
				if _, _, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if atomic.LoadUint64(&l.metrics.RangeCacheHit) <= before {
		t.Fatal("concurrent readers did not use a populated prefix namespace")
	}
}
