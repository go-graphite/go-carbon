package carbonserver

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	pickle "github.com/lomik/og-rek"
	"go.uber.org/zap"
)

func TestRenderRangeCacheResponseFormats(t *testing.T) {
	l, server := newQueryCacheTestServer(t, queryCacheTestMetrics, 3600, renderSampleMixed)
	l.initializeCaches()
	l.SetQueryCacheEnabled(false)
	for _, format := range []string{"json", "carbonapi_v2_pb", "carbonapi_v3_pb", "pickle"} {
		if err := l.SetQueryRangeCache(false, 0, 0); err != nil {
			t.Fatal(err)
		}
		want := queryCacheRequest(t, server, http.MethodGet, format, queryCacheTestMetrics[0], queryCacheTestNow-3600, true)
		if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
			t.Fatal(err)
		}
		l.queryRangeCache.Load().now = func() time.Time { return time.Unix(queryCacheTestNow, 0) }
		for range 2 {
			got := queryCacheRequest(t, server, http.MethodGet, format, queryCacheTestMetrics[0], queryCacheTestNow-3600, true)
			equal := bytes.Equal(got.decoded, want.decoded)
			if format == "pickle" {
				// Pickle dictionaries are emitted from Go maps; their key order
				// can differ even between two uncached renders.
				a, err := pickle.NewDecoder(bytes.NewReader(want.decoded)).Decode()
				if err != nil {
					t.Fatal(err)
				}
				b, err := pickle.NewDecoder(bytes.NewReader(got.decoded)).Decode()
				if err != nil {
					t.Fatal(err)
				}
				equal = reflect.DeepEqual(a, b)
			}
			if got.status != 200 || got.header.Get("Content-Type") != want.header.Get("Content-Type") || !equal {
				t.Fatal("cached response differs for", format)
			}
		}
	}
	if l.metrics.RangeCacheHit != 4 {
		t.Fatalf("format-independent cache hits=%d want 4", l.metrics.RangeCacheHit)
	}
}

func TestRenderRangeCacheInvalidationAndOverlay(t *testing.T) {
	l, db, _, names, stamp := newRangeScreenFixture(t, 1, 8<<20)
	var overlay []points.Point
	l.cacheGet = func(string) []points.Point { return overlay }
	oracle := NewCarbonserverListener(l.cacheGet)
	oracle.logger, oracle.accessLogger = zap.NewNop(), zap.NewNop()
	oracle.SetMetricStore(db)
	if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	c := l.queryRangeCache.Load()
	c.now = func() time.Time { return time.Unix(stamp.Load(), 0) }
	name := names[0]
	check := func(from, until int32, wantHit bool) {
		t.Helper()
		before := l.metrics.RangeCacheHit
		want, e := oracle.fetchSingleMetric(name, "alias", from, until)
		if e != nil {
			t.Fatal(e)
		}
		got, e := l.fetchSingleMetric(name, "alias", from, until)
		if e != nil {
			t.Fatal(e)
		}
		a, e := want.proto3().MarshalVT()
		if e != nil {
			t.Fatal(e)
		}
		b, e := got.proto3().MarshalVT()
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(a, b) {
			t.Fatal("range response differs from full fetch")
		}
		if (l.metrics.RangeCacheHit > before) != wantHit {
			t.Fatalf("hit=%v want=%v", l.metrics.RangeCacheHit > before, wantHit)
		}
	}
	const from, until = int32(rangeScreenNow - 3600), int32(rangeScreenNow)
	check(from, until, false)
	if e := c.get(name); e == nil || int64(e.series.UntilTime) > stamp.Load()-1800 {
		t.Fatal("cached recent data")
	}
	check(from+60, until, true)
	// Cached history is pre-overlay and detached from response arrays. Even a
	// late point still in the live ingestion cache must appear on every hit.
	overlay = []points.Point{{Timestamp: rangeScreenNow - 2700, Value: 888}}
	check(from+60, until, true)
	overlay = nil
	check(from+60, until, true)
	// Every committed revision invalidates the entire prefix, irrespective of
	// whether the write changes the recent tail or historical cached slots.
	for _, age := range []int64{100, 2700} {
		if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: rangeScreenNow - age, Value: 333}}); err != nil {
			t.Fatal(err)
		}
		check(from+60, until, false)
		check(from+60, until, false) // active revisions bypass admission
		stamp.Add(61)
		check(from+60, until, false) // stable series can be admitted again
		check(from+60, until, true)
	}
	snapshot, err := db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	// Flattened imports are deliberately rejected for continuation stores;
	// the failed mutation must leave the cached prefix valid.
	if _, err := db.Fill(context.Background(), snapshot); err == nil {
		t.Fatal("continuation accepted flattened fill")
	}
	if _, err := db.Replace(context.Background(), snapshot); err == nil {
		t.Fatal("continuation accepted flattened replacement")
	}
	check(from+60, until, true)
	// Coarse archives, future ranges and zero-width requests keep full fetch.
	check(rangeScreenNow-86520, rangeScreenNow-86460, false)
	check(until-10, until+60, false)
	check(until-5, until-5, false)
	// A fully historical subrange can be served entirely from cached values.
	check(from+60, rangeScreenNow-2400, true)
	if err := db.Delete(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if _, err := l.fetchSingleMetric(name, "alias", from, until); err == nil {
		t.Fatal("deleted metric returned old prefix")
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 2, Count: 43200}}, AggregationMethod: store.Sum}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: rangeScreenNow - 2400, Value: 123}}); err != nil {
		t.Fatal(err)
	}
	check(from, until, false)
	check(from+60, until, true)
	// Store replacement has a distinct epoch even for identical identities.
	other, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(rangeScreenNow, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 2, Count: 43200}}, AggregationMethod: store.Sum}); err != nil {
		t.Fatal(err)
	}
	l.SetMetricStore(other)
	oracle.SetMetricStore(other)
	check(from, until, false)
}

func TestRenderRangeCacheImportEpoch(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(rangeScreenNow, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const name = "range.import"
	if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 86400}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(ctx, name, []store.Point{{Timestamp: rangeScreenNow - 2700, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	c := &renderRangeCache{entries: make(map[string]*list.Element), limit: 1 << 20, mutable: 30 * time.Minute, now: func() time.Time { return time.Unix(rangeScreenNow, 0) }}
	read := func(wantHit bool) *store.Series {
		t.Helper()
		s, n, e := c.fetch(ctx, db, name, rangeScreenNow-3600, rangeScreenNow)
		if e != nil || (n > 0) != wantHit {
			t.Fatalf("reused=%d wantHit=%v err=%v", n, wantHit, e)
		}
		return s
	}
	read(false)
	read(true)
	snapshot, err := db.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Fill(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	read(false)
	read(true)
	snapshot.Archives[0].Points[0].Value = 99
	if _, err := db.Replace(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	s := read(false)
	if s.Values[(rangeScreenNow-2700-s.FromTime)/s.Step] != 99 {
		t.Fatal("restore returned stale values")
	}
	read(true)
}

func TestRenderRangeCacheConcurrentRevision(t *testing.T) {
	l, db, _, names, stamp := newRangeScreenFixture(t, 1, 8<<20)
	name := names[0]
	if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	l.queryRangeCache.Load().now = func() time.Time { return time.Unix(stamp.Load(), 0) }
	write := func(v float64) error {
		return db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: rangeScreenNow - 2700, Value: v}, {Timestamp: rangeScreenNow - 100, Value: v}})
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 60 {
			if err := write(float64(i + 1)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	defer wg.Wait()
	for range 100 {
		r, err := l.fetchSingleMetric(name, "alias", rangeScreenNow-3600, rangeScreenNow)
		if err != nil {
			t.Fatal(err)
		}
		old := r.Values[(rangeScreenNow-2700-r.StartTime)/r.StepTime]
		recent := r.Values[(rangeScreenNow-100-r.StartTime)/r.StepTime]
		if old != recent {
			t.Fatalf("mixed revisions: prefix=%v tail=%v", old, recent)
		}
	}
}

func TestRenderRangeCacheRetentionBoundary(t *testing.T) {
	l, db, _, names, stamp := newRangeScreenFixture(t, 1, 8<<20)
	if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	c := l.queryRangeCache.Load()
	c.now = func() time.Time { return time.Unix(stamp.Load(), 0) }
	const from, until = rangeScreenNow - 86399, rangeScreenNow
	if _, err := l.fetchSingleMetric(names[0], "alias", from, until); err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.now = func() time.Time {
		calls++
		if calls == 2 {
			stamp.Add(2)
		}
		return time.Unix(stamp.Load(), 0)
	}
	r, err := l.fetchSingleMetric(names[0], "alias", from, until)
	if err != nil {
		t.Fatal(err)
	}
	want, err := db.Fetch(context.Background(), names[0], from, until)
	if err != nil || r.StepTime != 60 || r.StepTime != int64(want.Step) || l.metrics.RangeCacheHit != 0 {
		t.Fatalf("retention crossing used cached fine data: step=%d err=%v", r.StepTime, err)
	}
}

func TestRenderRangeCacheBudgetAndConcurrentReaders(t *testing.T) {
	l, _, _, names, stamp := newRangeScreenFixture(t, 1, 8<<20)
	if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	c := l.queryRangeCache.Load()
	c.now = func() time.Time { return time.Unix(stamp.Load(), 0) }
	// Two prefixes fit; a third evicts the least recently used entry.
	c.limit = 32 << 10
	for _, i := range []int{0, 1, 0, 2} {
		if _, err := l.fetchSingleMetric(names[i], "alias", rangeScreenNow-3600, rangeScreenNow); err != nil {
			t.Fatal(err)
		}
	}
	if c.get(names[1]) != nil || c.get(names[0]) == nil || c.get(names[2]) == nil || c.used > c.limit {
		t.Fatal("LRU/budget failure")
	}
	c.limit = 1 // Oversize results are served without retention.
	if _, err := l.fetchSingleMetric(names[3], "alias", rangeScreenNow-3600, rangeScreenNow); err != nil {
		t.Fatal(err)
	}
	if c.get(names[3]) != nil {
		t.Fatal("retained oversized entry")
	}
	// Reconfigure before concurrent readers; the cache and returned values must
	// not share mutable arrays. Writers mutate only their own returned slice.
	if err := l.SetQueryRangeCache(true, 1, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	c = l.queryRangeCache.Load()
	c.now = func() time.Time { return time.Unix(stamp.Load(), 0) }
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 30 {
				r, err := l.fetchSingleMetric(names[(worker+j)%len(names)], "alias", rangeScreenNow-3600, rangeScreenNow)
				if err != nil {
					t.Error(err)
					return
				}
				if len(r.Values) > 0 {
					r.Values[0] = -123
				}
			}
		}()
	}
	wg.Wait()
	if c.used > c.limit {
		t.Fatal("concurrent fills exceeded budget")
	}
	if err := l.SetQueryRangeCache(false, 0, 0); err != nil || l.queryRangeCache.Load() != nil {
		t.Fatal("disable retained cache", err)
	}
	for _, tc := range []struct {
		size    int
		mutable time.Duration
	}{{0, time.Minute}, {1, 0}, {-1, time.Minute}} {
		if err := l.SetQueryRangeCache(true, tc.size, tc.mutable); err == nil {
			t.Fatal(fmt.Sprint(tc), "accepted")
		}
	}
}
