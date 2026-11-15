package downsize

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowNameBytesPlanDifferentialResume(t *testing.T) {
	ctx := context.Background()
	source, target, reference := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	names := make([]string, 260)
	for i := range names {
		names[i] = fmt.Sprintf("metric.%04d.", i) + strings.Repeat("x", store.MaxMetricNameBytes-len("metric.0000."))
		addPoints(t, source, names[i], math.SmallestNonzeroFloat64)
		if i%3 == 0 {
			addPoints(t, target, names[i], math.Copysign(0, -1))
			addPoints(t, reference, names[i], math.Copysign(0, -1))
		}
	}
	var exports, pulls atomic.Int64
	wrap := func(h http.Handler, counter *atomic.Int64, path string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == path {
				counter.Add(1)
			}
			h.ServeHTTP(w, r)
		})
	}
	donor := httptest.NewServer(wrap(nativeControllerService(t, source), &exports, "/v1/native/export"))
	defer donor.Close()
	receiver := httptest.NewServer(wrap(nativeControllerService(t, target), &pulls, "/v1/native/pull"))
	defer receiver.Close()
	windows := syncRanges{{From: testMetricNow - 20, Until: testMetricNow}}
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 2})
	stop := b.beginWindowCopy()
	defer stop()
	state := filepath.Join(t.TempDir(), "copy")
	for range 2 {
		res, err := b.CopyWindow(ctx, donor.URL, receiver.URL, names, state, windows)
		if err != nil || len(res.Failed) > 0 || res.Copied != len(names) {
			t.Fatal(res, err)
		}
	}
	if pulls.Load() != 2 {
		t.Fatal("oversized request or completed child repeated", pulls.Load())
	}
	before := exports.Load()
	if _, err := b.WindowPresence(ctx, donor.URL, names, windows[0]); err != nil {
		t.Fatal(err)
	}
	if exports.Load()-before != 2 {
		t.Fatal("selection did not plan known name-byte bound", exports.Load()-before)
	}
	// The established per-name path is the differential oracle. Use an independent
	// journal and target, then compare every persisted timestamp and float bit.
	ref := httptest.NewServer(nativeControllerService(t, reference))
	defer ref.Close()
	for i, n := range names {
		if _, err := b.CopyWindow(ctx, donor.URL, ref.URL, []string{n}, fmt.Sprintf("%s.ref-%d", state, i), windows); err != nil {
			t.Fatal(err)
		}
		want, err := reference.Snapshot(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Snapshot(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(n, err)
		}
	}
}

func TestSyncWindowFutureSlotRecoveredOnResume(t *testing.T) {
	ctx := context.Background()
	// Keep future-slot classification independent of the rest of the suite's
	// runtime: its seeded package timestamp may be many minutes old by now.
	open := func() *store.Store {
		s, err := store.Open("/db", store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(testMetricNow, 0) }}.WithFS(vfs.NewMem()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	}
	source, target := open(), open()
	names := []string{"a.healthy", "b.conflict", "c.healthy"}
	from := testMetricNow - 20
	for _, name := range names {
		ret := store.Retention{Step: 10, Count: 10}
		cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{ret}, AggregationMethod: store.Sum}
		if _, err := source.CreateFromSnapshot(ctx, store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: ret, Points: []store.Point{{Timestamp: from, Value: math.Copysign(0, -1)}}}}}); err != nil {
			t.Fatal(err)
		}
		if name == names[1] {
			if _, err := target.CreateFromSnapshot(ctx, store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: ret, Points: []store.Point{{Timestamp: from + 100, Value: 42}}}}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	var pulls atomic.Int64
	h := nativeControllerService(t, target)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			pulls.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	defer receiver.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 1})
	stop := b.beginWindowCopy()
	defer stop()
	state := filepath.Join(t.TempDir(), "copy")
	// A future owner of the same circular slot cannot hide retained history.
	// The server already reclaims that slot; the client must credit the receipt
	// and leave no residual or repeated pull when the same journal is resumed.
	for range 2 {
		result, err := b.CopyWindow(ctx, donor.URL, receiver.URL, names, state, syncRanges{{From: from, Until: from + 10}})
		if err != nil || result.Copied != len(names) || len(result.Failed) != 0 {
			t.Fatal(result, err)
		}
		got, err := target.Snapshot(ctx, names[1])
		if err != nil || len(got.Archives) != 1 || len(got.Archives[0].Points) != 1 {
			t.Fatal(got, err)
		}
		point := got.Archives[0].Points[0]
		if point.Timestamp != from || math.Float64bits(point.Value) != math.Float64bits(math.Copysign(0, -1)) {
			t.Fatal("retained point was not restored bit-exactly", point)
		}
	}
	if pulls.Load() != 1 {
		t.Fatal("journal replay repeated the pull", pulls.Load())
	}
}
