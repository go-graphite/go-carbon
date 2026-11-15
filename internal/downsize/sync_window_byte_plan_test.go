package downsize

import (
	"bytes"
	"context"
	"fmt"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
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

func TestSyncWindowConflictReceiptKeepsNamedResidual(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
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
	for range 2 {
		result, err := b.CopyWindow(ctx, donor.URL, receiver.URL, names, state, syncRanges{{From: from, Until: from + 10}})
		if err != nil || result.Copied != 2 || len(result.Failed) != 1 || result.Failed[0] != names[1] {
			t.Fatal(result, err)
		}
	}
	// The unresolved ordinal can be recovered through the same receipt; it never
	// triggers recursive range splitting or becomes a copied name on resume.
	if pulls.Load() != 2 {
		t.Fatal("recursive or per-name pulls", pulls.Load())
	}
	files, err := filepath.Glob(state + ".window-*")
	if err != nil {
		t.Fatal(err)
	}
	retained := false
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if bytes.Contains(raw, []byte("window combine conflict")) && bytes.Contains(raw, []byte(names[1])) {
			retained = true
		}
	}
	if !retained {
		t.Fatal("missing named durable residual")
	}
}
