package downsize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Real exporter/importer receipts reproduce a name omitted by a pending source
// snapshot. Reusing the original pull would return its all-deferred receipt
// forever, even after that source name becomes available.
func TestSyncWindowDeferredResume(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "lost-ack"}[lostAck], func(t *testing.T) {
			ctx := context.Background()
			source, target := nativeControllerStore(t), nativeControllerStore(t)
			names := []string{"deferred", "existing"}
			addPoints(t, source, names[0], 1)
			addPoints(t, source, names[1], 2)
			addPoints(t, target, names[1], 42)
			var pending atomic.Bool
			pending.Store(true)
			source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
				if pending.Load() {
					return map[string]struct{}{names[0]: {}}, nil
				}
				return nil, nil
			})
			donor := httptest.NewServer(nativeControllerService(t, source))
			defer donor.Close()
			handler := nativeControllerService(t, target)
			var fail atomic.Bool
			var deferred atomic.Int64
			survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/pull" && !pending.Load() {
					deferred.Add(1)
					if fail.CompareAndSwap(true, false) {
						rec := httptest.NewRecorder()
						handler.ServeHTTP(rec, r)
						if rec.Code != 200 {
							t.Errorf("pull: %s", rec.Body.String())
						}
						http.Error(w, "lost deferred acknowledgement", 503)
						return
					}
				}
				handler.ServeHTTP(w, r)
			}))
			defer survivor.Close()
			path := filepath.Join(t.TempDir(), "copy")
			windows := syncRanges{{From: testMetricNow - 10, Until: testMetricNow}}
			copy := func() (syncCopyResult, error) {
				return newSyncLiveBackend(syncLiveOptions{}).CopyWindow(ctx, donor.URL, survivor.URL, names, path, windows)
			}
			first, err := copy()
			if err != nil || first.Copied != 1 || len(first.Failed) != 1 || first.Failed[0] != names[0] {
				t.Fatal(first, err)
			}
			pending.Store(false)
			fail.Store(lostAck)
			if lostAck {
				_, _ = copy()
			}
			result, err := copy()
			if err != nil || result.Copied != 2 || len(result.Failed) != 0 {
				t.Fatal(result, err)
			}
			if deferred.Load() != 1 {
				t.Fatal("deferred pull replayed", deferred.Load())
			}
			for i, n := range names {
				snap, err := target.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				value := float64(1)
				if i == 1 {
					value = 42
				}
				found := false
				for _, a := range snap.Archives {
					for _, p := range a.Points {
						if p.Value == value {
							found = true
						}
					}
				}
				if !found {
					t.Fatal("missing source or target-wins point", n, snap)
				}
			}
			// The original receipt remains immutable and the completed selection is
			// durable; a second resume must issue no further pull.
			result, err = copy()
			if err != nil || result.Copied != 2 || deferred.Load() != 1 {
				t.Fatal(result, err, deferred.Load())
			}
		})
	}
}

// A deferred receipt may copy fine points while refusing an incomplete coarse
// boundary. Its completed bitmap must not hide that residual on later resumes.
func TestSyncWindowDeferredBoundaryRemainsResidual(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	base := testMetricNow/60*60 - 120
	ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "boundary", AggregationMethod: store.Sum, Retentions: ret}}, Archives: []store.Archive{
		{Retention: ret[0], Points: []store.Point{{Timestamp: base, Value: 1}}},
		{Retention: ret[1], Points: []store.Point{{Timestamp: base, Value: 2}}},
	}}
	if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	var pending atomic.Bool
	pending.Store(true)
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		if pending.Load() {
			return map[string]struct{}{"boundary": {}}, nil
		}
		return nil, nil
	})
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	survivor := httptest.NewServer(nativeControllerService(t, target))
	defer survivor.Close()
	path := filepath.Join(t.TempDir(), "copy")
	for range 3 {
		res, err := newSyncLiveBackend(syncLiveOptions{}).CopyWindow(ctx, donor.URL, survivor.URL, []string{"boundary"}, path, syncRanges{{From: base, Until: base + 20}})
		if err != nil || res.Copied != 0 || len(res.Failed) != 1 || res.Failed[0] != "boundary" {
			t.Fatal("lost deferred boundary residual", res, err)
		}
		pending.Store(false)
	}
}
