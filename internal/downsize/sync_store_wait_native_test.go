package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowUnavailableSplitDoesNotBlockSiblings(t *testing.T) {
	t.Parallel()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"a.unavailable", "b.healthy", "c.healthy", "d.healthy"}
	for _, name := range names {
		addPoints(t, source, name, math.Copysign(0, -1))
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	h := nativeControllerService(t, target)
	var recovered atomic.Bool
	var mu sync.Mutex
	imports := map[string]int{}
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var req struct{ Range store.NativeRange }
			_ = json.Unmarshal(raw, &req)
			if len(req.Range.Names) > 1 {
				http.Error(w, "split range", 413)
				return
			}
			name := req.Range.Names[0]
			if name == names[0] && !recovered.Load() {
				http.Error(w, "store temporarily unavailable", 503)
				return
			}
			mu.Lock()
			imports[name]++
			mu.Unlock()
			if name == names[len(names)-1] {
				recovered.Store(true)
			}
		}
		h.ServeHTTP(w, r)
	}))
	defer receiver.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 1})
	b.retryTick = time.Microsecond
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "copy")
	windows := syncRanges{{From: testMetricNow - 20, Until: testMetricNow}}
	for range 2 {
		result, err := b.CopyWindow(ctx, donor.URL, receiver.URL, names, path, windows)
		if err != nil || result.Copied != len(names) {
			t.Fatal("unavailable left split stranded healthy right split", result, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range names {
		if imports[name] != 1 {
			t.Fatalf("completed name imported %d times: %s", imports[name], name)
		}
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := source.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Archives) != len(want.Archives) {
			t.Fatal("archive count", name)
		}
		for ai, a := range want.Archives {
			points := got.Archives[ai].Points
			if len(points) != len(a.Points) {
				t.Fatal("point count", name)
			}
			for i, p := range a.Points {
				if p.Timestamp != points[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(points[i].Value) {
					t.Fatal(fmt.Sprintf("point bits changed: %s", name))
				}
			}
		}
	}
}

func TestSyncWindowUnavailableWindowDoesNotBlockOtherWindow(t *testing.T) {
	t.Parallel()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	addPoints(t, source, "window.metric", math.Copysign(0, -1))
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	windows := syncRanges{{From: testMetricNow - 20, Until: testMetricNow - 10}, {From: testMetricNow - 10, Until: testMetricNow}}
	var recovered atomic.Bool
	var first, second atomic.Int64
	h := nativeControllerService(t, target)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var req struct{ Range store.NativeRange }
			_ = json.Unmarshal(raw, &req)
			if req.Range.Window.From == windows[0].From {
				if !recovered.Load() {
					http.Error(w, "first window unavailable", 503)
					return
				}
				first.Add(1)
			} else {
				second.Add(1)
				recovered.Store(true)
			}
		}
		h.ServeHTTP(w, r)
	}))
	defer receiver.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 1})
	b.retryTick = time.Microsecond
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "copy")
	for range 2 {
		result, err := b.CopyWindow(ctx, donor.URL, receiver.URL, []string{"window.metric"}, path, windows)
		if err != nil || result.Copied != 1 {
			t.Fatal("first window stranded second", result, err)
		}
	}
	if first.Load() != 1 || second.Load() != 1 {
		t.Fatal("completed window imported again", first.Load(), second.Load())
	}
	snapshot, err := target.Snapshot(ctx, "window.metric")
	if err != nil {
		t.Fatal(err)
	}
	points := snapshot.Archives[0].Points
	if len(points) != 2 || points[0].Timestamp != windows[0].From || points[1].Timestamp != windows[1].From || math.Float64bits(points[1].Value) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal("point bits or timestamps changed", points)
	}
}
