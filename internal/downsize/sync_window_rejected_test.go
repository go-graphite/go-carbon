package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowRejectedBatchResume(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusRequestEntityTooLarge, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
			// Eight names still force two recursive split levels and retain
			// overlap, signed zero, a rejected singleton, and clean siblings.
			names := []string{"a.new", "b.overlap", "c.conflict", "d.new"}
			for i := 0; i < 4; i++ {
				names = append(names, fmt.Sprintf("extra.%02d", i))
			}
			for _, n := range names {
				addPoints(t, source, n, math.Copysign(0, -1))
			}
			for _, s := range []*store.Store{target, oracle} {
				addPoints(t, s, "b.overlap", 42)
			}
			donor := httptest.NewServer(nativeControllerService(t, source))
			defer donor.Close()
			var accepted atomic.Int32
			handler := nativeControllerService(t, target)
			dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/pull" {
					raw, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(raw))
					var req struct{ Range store.NativeRange }
					if err := json.Unmarshal(raw, &req); err != nil {
						t.Error(err)
						return
					}
					if len(req.Range.Names) > 2 || status == http.StatusConflict && slices.Contains(req.Range.Names, "c.conflict") {
						http.Error(w, "rejected window range", status)
						return
					}
					accepted.Add(1)
				}
				handler.ServeHTTP(w, r)
			}))
			defer dst.Close()
			reference := httptest.NewServer(nativeControllerService(t, oracle))
			defer reference.Close()
			backend := newSyncLiveBackend(syncLiveOptions{})
			windows := syncRanges{{Name: "first", From: testMetricNow - 10, Until: testMetricNow - 5}, {Name: "second", From: testMetricNow - 5, Until: testMetricNow}}
			good := slices.Clone(names)
			if status == http.StatusConflict {
				good = slices.Delete(good, 2, 3)
			}
			if res, err := backend.CopyWindow(ctx, donor.URL, reference.URL, good, filepath.Join(t.TempDir(), "oracle"), windows); err != nil || res.Copied != len(good) {
				t.Fatal(res, err)
			}
			path := filepath.Join(t.TempDir(), "copy")
			for run := range 2 {
				before := accepted.Load()
				res, err := backend.CopyWindow(ctx, donor.URL, dst.URL, names, path, windows)
				if err != nil || res.Copied != len(good) {
					t.Fatal(res, err)
				}
				wantFailed := []string(nil)
				if status == http.StatusConflict {
					wantFailed = []string{"c.conflict"}
				}
				if !slices.Equal(res.Failed, wantFailed) {
					t.Fatalf("failed %v want %v", res.Failed, wantFailed)
				}
				if run == 1 && accepted.Load() != before {
					t.Fatal("resumed successful child replayed a pull")
				}
			}
			for _, n := range good {
				got, err := target.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				want, err := oracle.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Archives) != len(want.Archives) {
					t.Fatal("archives differ")
				}
				for ai, a := range want.Archives {
					b := got.Archives[ai]
					if len(a.Points) != len(b.Points) {
						t.Fatal("point count differs", n)
					}
					for i, p := range a.Points {
						q := b.Points[i]
						if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							t.Fatal("point bits differ", n, p, q)
						}
					}
				}
			}
		})
	}
}

func TestSyncWindowRejectedBatchDoesNotSplitTransportFailure(t *testing.T) {
	t.Parallel()
	source := nativeControllerStore(t)
	addPoints(t, source, "one", 1)
	addPoints(t, source, "two", 2)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	target := nativeControllerStore(t)
	handler := nativeControllerService(t, target)
	var pulls atomic.Int32
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			pulls.Add(1)
			http.Error(w, "lost acknowledgement", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer dst.Close()
	backend := newSyncLiveBackend(syncLiveOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := backend.CopyWindow(ctx, donor.URL, dst.URL, []string{"one", "two"}, filepath.Join(t.TempDir(), "copy"), syncRanges{{From: testMetricNow - 10, Until: testMetricNow}})
		done <- err
	}()
	for backend.waiting.Load() == 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || pulls.Load() != 1 {
		t.Fatal("ambiguous acknowledgement was split", err, pulls.Load())
	}
}
