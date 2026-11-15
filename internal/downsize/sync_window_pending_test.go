package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncWindowRetriesDeferredWithinCall(t *testing.T) {
	for _, mode := range []string{"target", "source", "lost-ack", "stuck", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			source, target := nativeControllerStore(t), nativeControllerStore(t)
			names := []string{"a.pending", "b.complete"}
			for _, n := range names {
				addPoints(t, source, n, 1)
			}
			var pending atomic.Bool
			pending.Store(true)
			if mode == "source" {
				source.SetNativePendingCheck(func(n string) bool { return n == names[0] && pending.Load() })
			} else {
				target.SetTransferPending(func(n string) bool { return n == names[0] && pending.Load() })
			}
			donor := httptest.NewServer(nativeControllerService(t, source))
			defer donor.Close()
			handler := nativeControllerService(t, target)
			var pulls atomic.Int32
			dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/native/pull" {
					handler.ServeHTTP(w, r)
					return
				}
				n := pulls.Add(1)
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(raw))
				var req struct{ Range store.NativeRange }
				if err := json.Unmarshal(raw, &req); err != nil {
					t.Error(err)
				}
				if n > 1 && (req.Range.DeferredFrom == "" || !bytes.Equal(req.Range.Selection, []byte{1})) {
					t.Errorf("retry includes completed ordinal or lacks chain: %+v", req.Range)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, r)
				if n == 1 && mode != "stuck" && mode != "deadline" {
					pending.Store(false)
					if mode == "source" {
						// The borrowed scan pin now has a stale revision. Retrying
						// through that same pin would omit this name forever.
						if err := source.UpdateMany(ctx, names[0], []store.Point{{Timestamp: testMetricNow - 2, Value: 7}}); err != nil {
							t.Error(err)
						}
					} else {
						addPoints(t, target, names[0], 42)
					}
				}
				if n == 2 && mode == "lost-ack" {
					http.Error(w, "lost durable acknowledgement", 503)
					return
				}
				for k, v := range rec.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
			}))
			defer dst.Close()
			b := newSyncLiveBackend(syncLiveOptions{copyAttempts: 3})
			b.retryTick = time.Microsecond
			if mode == "deadline" {
				b.o.hostWait, b.retryTick = 50*time.Millisecond, time.Second
			}
			finish := b.beginWindowCopy()
			defer finish()
			path := filepath.Join(t.TempDir(), "copy")
			windows := syncRanges{{From: testMetricNow - 10, Until: testMetricNow}}
			var result syncCopyResult
			events, err := captureCopyEvents(t, func() error {
				var err error
				result, err = b.CopyWindow(ctx, donor.URL, dst.URL, names, path, windows)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "stuck" || mode == "deadline" {
				wantPulls := int32(3)
				if mode == "deadline" {
					wantPulls = 1
				}
				if result.Copied != 1 || len(result.Failed) != 1 || result.Failed[0] != names[0] || pulls.Load() != wantPulls {
					t.Fatal("pending name lost or retries not bounded", result, pulls.Load())
				}
				loud := false
				for _, e := range events {
					loud = loud || e["event"] == "sync_window_deferred" && e["level"] == "ERROR" && e["metric"] == names[0] && e["reason"] == "target catalog absent with pending writes" && e["exhausted"] == true
				}
				if !loud {
					t.Fatal("pending exhaustion lost its name/reason", events)
				}
				return
			}
			if result.Copied != 2 || len(result.Failed) != 0 || pulls.Load() != 2 {
				t.Fatal("did not resolve deferral within call", result, pulls.Load())
			}
			snap, err := target.Snapshot(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			want := float64(42)
			if mode == "source" {
				want = 7
			}
			found := false
			for _, a := range snap.Archives {
				for _, p := range a.Points {
					found = found || p.Value == want
				}
			}
			if !found {
				t.Fatal("missing refreshed source or target-wins value", snap)
			}
			if result, err = b.CopyWindow(ctx, donor.URL, dst.URL, names, path, windows); err != nil || result.Copied != 2 || pulls.Load() != 2 {
				t.Fatal("completed receipt replayed", result, err, pulls.Load())
			}
		})
	}
}
