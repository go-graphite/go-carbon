package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// One range that keeps answering a restart-class error made the host wait
// rerun the whole donor pass, and each rerun granted every deferred range a
// fresh native budget, so CopyWindow never returned. Budgets now outlive the
// pass: the run ends with the deferred and failing names in Failed, and the
// clean name bit-identical to an independent copy.
func TestSyncWindowRangeBudgetEndsRetryLoop(t *testing.T) {
	// Not parallel: one name per native range isolates each failure mode.
	defer func(limit int64) { nativeRangeNames = limit }(nativeRangeNames)
	nativeRangeNames = 1
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"a.clean", "b.fenced", "c.timeout"}
	for i, n := range names {
		addPoints(t, source, n, float64(i)+0.5)
	}
	// A target creation fence defers the name in every receipt.
	target.SetTransferPending(func(name string) bool { return name == "b.fenced" })
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	handler := nativeControllerService(t, target)
	var fenced, timeouts atomic.Int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var req struct{ Range store.NativeRange }
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Error(err)
				return
			}
			switch {
			case slices.Contains(req.Range.Names, "c.timeout"):
				timeouts.Add(1)
				http.Error(w, "import timed out", http.StatusGatewayTimeout)
				return
			case slices.Contains(req.Range.Names, "b.fenced"):
				fenced.Add(1)
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer dst.Close()
	reference := httptest.NewServer(nativeControllerService(t, oracle))
	defer reference.Close()
	windows := syncRanges{{From: testMetricNow - 20, Until: testMetricNow}}
	if res, err := newSyncLiveBackend(syncLiveOptions{}).CopyWindow(ctx, donor.URL, reference.URL, names[:1], filepath.Join(t.TempDir(), "oracle"), windows); err != nil || res.Copied != 1 {
		t.Fatal(res, err)
	}

	const attempts = 3
	b := newSyncLiveBackend(syncLiveOptions{copyAttempts: attempts})
	b.retryTick = time.Millisecond
	path := filepath.Join(t.TempDir(), "copy")
	res, err := b.CopyWindow(ctx, donor.URL, dst.URL, names, path, windows)
	if err != nil {
		t.Fatalf("copy did not terminate: %v (fenced pulls %d, timed-out pulls %d)", err, fenced.Load(), timeouts.Load())
	}
	if res.Copied != 1 || !slices.Equal(res.Failed, []string{"b.fenced", "c.timeout"}) {
		t.Fatal("unexpected result", res)
	}
	// Each range spends one budget per process, however often the pass reruns.
	if n := timeouts.Load(); n != attempts {
		t.Fatalf("timed-out range pulled %d times, want %d", n, attempts)
	}
	if n := fenced.Load(); n == 0 || n > attempts {
		t.Fatalf("fenced range pulled %d times, want 1..%d", n, attempts)
	}
	if why := b.windowFailureReason("b.fenced"); !strings.Contains(why, "pending writes") {
		t.Fatalf("fenced reason %q", why)
	}
	if why := b.windowFailureReason("c.timeout"); !strings.HasPrefix(why, windowTransferExhausted+" 3 times") {
		t.Fatalf("timeout reason %q", why)
	}
	if why := b.windowFailureReason("a.clean"); why != "" {
		t.Fatalf("clean name has reason %q", why)
	}

	// A rerun in the same process never transfers an exhausted range again.
	before := fenced.Load() + timeouts.Load()
	again, err := b.CopyWindow(ctx, donor.URL, dst.URL, names, path, windows)
	if err != nil || again.Copied != 1 || !slices.Equal(again.Failed, res.Failed) {
		t.Fatal("rerun", again, err)
	}
	if after := fenced.Load() + timeouts.Load(); after != before {
		t.Fatalf("rerun pulled exhausted ranges %d times", after-before)
	}

	got, err := target.Snapshot(ctx, "a.clean")
	if err != nil {
		t.Fatal(err)
	}
	want, err := oracle.Snapshot(ctx, "a.clean")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives) != len(want.Archives) {
		t.Fatal("archives differ")
	}
	for ai, a := range want.Archives {
		if len(a.Points) != len(got.Archives[ai].Points) || len(a.Points) == 0 {
			t.Fatal("point count differs", len(a.Points), len(got.Archives[ai].Points))
		}
		for i, p := range a.Points {
			q := got.Archives[ai].Points[i]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatal("point bits differ", p, q)
			}
		}
	}
	for _, n := range []string{"b.fenced", "c.timeout"} {
		if _, err := target.Snapshot(ctx, n); err == nil {
			t.Fatal("failed name was created on the target", n)
		}
	}
}

// Store-down passes fail the capability checks before any range runs, so an
// outage never spends range budgets.
func TestWindowBudgetsCountOnlyToLimit(t *testing.T) {
	t.Parallel()
	var w windowBudgets
	key := windowRangeKey("journal", 0, 1)
	for i := 1; i < 3; i++ {
		if why := w.failed(key, 3, io.ErrUnexpectedEOF); why != "" || w.exhausted(key) != "" {
			t.Fatal("exhausted early", i, why)
		}
	}
	if why := w.failed(key, 3, io.ErrUnexpectedEOF); !strings.HasPrefix(why, windowTransferExhausted+" 3 times") {
		t.Fatal(why)
	}
	if why := w.failed(key, 3, io.ErrUnexpectedEOF); why != "" {
		t.Fatal("reported exhaustion twice", why)
	}
	unset := windowRangeKey("journal", 1, 2)
	for i := 1; i < windowRangeRestartLimit; i++ {
		if why := w.failed(unset, 0, io.EOF); why != "" {
			t.Fatal("unset limit exhausted early", i, why)
		}
	}
	if w.failed(unset, 0, io.EOF) == "" {
		t.Fatal("unset limit never exhausted")
	}
	w.exhaust(key, "later reason")
	if !strings.HasPrefix(w.exhausted(key), windowTransferExhausted) {
		t.Fatal("first reason must win", w.exhausted(key))
	}
}
