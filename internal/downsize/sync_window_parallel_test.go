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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A delayed native receiver exposes whether ranges overlap, independently of disk speed.
func TestSyncWindowRangeTiming(t *testing.T) {
	source := nativeControllerStore(t)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	var active, peak, pulls atomic.Int64
	var progressedBeforeLastPull atomic.Bool
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/capabilities" {
			_ = json.NewEncoder(w).Encode(map[string]bool{"NativeTimeWindow": true, "NativeExactCombine": true, "NativeDeferredCombine": true})
			return
		}
		if r.URL.Path != "/v1/native/pull" {
			http.NotFound(w, r)
			return
		}
		pulls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		var req struct{ Range store.NativeRange }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		id, hash, err := req.Range.Identity()
		if err != nil {
			t.Error(err)
			return
		}
		count := len(req.Range.Names)
		bits := make([]byte, (count+7)/8)
		for i := range count {
			bits[i/8] |= 1 << uint(i%8)
		}
		receipt := store.NativeReceipt{Identity: id, NamesSHA256: hash, Count: count, CopiedCount: count, Copied: bits, Digest: fmt.Sprintf("%064d", 0)}
		_ = json.NewEncoder(w).Encode(receipt)
	}))
	defer dst.Close()
	names := copyNames("metric.", store.NativeMaxRangeNames*8)
	for _, streams := range []int{1, 4} {
		b := newSyncLiveBackend(syncLiveOptions{copyStreams: streams, hostWait: time.Nanosecond})
		stop := b.beginWindowCopy()
		peak.Store(0)
		pulls.Store(0)
		progressedBeforeLastPull.Store(false)
		started := time.Now()
		ctx := context.WithValue(context.Background(), syncWindowProgressKey{}, func(_ string, n int, replayed bool) {
			if n > 0 && !replayed && pulls.Load() < int64(len(names)/store.NativeMaxRangeNames) {
				progressedBeforeLastPull.Store(true)
			}
		})
		res, err := b.CopyWindow(ctx, donor.URL, dst.URL, names, t.TempDir()+"/copy", syncRanges{{From: testMetricNow - 10, Until: testMetricNow}})
		stop()
		t.Logf("streams=%d elapsed=%s peak=%d names=%d", streams, time.Since(started), peak.Load(), res.Copied)
		if err != nil {
			t.Fatal(err)
		}
		if streams > 1 && (peak.Load() < 2 || peak.Load() > int64(streams) || !progressedBeforeLastPull.Load()) {
			t.Fatal("ranges failed to overlap/report progress", peak.Load(), progressedBeforeLastPull.Load())
		}
	}
}

func TestSyncWindowProgressRetryDeduplicates(t *testing.T) {
	var p syncProgress
	p.begin("fill", 8)
	ctx := syncWindowProgressContext(context.Background(), p.windowCommitted)
	report := ctx.Value(syncWindowProgressKey{}).(func(string, int, bool))
	var workers sync.WaitGroup
	for range 192 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			report("first:0:4", 4, false)
			report("second:0:4", 4, true)
		}()
	}
	workers.Wait()
	if p.copyActive != 8 || p.copyFresh != 4 {
		t.Fatal("retry progress duplicated", p.copyActive, p.copyFresh)
	}
}

// Resume a serial journal with a parallel driver. Native identities, target
// precedence and all float bits must match a clean serial reference.
func TestSyncWindowParallelDifferentialResume(t *testing.T) {
	previousLimit := nativeRangeNames
	nativeRangeNames = 16
	defer func() { nativeRangeNames = previousLimit }()
	for _, status := range []int{0, http.StatusConflict, http.StatusRequestEntityTooLarge} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx := context.Background()
			source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
			names := copyNames("metric.", int(nativeRangeNames)*2+1)
			for _, n := range names {
				addPoints(t, source, n, math.Copysign(0, -1))
			}
			for _, s := range []*store.Store{target, oracle} {
				addPoints(t, s, names[0], 42)
			}
			donor := httptest.NewServer(nativeControllerService(t, source))
			defer donor.Close()
			ref := httptest.NewServer(nativeControllerService(t, oracle))
			defer ref.Close()
			handler := nativeControllerService(t, target)
			var fail atomic.Bool
			fail.Store(true)
			var mu sync.Mutex
			imported := map[string]int{}
			dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/native/pull" {
					raw, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(raw))
					var req struct{ Range store.NativeRange }
					_ = json.Unmarshal(raw, &req)
					if req.Range.Start >= nativeRangeNames && fail.Load() {
						http.Error(w, "interrupted fill", 503)
						return
					}
					reject := status == http.StatusConflict && slices.Contains(req.Range.Names, names[int(nativeRangeNames)+7]) || status == http.StatusRequestEntityTooLarge && len(req.Range.Names) > int(nativeRangeNames)/2
					if reject {
						http.Error(w, "isolate rejected range", status)
						return
					}
					mu.Lock()
					for _, n := range req.Range.Names {
						imported[n]++
					}
					mu.Unlock()
				}
				handler.ServeHTTP(w, r)
			}))
			defer dst.Close()
			windows := syncRanges{{Name: "early", From: testMetricNow - 20, Until: testMetricNow - 10}, {Name: "late", From: testMetricNow - 10, Until: testMetricNow}}
			good := slices.Clone(names)
			if status == http.StatusConflict {
				good = slices.Delete(good, int(nativeRangeNames)+7, int(nativeRangeNames)+8)
			}
			serial := newSyncLiveBackend(syncLiveOptions{copyStreams: 1})
			stop := serial.beginWindowCopy()
			if res, err := serial.serialCopyWindowOracle(ctx, donor.URL, ref.URL, good, t.TempDir()+"/ref", windows); err != nil || res.Copied != len(good) {
				t.Fatal(res, err)
			}
			stop()
			journal := t.TempDir() + "/copy"
			stop = serial.beginWindowCopy()
			if _, err := serial.serialCopyWindowOracle(ctx, donor.URL, dst.URL, names, journal, windows); err == nil {
				t.Fatal("interrupted fill succeeded")
			}
			stop()
			fail.Store(false)
			parallel := newSyncLiveBackend(syncLiveOptions{copyStreams: 3})
			var fresh, replayed atomic.Int64
			progressCtx := context.WithValue(ctx, syncWindowProgressKey{}, func(_ string, n int, replay bool) {
				if replay {
					replayed.Add(int64(n))
				} else {
					fresh.Add(int64(n))
				}
			})
			for run := range 2 {
				fresh.Store(0)
				replayed.Store(0)
				stop = parallel.beginWindowCopy()
				res, err := parallel.CopyWindow(progressCtx, donor.URL, dst.URL, names, journal, windows)
				stop()
				if err != nil || res.Copied != len(good) {
					t.Fatal(res, err)
				}
				if run == 1 && fresh.Load() != 0 {
					t.Fatal("replayed receipts counted fresh", fresh.Load())
				}
				if fresh.Load()+replayed.Load() != int64(len(names)) && status == http.StatusConflict { // unresolved single-name failures do not advance receipt progress
					t.Fatal("progress differs", fresh.Load(), replayed.Load())
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for _, n := range good {
				if imported[n] != len(windows) {
					t.Fatal("duplicate import", n, imported[n])
				}
				got, err := target.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				want, err := oracle.Snapshot(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Archives) != len(want.Archives) {
					t.Fatal("archives differ", n)
				}
				for ai, a := range want.Archives {
					b := got.Archives[ai]
					if len(a.Points) != len(b.Points) {
						t.Fatal("point count differs", n)
					}
					for i, p := range a.Points {
						q := b.Points[i]
						if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							t.Fatal("point bits differ", n)
						}
					}
				}
			}
		})
	}
}

func TestSyncWindowRangeBudgetAndCancellation(t *testing.T) {
	const streams = 192
	entered := make(chan struct{}, streams)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runSyncWindowRanges(ctx, store.NativeMaxRangeNames*streams*2, streams, func(ctx context.Context, start, end int) error {
			if end-start > store.NativeMaxRangeNames {
				t.Error("unbounded range")
			}
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	for range streams {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("did not use full stream count")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not join")
	}
}
