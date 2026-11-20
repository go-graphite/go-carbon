package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestSyncRestartWaitNeverExpiresAndRealErrorsFail(t *testing.T) {
	t.Parallel()
	for _, failure := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, io.EOF, io.ErrUnexpectedEOF, &apiError{503, "stopping"}, &apiError{500, "application lifecycle is busy; retry native copy"}, &apiError{500, "store recovery has not finished"}, &syncHTTPError{500, "not ready"}} {
		b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})
		b.retryTick = time.Microsecond
		calls := 0
		err := b.wait(context.Background(), "test", "donor", func() error {
			calls++
			if calls <= 8 {
				return failure
			}
			return nil
		})
		if err != nil || calls != 9 || b.waiting.Load() != 0 {
			t.Fatal(failure, calls, err)
		}
	}
	for _, failure := range []error{&apiError{403, "forbidden"}, &apiError{500, "corrupt storage"}, errors.New("bad checksum")} {
		b := newSyncLiveBackend(syncLiveOptions{})
		calls := 0
		if err := b.wait(context.Background(), "test", "donor", func() error { calls++; return failure }); !errors.Is(err, failure) || calls != 1 {
			t.Fatal(calls, err)
		}
	}
}

// Real exporters/importers exercise lost ACK receipts and expired donor pins.
// Each operation must finish within one invocation and match an uninterrupted
// run, including signed zero and existing target values.
func TestSyncRestartWindowDifferential(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"capabilities", "source-pin", "selection-stream", "pull-before", "pull-lost-ack", "expired-pin", "handover", "source-export-503"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			source, target, oracle := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
			names := []string{"a.new", "b.existing", "c.new"}
			for _, n := range names {
				addPoints(t, source, n, math.Copysign(0, -1))
			}
			addPoints(t, target, "b.existing", 42)
			addPoints(t, oracle, "b.existing", 42)
			var injected atomic.Bool
			var handedOver atomic.Bool
			var copying atomic.Bool
			var unavailable atomic.Int64
			cut := func(w http.ResponseWriter) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}
			donorHandler := nativeControllerService(t, source)
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handedOver.Load() {
					http.Error(w, "buckyd is stopping", 503)
					return
				}
				if stage == "capabilities" && r.URL.Path == "/v1/native/capabilities" && injected.CompareAndSwap(false, true) {
					http.Error(w, "buckyd is stopping", 503)
					return
				}
				if stage == "source-pin" && r.URL.Path == "/v1/native/source" && injected.CompareAndSwap(false, true) {
					http.Error(w, "store recovery has not finished", 500)
					return
				}
				if r.URL.Path == "/v1/native/export" {
					if stage == "source-export-503" && copying.Load() && unavailable.Add(1) <= 3 {
						injected.Store(true)
						http.Error(w, "buckyd is stopping; retry after restart", 503)
						return
					}
					if stage == "expired-pin" && injected.CompareAndSwap(false, true) {
						http.Error(w, "native source session expired or daemon restarted", 410)
						return
					}
					if stage == "selection-stream" && injected.CompareAndSwap(false, true) {
						rec := httptest.NewRecorder()
						donorHandler.ServeHTTP(rec, r)
						if rec.Code != 200 {
							t.Error(rec.Body.String())
						}
						for k, vs := range rec.Header() {
							for _, v := range vs {
								w.Header().Add(k, v)
							}
						}
						w.WriteHeader(200)
						_, _ = w.Write(rec.Body.Bytes()[:rec.Body.Len()/2])
						w.(http.Flusher).Flush()
						cut(w)
						return
					}
				}
				donorHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			targetHandler := nativeControllerService(t, target)
			dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handedOver.Load() {
					http.Error(w, "buckyd is stopping", 503)
					return
				}
				if r.URL.Path == "/v1/native/pull" && (stage == "pull-before" || stage == "pull-lost-ack") && injected.CompareAndSwap(false, true) {
					if stage == "pull-before" {
						http.Error(w, "not ready", 503)
						return
					}
					rec := httptest.NewRecorder()
					targetHandler.ServeHTTP(rec, r)
					if rec.Code != 200 {
						t.Error(rec.Body.String())
					}
					cut(w)
					return
				}
				targetHandler.ServeHTTP(w, r)
			}))
			defer dst.Close()
			ref := httptest.NewServer(nativeControllerService(t, oracle))
			defer ref.Close()
			b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})
			b.retryTick = time.Microsecond
			stopPins := b.beginWindowCopy()
			defer stopPins()
			windows := syncRanges{{From: testMetricNow - 10, Until: testMetricNow}}
			if _, err := b.WindowPresenceRanges(ctx, donor.URL, names, windows); err != nil {
				t.Fatal(err)
			}
			if stage == "handover" {
				// Keep both old processes and their pooled sockets alive while
				// replacing the listeners. Fresh services have no old pins.
				handedOver.Store(true)
				replaceStoreServer(t, donor, nativeControllerService(t, source))
				replaceStoreServer(t, dst, nativeControllerService(t, target))
				injected.Store(true)
			}
			copying.Store(true)
			journal := filepath.Join(t.TempDir(), "copy")
			if res, err := b.CopyWindow(ctx, donor.URL, dst.URL, names, journal, windows); err != nil || res.Copied != len(names) {
				t.Fatal(res, err)
			}
			if !injected.Load() {
				t.Fatal("restart injection missed")
			}
			if res, err := b.CopyWindow(ctx, donor.URL, ref.URL, names, filepath.Join(t.TempDir(), "oracle"), windows); err != nil || res.Copied != len(names) {
				t.Fatal(res, err)
			}
			for _, n := range names {
				got, e := target.Snapshot(ctx, n)
				want, f := oracle.Snapshot(ctx, n)
				if e != nil || f != nil {
					t.Fatal(e, f)
				}
				if len(got.Archives) != len(want.Archives) {
					t.Fatal("archive count")
				}
				for ai, a := range want.Archives {
					z := got.Archives[ai]
					if len(a.Points) != len(z.Points) {
						t.Fatal("point count")
					}
					for i, p := range a.Points {
						q := z.Points[i]
						if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							t.Fatal("point bits", n, p, q)
						}
					}
				}
			}
			// Same durable journal must not add names or change data on replay.
			before, _ := target.Snapshot(ctx, names[0])
			if _, err := b.CopyWindow(ctx, donor.URL, dst.URL, names, journal, windows); err != nil {
				t.Fatal(err)
			}
			after, _ := target.Snapshot(ctx, names[0])
			if !reflect.DeepEqual(before, after) {
				t.Fatal("journal replay changed data")
			}
		})
	}
}

// The complete orchestration retries verification and fill in the same run.
// A committed-but-unacknowledged fill is idempotent; journal replay retains the
// same final names, timestamps and value bits as an uninterrupted sync.
type restartingSyncBackend struct {
	*fakeSync
	retry       *syncLiveBackend
	verifyCalls atomic.Int64
	fillCalls   atomic.Int64
}

func (b *restartingSyncBackend) Digest(ctx context.Context, host string, names []string, from, until int64) (out map[string][]syncDigest, err error) {
	err = b.retry.wait(ctx, "verify", host, func() error {
		if b.verifyCalls.Add(1) == 2 {
			return &apiError{500, "store recovery has not finished"}
		}
		var e error
		out, e = b.fakeSync.Digest(ctx, host, names, from, until)
		return e
	})
	return
}
func (b *restartingSyncBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (out syncCopyResult, err error) {
	err = b.retry.wait(ctx, "fill", target, func() error {
		var e error
		out, e = b.fakeSync.Copy(ctx, source, target, names, journal)
		if e == nil && b.fillCalls.Add(1) == 1 {
			return io.EOF
		}
		return e
	})
	return
}
func TestSyncRestartVerifyFillConvergesWithoutExit(t *testing.T) {
	t.Parallel()
	var oracle map[string]map[int64]map[int64]uint64
	for _, restart := range []bool{false, true} {
		f := newFakeSync()
		o := convergeOptions(t)
		o.from = []string{syncD1}
		o.batch = 3
		o.streams = 1
		for i := 0; i < 12; i++ {
			n := fmt.Sprintf("restart.%02d", i)
			for _, h := range []string{syncD1, syncT} {
				f.store(h).archives[n] = []syncArchive{{Step: 3600, Points: 100}}
				f.store(h).points[n] = map[int64]map[int64]float64{3600: {}}
			}
			f.store(syncD1).points[n][3600][syncTestNow-7200] = math.Float64frombits(0x3ff0000000000000 + uint64(i))
			f.store(syncT).points[n][3600][syncTestNow-10800] = math.Copysign(0, -1)
		}
		var backend syncBackend = f
		r := &restartingSyncBackend{fakeSync: f, retry: newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})}
		r.retry.retryTick = time.Microsecond
		if restart {
			backend = r
		}
		if err := runSync(context.Background(), o, backend); err != nil {
			t.Fatal("unexpected exit", err)
		}
		if restart && (r.verifyCalls.Load() < 3 || r.fillCalls.Load() < 2) {
			t.Fatal("restart stages not exercised")
		}
		got := map[string]map[int64]map[int64]uint64{}
		for n, steps := range f.store(syncT).points {
			got[n] = map[int64]map[int64]uint64{}
			for step, points := range steps {
				got[n][step] = map[int64]uint64{}
				for ts, v := range points {
					got[n][step][ts] = math.Float64bits(v)
				}
			}
		}
		if !restart {
			oracle = got
		} else if !reflect.DeepEqual(got, oracle) {
			t.Fatal("restart changed names, timestamps or bits")
		}
		before := len(f.copies)
		if err := runSync(context.Background(), o, backend); err != nil {
			t.Fatal(err)
		}
		if len(f.copies) != before {
			t.Fatal("completed journal replay duplicated names")
		}
	}
}

// Closing a listener does not evict existing keepalive connections. Model the
// old daemon draining there while the replacement serves the same address.
func TestSyncWindowRestartSameAddress(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	source := nativeControllerStore(t)
	addPoints(t, source, "restart.metric", 7)
	handler := nativeControllerService(t, source)
	var stopping atomic.Bool
	var stoppedCalls, freshCalls atomic.Int64
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stopping.Load() {
			stoppedCalls.Add(1)
			http.Error(w, "buckyd is stopping", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer old.Close()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})
	defer b.client.CloseIdleConnections()
	defer b.m.client.CloseIdleConnections()
	b.retryTick = time.Millisecond
	stop := b.beginWindowCopy()
	defer stop()
	names := []string{"restart.metric"}
	windows := syncRanges{{From: testMetricNow - 10, Until: testMetricNow}}
	want, err := b.WindowPresenceRanges(ctx, old.URL, names, windows)
	if err != nil {
		t.Fatal(err)
	}
	addr := old.Listener.Addr().String()
	stopping.Store(true)
	if err := old.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	replacement := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		freshCalls.Add(1)
		handler.ServeHTTP(w, r)
	}))
	replacement.Listener = listener
	replacement.Start()
	defer replacement.Close()
	started := time.Now()
	got, err := b.WindowPresenceRanges(ctx, old.URL, names, windows)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, want, err)
	}
	if stoppedCalls.Load() == 0 || freshCalls.Load() == 0 {
		t.Fatal("restart paths not exercised", stoppedCalls.Load(), freshCalls.Load(), b.waits.Load())
	}
	if time.Since(started) >= 2*time.Minute {
		t.Fatal("recovery exceeded two minutes")
	}
}

func TestSyncWindowRefusedThenServing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	source := nativeControllerStore(t)
	addPoints(t, source, "restart.metric", 7)
	server := httptest.NewUnstartedServer(nativeControllerService(t, source))
	addr := server.Listener.Addr().String()
	if err := server.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})
	defer b.client.CloseIdleConnections()
	defer b.m.client.CloseIdleConnections()
	b.retryTick = time.Millisecond
	done := make(chan error, 1)
	go func() {
		got, err := b.WindowPresenceRanges(ctx, "http://"+addr, []string{"restart.metric"}, syncRanges{{From: testMetricNow - 10, Until: testMetricNow}})
		if err == nil && len(got["restart.metric"]) == 0 {
			err = errors.New("selection missing after restart")
		}
		done <- err
	}()
	for b.waits.Load() == 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	server.Listener = listener
	server.Start()
	defer server.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func replaceStoreServer(t *testing.T, old *httptest.Server, handler http.Handler) {
	t.Helper()
	addr := old.Listener.Addr().String()
	if err := old.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	next := httptest.NewUnstartedServer(handler)
	next.Listener = ln
	next.Start()
	t.Cleanup(next.Close)
}

// Digest may retry inside a window-copy operation that still borrows a source
// pin. Expiration must never wait for that caller's own read lock.
func TestSyncNestedRestartMarksPinsWithoutBlocking(t *testing.T) {
	t.Parallel()
	b := newSyncLiveBackend(syncLiveOptions{})
	b.retryTick = time.Millisecond
	b.windowMu.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		calls := 0
		done <- b.wait(ctx, "digest", "target", func() error {
			calls++
			if calls == 1 {
				return &apiError{503, "store recovery has not finished"}
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		b.windowMu.RUnlock()
		if err != nil || !b.windowRestarted.Load() {
			t.Fatal("restart not deferred", err)
		}
	case <-ctx.Done():
		b.windowMu.RUnlock()
		<-done
		t.Fatal("restart waited for its caller's borrowed pin")
	}
}
