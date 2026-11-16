package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
)

func TestSyncWindowUnavailableRangeDrainsHealthyWork(t *testing.T) {
	t.Parallel()
	b := newSyncLiveBackend(syncLiveOptions{})
	unavailable := &apiError{503, "stopping"}
	var healthy atomic.Int64
	err := runSyncWindowRanges(t.Context(), 5*int(nativeRangeNames), 1, func(ctx context.Context, start, end int) error {
		// A nested split uses the same outer retry owner. It must release this
		// worker on unavailability rather than wait before the next healthy range.
		if start == 0 {
			calls := 0
			err := b.wait(ctx, "nested copy", "source", func() error { calls++; return unavailable })
			if calls != 1 {
				t.Errorf("nested waiter retained worker: %d attempts", calls)
			}
			return err
		}
		if ctx.Err() != nil {
			t.Error("unavailable sibling canceled healthy work")
		}
		healthy.Add(int64(end - start))
		return nil
	})
	if !errors.Is(err, unavailable) || healthy.Load() != 4*nativeRangeNames {
		t.Fatalf("healthy work stranded: names=%d err=%v", healthy.Load(), err)
	}
}

func TestSyncWindowFatalRangeStillCancels(t *testing.T) {
	t.Parallel()
	fatal := errors.New("receipt identity mismatch")
	calls := 0
	err := runSyncWindowRanges(t.Context(), 5*int(nativeRangeNames), 1, func(context.Context, int, int) error { calls++; return fatal })
	if !errors.Is(err, fatal) || calls != 1 {
		t.Fatal(calls, err)
	}
}

func TestSyncExpirationEvidenceBoundedParallel(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 64)
	release := make(chan struct{})
	var calls, active, peak atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		var req struct {
			Names        []string
			ReceiptsOnly bool
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if len(req.Names) > 128 || !req.ReceiptsOnly {
			t.Error("unbounded or changed request", req)
		}
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		out := map[string]buckyd.ExpirationEvidence{}
		for _, n := range req.Names {
			out[n] = buckyd.ExpirationEvidence{Latest: 17, Policy: n}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 192})
	names := copyNames("expiry.", 128*33+7)
	done := make(chan error, 1)
	go func() {
		got, err := b.ExpirationEvidence(ctx, server.URL, names, true)
		if err == nil {
			if len(got) != len(names) {
				err = fmt.Errorf("lost evidence: %d/%d", len(got), len(names))
			}
			for _, n := range names {
				if got[n].Latest != 17 || got[n].Policy != n {
					err = fmt.Errorf("changed evidence: %s", n)
					break
				}
			}
		}
		done <- err
	}()
	for range 16 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			cancel()
			close(release)
			t.Fatal("evidence requests serialized")
		}
	}
	if peak.Load() != 16 {
		cancel()
		close(release)
		t.Fatal("request bound", peak.Load())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 34 || active.Load() != 0 || peak.Load() > 16 {
		t.Fatal(calls.Load(), active.Load(), peak.Load())
	}
}

func TestSyncExpirationEvidenceUnsupported(t *testing.T) {
	t.Parallel()
	for _, status := range []int{404, 501} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unsupported", status) }))
			defer server.Close()
			b := newSyncLiveBackend(syncLiveOptions{copyStreams: 192})
			got, err := b.ExpirationEvidence(t.Context(), server.URL, copyNames("expiry.", 4096), true)
			if err != nil || len(got) != 0 {
				t.Fatal(got, err)
			}
		})
	}
}
