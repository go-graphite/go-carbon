package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncCacheGovernor(t *testing.T) {
	var size, down atomic.Int64 // cache points out of a 100-point limit
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() != 0 {
			http.Error(w, "restarting", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"cache":{"size":%d,"limit":100,"estimated_bytes":1,"byte_limit":0}}`, size.Load())
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if ratio, bounded, err := cacheFill(ctx, server.Client(), server.URL); err != nil || !bounded || ratio != 0 {
		t.Fatalf("cacheFill = %v, %v, %v", ratio, bounded, err)
	}

	r := &syncRateLimit{}
	go r.govern(ctx, server.Client(), map[string]string{"target": server.URL}, 0.75, 5*time.Millisecond)
	state := func() (throttled int64, paused bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.throttled, r.paused != nil
	}
	eventually := func(what string, ok func(int64, bool) bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if ok(state()) {
				return
			}
		}
		throttled, paused := state()
		t.Fatalf("%s: throttled=%d paused=%v", what, throttled, paused)
	}

	// Over the threshold: slowed to the floor, but not paused.
	size.Store(80)
	eventually("throttle", func(throttled int64, paused bool) bool { return throttled == syncRateFloor && !paused })
	if reason, paused := r.limiter(); reason == "" || paused {
		t.Fatal("throttled copy reports no limiter")
	}

	// Nearly full: batches block, and stay blocked between threshold and pause.
	size.Store(96)
	eventually("pause", func(_ int64, paused bool) bool { return paused })
	size.Store(80)
	waited := make(chan error, 1)
	go func() { waited <- r.wait(ctx, 1) }()
	select {
	case err := <-waited:
		t.Fatalf("wait returned while paused: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Recovered: the batch proceeds and the rate returns to unlimited.
	size.Store(10)
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	eventually("recover", func(throttled int64, paused bool) bool { return throttled == 0 && !paused })
	if reason, paused := r.limiter(); reason != "" || paused {
		t.Fatalf("recovered copy still limited by %q, paused=%t", reason, paused)
	}

	// Lost telemetry releases a pause but keeps the reduced rate.
	size.Store(96)
	eventually("pause again", func(_ int64, paused bool) bool { return paused })
	down.Store(1)
	eventually("telemetry lost", func(throttled int64, paused bool) bool { return throttled > 0 && !paused })
}

func TestSyncRateRelaxReturnsToCeiling(t *testing.T) {
	r := &syncRateLimit{bytesPerSecond: 8 << 20}
	if got := r.throttle(false, "test"); got != 4<<20 {
		t.Fatalf("throttle from an idle ceiling = %d", got)
	}
	for i := 0; r.relax() != 0; i++ {
		if i > 10 {
			t.Fatal("rate never returned to the ceiling")
		}
	}
}
