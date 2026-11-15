package downsize

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Each sync batch owns a short copy invocation. Its transport must release
// sockets on both successful completion and preflight failure.
func TestCopyBatchClosesConnections(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "preflight-error"}[denied], func(t *testing.T) {
			var open atomic.Int64
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/transfer/status":
					if denied {
						http.Error(w, "forbidden", http.StatusForbidden)
						return
					}
					json.NewEncoder(w).Encode(copyStatus{MaxTransfers: 8, MaxBodyBytes: 1 << 20})
				case "/v1/transfer/list":
					json.NewEncoder(w).Encode([]string{})
				default:
					http.NotFound(w, r)
				}
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					open.Add(1)
				}
				if state == http.StateClosed || state == http.StateHijacked {
					open.Add(-1)
				}
			}
			srv.Start()
			defer srv.Close()
			target := httptest.NewUnstartedServer(srv.Config.Handler)
			target.Config.ConnState = srv.Config.ConnState
			target.Start()
			defer target.Close()
			for range 5 {
				o := copyTestOptions(t, srv.URL, target.URL)
				err := planFreeCopy(context.Background(), o)
				if (err != nil) != denied {
					t.Fatalf("denied=%v err=%v", denied, err)
				}
			}
			deadline := time.Now().Add(time.Second)
			for open.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if n := open.Load(); n != 0 {
				t.Fatalf("completed batches retained %d connections", n)
			}
		})
	}
}

func TestProgressRateUsesFreshWorkAndExpiresStalls(t *testing.T) {
	now := time.Unix(1000, 0)
	var rate progressRate
	p := progressSnapshot{Phase: "copy", Done: 1000000, FreshDoneAvailable: true, FreshDone: 0}
	if got := rate.observe(now, p); got != 0 {
		t.Fatal(got)
	}
	// Replaying a million names and discovering more names is not copy work.
	p.Done += 1000000
	p.Discovered = 3000000
	p.Verified = 2000000
	if got := rate.observe(now.Add(time.Minute), p); got != 0 {
		t.Fatal(got)
	}
	p.Done += 600
	p.FreshDone = 600
	if got := rate.observe(now.Add(2*time.Minute), p); got != 5 {
		t.Fatal(got)
	}
	// The old lifetime average used to advertise throughput indefinitely.
	for minute := 3; minute <= 7; minute++ {
		rate.observe(now.Add(time.Duration(minute)*time.Minute), p)
	}
	if got := rate.observe(now.Add(8*time.Minute), p); got != 0 {
		t.Fatal(got)
	}
	p.Phase = "verify round 2"
	p.FreshDone = 50000
	if got := rate.observe(now.Add(9*time.Minute), p); got != 0 {
		t.Fatal("phase rate leaked", got)
	}
	p.FreshDone = 0
	if got := rate.observe(now.Add(10*time.Minute), p); got != 0 {
		t.Fatal("counter reset", got)
	}
}

func TestProgressMetricsIncludesReadyRecoveryOnly(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "progress.json")
	p := &progressMetrics{paths: map[string]bool{path: true}}
	cases := []struct {
		path string
		want bool
	}{
		{path, true},
		{filepath.Join(root, "ready-recovery-123-0", "progress.json"), true},
		{filepath.Join(root, "ready-fill-123", "progress.json"), true},
		{filepath.Join(root, "copy", "progress.json"), false},
		{filepath.Join(root, "ready-recovery-123-0", "batch", "progress.json"), false},
		{filepath.Join(root, "sibling", "ready-fill-123", "progress.json"), false},
	}
	for _, tc := range cases {
		if got := p.accepts(tc.path); got != tc.want {
			t.Fatalf("%s: %v", tc.path, got)
		}
	}
	p.observe(path, progressSnapshot{Phase: "verify round 1", Status: "running", Done: 100})
	p.samples = []progressMetricSample{{at: time.Now(), done: 100}}
	p.observe(cases[1].path, progressSnapshot{Phase: "verify round 1", Status: "running", Done: 200})
	if p.done != 200 || len(p.samples) != 0 {
		t.Fatalf("recovery hidden or mixed with parent: %+v", p)
	}
}
