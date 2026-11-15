package downsize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCopyBackpressureResponseReasons(t *testing.T) {
	for _, tc := range []struct{ body, reason, label string }{
		{"too many concurrent transfers\n", "transfer_slots", "transfer slots"},
		{"target storage pressure; import paused\n", "storage_pressure", "storage pressure"},
		{"source storage pressure; deletion paused\n", "storage_pressure", "storage pressure"},
		{"proxy unavailable\n", "unknown", "unclassified"},
		{"proxy says too many concurrent transfers but no rejection proof", "unknown", "unclassified"},
	} {
		t.Run(tc.reason+tc.label+fmt.Sprint(len(tc.body)), func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					http.Error(w, tc.body, 429)
					return
				}
				w.WriteHeader(200)
			}))
			defer server.Close()
			m := &mover{client: server.Client(), options: options{tick: time.Millisecond}}
			if _, err := m.request(t.Context(), server.URL, "/v1/transfer/export", []string{"metric"}, nil); err != nil {
				t.Fatal(err)
			}
			total, p := m.backpressureSnapshot(server.URL)
			if total != 1 || p.total() != 1 || p.reasons()[tc.reason] != 1 || !strings.Contains(p.limiter(), tc.label) {
				t.Fatalf("total%d sample%+v reasons%v limiter%s", total, p, p.reasons(), p.limiter())
			}
			_, other := m.backpressureSnapshot("another-peer")
			if other.total() != 0 {
				t.Fatal("attributed another peer's response to target")
			}
		})
	}
}

func TestCopyBackpressureForwardedSource(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "source export: 429 Too Many Requests", 429)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	m := &mover{client: server.Client(), options: options{tick: time.Millisecond}}
	request := struct {
		Source   string
		Names    []string
		MaxBytes int64
	}{Source: "http://source.invalid", Names: []string{"metric"}}
	if _, err := m.request(context.Background(), server.URL, "/v1/transfer/pull", request, nil); err != nil {
		t.Fatal(err)
	}
	total, target := m.backpressureSnapshot(server.URL)
	_, source := m.backpressureSnapshot(request.Source)
	if total != 1 || target.total() != 0 || source.Unknown != 1 {
		t.Fatalf("total%d target%+v source%+v", total, target, source)
	}
}

func TestCopyBackpressureConcurrentSnapshots(t *testing.T) {
	m := &mover{}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			host := "target"
			if i%2 == 0 {
				host = "source"
			}
			for j := 0; j < 100; j++ {
				m.recordBackpressure(host, &apiError{429, host + " /v1/transfer/export: HTTP 429 too many concurrent transfers"})
				total, target := m.backpressureSnapshot("target")
				if total < target.total() {
					t.Error("inconsistent snapshot")
				}
			}
		}(i)
	}
	wg.Wait()
	total, target := m.backpressureSnapshot("target")
	_, source := m.backpressureSnapshot("source")
	if total != 1600 || target.Slots != 800 || source.Slots != 800 {
		t.Fatalf("total%d target%+v source%+v", total, target, source)
	}
	delta := target.since(copyBackpressure{Slots: 700})
	if delta.total() != 100 {
		t.Fatalf("delta%+v", delta)
	}
}

func TestCopyBackpressureSummaryWire(t *testing.T) {
	m := &mover{}
	m.recordBackpressure("source", &apiError{429, "source /v1/transfer/export: HTTP 429 too many concurrent transfers"})
	m.recordBackpressure("target", &apiError{429, "target /v1/transfer/pull: HTTP 429 too many concurrent transfers"})
	m.recordBackpressure("target", &apiError{429, "target /v1/transfer/pull: HTTP 429 target storage pressure; import paused"})
	c := &planFreeCopier{hostWaiter: &hostWaiter{m: m}, o: copyOptions{to: "target"}, progress: newCopyProgress(), started: time.Now().Add(-time.Second)}
	events, err := captureCopyEvents(t, func() error { c.summary(nil, ""); return nil })
	if err != nil || len(events) != 1 {
		t.Fatalf("events%v err%v", events, err)
	}
	e := events[0]
	if e["http_429s"] != float64(3) || e["target_429s"] != float64(2) || e["source_429s"] != float64(1) {
		t.Fatalf("wrong scope: %+v", e)
	}
	reasons := e["target_429_reasons"].(map[string]any)
	if reasons["transfer_slots"] != float64(1) || reasons["storage_pressure"] != float64(1) {
		t.Fatalf("wrong reasons: %+v", reasons)
	}
}
