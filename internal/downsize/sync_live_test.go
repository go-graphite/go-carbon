package downsize

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// TestSyncLiveCarbonserver: v3 info fails a whole request with 404 when one
// name is unknown, so the backend splits; multi-fetch decodes per name.
func TestSyncLiveCarbonserver(t *testing.T) {
	known := map[string]bool{"a": true, "c": true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/info/":
			var req protov3.MultiGlobRequest
			if err := req.UnmarshalVT(body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			var resp protov3.MultiMetricsInfoResponse
			for _, n := range req.Metrics {
				if !known[n] {
					http.Error(w, "Metric not found", 404)
					return
				}
				resp.Metrics = append(resp.Metrics, &protov3.MetricsInfoResponse{Name: n, Retentions: []*protov3.Retention{{SecondsPerPoint: 60, NumberOfPoints: 10}}})
			}
			out, _ := resp.MarshalVT()
			_, _ = w.Write(out)
		case "/render/":
			var req protov3.MultiFetchRequest
			_ = req.UnmarshalVT(body)
			var resp protov3.MultiFetchResponse
			for _, m := range req.Metrics {
				if known[m.Name] {
					resp.Metrics = append(resp.Metrics, &protov3.FetchResponse{Name: m.Name, StartTime: m.StartTime, StepTime: 60, Values: []float64{1, 2}})
				}
			}
			out, _ := resp.MarshalVT()
			_, _ = w.Write(out)
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port := u.Port()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
	b.o.carbonPort = mustAtoi(t, port)
	store := "http://" + u.Hostname() + ":4242"
	info, err := b.Info(context.Background(), store, []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(info) != 2 || info["a"][0].Step != 60 || info["c"] == nil {
		t.Fatalf("info %+v", info)
	}
	got, err := b.Fetch(context.Background(), store, []string{"a", "b"}, 120, 240)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["a"].Step != 60 || len(got["a"].Values) != 2 {
		t.Fatalf("fetch %+v", got)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}

// TestSyncLiveCopyForbiddenProbe: a target refusing the source (403 in the
// copy journal) is reported as forbidden after the one-name probe, without
// running the full list.
func TestSyncLiveCopyForbiddenProbe(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "fill-r1-s0.jsonl")
	if err := os.WriteFile(filepath.Join(dir, "fill-r1-s0.names"), []byte("a\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	calls := 0
	b := newSyncLiveBackend(syncLiveOptions{copyAttempts: 2})
	b.copy = func(_ context.Context, o copyOptions) error {
		calls++
		j, err := openCopyJournal(o.state, "x")
		if err != nil {
			return err
		}
		defer j.close()
		_ = j.failed("a", http.StatusForbidden, "source is neither a ring node nor a configured transfer source")
		return &apiError{Status: 0, Message: "1 names failed"}
	}
	res, err := b.Copy(context.Background(), "http://s:4242", "http://t:4242", []string{"a", "b"}, journal)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Forbidden || calls != 1 {
		t.Fatalf("res %+v after %d engine runs", res, calls)
	}
	// A source that works: names not covered by a done range are failures.
	b.copy = func(_ context.Context, o copyOptions) error {
		j, err := openCopyJournal(o.state, "y"+o.names)
		if err != nil {
			return err
		}
		defer j.close()
		return j.done("a", "a", 1, 10, time.Millisecond)
	}
	_ = os.Remove(journal)
	res, err = b.Copy(context.Background(), "http://s:4242", "http://t:4242", []string{"a", "b"}, journal)
	if err != nil {
		t.Fatal(err)
	}
	if res.Forbidden || res.Copied != 1 || len(res.Failed) != 1 || res.Failed[0] != "b" {
		t.Fatalf("res %+v", res)
	}
}
