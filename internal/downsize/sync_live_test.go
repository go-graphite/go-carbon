package downsize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// TestSyncLiveCarbonserver: v3 info fails a whole request with 404 when one
// name is unknown; fallback requests each name exactly once.
func TestSyncLiveCarbonserver(t *testing.T) {
	known := map[string]bool{"a": true, "c": true}
	var requests [][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/info/":
			var req protov3.MultiGlobRequest
			if err := req.UnmarshalVT(body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			requests = append(requests, req.Metrics)
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
	if want := [][]string{{"a", "b", "c", "d"}, {"a"}, {"b"}, {"c"}, {"d"}}; !reflect.DeepEqual(requests, want) {
		t.Fatalf("info requests %v, want %v", requests, want)
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
		_ = j.failed("http://s:4242", "a", http.StatusForbidden, "source is neither a ring node nor a configured transfer source")
		return &apiError{Status: 0, Message: "source probe rejected"}
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
		return j.done("http://s:4242", "a", "a", 1, 10, time.Millisecond)
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

// A long-name page can be truncated by the shared decoder byte budget even
// when the server returns HTTP 200. Retry the cursor and remember the cap.
func TestSyncListTruncatedPage(t *testing.T) {
	var limits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limits = append(limits, r.URL.Query().Get("limit"))
		if r.URL.Query().Get("after") != "cursor" {
			t.Errorf("cursor changed: %s", r.URL.RawQuery)
		}
		if r.URL.Query().Get("limit") == "10000" {
			_, _ = io.WriteString(w, `["truncated`)
			return
		}
		_, _ = io.WriteString(w, `["next"]`)
	}))
	defer srv.Close()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
	for range 2 {
		names, err := b.List(context.Background(), srv.URL, "", "cursor", 10000)
		if err != nil || len(names) != 1 || names[0] != "next" {
			t.Fatalf("names=%v err=%v", names, err)
		}
	}
	if len(limits) != 3 || limits[0] != "10000" || limits[1] != "5000" || limits[2] != "5000" {
		t.Fatalf("limits=%v", limits)
	}
}

func TestSyncCopyTransferControls(t *testing.T) {
	b := newSyncLiveBackend(syncLiveOptions{copyBytes: 16 << 20, copyIdle: 80 * time.Second})
	dir := t.TempDir()
	journal := filepath.Join(dir, "fill-r1-s0.jsonl")
	if err := os.WriteFile(filepath.Join(dir, "fill-r1-s0.names"), []byte("a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	b.o.copyAttempts = 1
	b.copy = func(_ context.Context, o copyOptions) error {
		calls++
		if o.maxBytes != 16<<20 || o.transferIdle != 80*time.Second {
			t.Fatalf("controls not forwarded: %+v", o)
		}
		j, err := openCopyJournal(o.state, o.names)
		if err != nil {
			return err
		}
		defer j.close()
		return j.done("http://s:4242", "a", "a", 1, 10, time.Millisecond)
	}
	res, err := b.Copy(context.Background(), "http://s:4242", "http://t:4242", []string{"a"}, journal)
	if err != nil || res.Copied != 1 || calls != 2 {
		t.Fatalf("res=%+v calls=%d err=%v", res, calls, err)
	}
}

// Exercise the real engine so new required engine options cannot silently
// turn every sync probe and fill into validation retries.
func TestSyncCopyRealEngine(t *testing.T) {
	_, src, dst := newFakeCopy(t, []string{"a", "b"})
	b := newSyncLiveBackend(syncLiveOptions{copyBytes: 16 << 20, copyIdle: 80 * time.Second, copyAttempts: 1, copyStreams: 1})
	dir := t.TempDir()
	journal := filepath.Join(dir, "fill-r1-s0.jsonl")
	if err := os.WriteFile(filepath.Join(dir, "fill-r1-s0.names"), []byte("a\nb\n"), 0600); err != nil {
		t.Fatal(err)
	}
	res, err := b.Copy(context.Background(), src, dst, []string{"a", "b"}, journal)
	if err != nil || res.Copied != 2 || len(res.Failed) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestSyncLiveCopyFinalSkipsAreNotEngineAborts(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "copy.jsonl")
	if err := os.WriteFile(filepath.Join(dir, "copy.names"), []byte("a\nb\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b := newSyncLiveBackend(syncLiveOptions{copyAttempts: 6})
	calls := 0
	b.copy = func(_ context.Context, o copyOptions) error {
		calls++
		if calls == 1 {
			return nil
		} // source probe
		j, err := openCopyJournal(o.state, "test")
		if err != nil {
			return err
		}
		defer j.close()
		if err := j.done("http://s:4242", "a", "a", 1, 10, time.Millisecond); err != nil {
			return err
		}
		if err := j.failed("http://s:4242", "b", http.StatusNotFound, "source export: not found"); err != nil {
			return err
		}
		return &copySkippedError{count: 1, path: o.state + ".skipped"}
	}
	res, err := b.Copy(context.Background(), "http://s:4242", "http://t:4242", []string{"a", "b"}, journal)
	if err != nil || calls != 2 || res.Copied != 1 || len(res.Failed) != 1 || res.Failed[0] != "b" {
		t.Fatalf("calls=%d result=%+v err=%v", calls, res, err)
	}
}

func TestSyncIOTimings(t *testing.T) {
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
	if err := b.wait(context.Background(), "fetch", "source", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := b.wait(context.Background(), "digest", "target", func() error { return &syncHTTPError{status: 404} }); err == nil {
		t.Fatal(err)
	}
	got := b.syncTimings()
	if len(got) != 2 || got[0].Operation != "digest" || got[0].Store != "target" || got[1].Operation != "fetch" || got[1].Store != "source" {
		t.Fatalf("timings %+v", got)
	}
	for _, g := range got {
		if g.Calls != 1 || g.Seconds <= 0 {
			t.Fatalf("timing %+v", g)
		}
	}
}

// Oversized metadata requests retry at the same cursor, and future calls to
// that store reuse the cap. A different store retains its own request size.
func TestSyncInfoAdaptiveBatch(t *testing.T) {
	for _, failure := range []error{io.EOF, io.ErrUnexpectedEOF, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			var requests [][]string
			b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second, carbonPort: 8080})
			b.client.Transport = syncInfoRoundTrip(func(r *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(r.Body)
				var req protov3.MultiGlobRequest
				if err := req.UnmarshalVT(body); err != nil {
					t.Fatal(err)
				}
				requests = append(requests, append([]string(nil), req.Metrics...))
				if r.URL.Hostname() == "slow" && len(req.Metrics) > 2 {
					return nil, failure
				}
				var reply protov3.MultiMetricsInfoResponse
				for _, name := range req.Metrics {
					if name == "absent" {
						return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("Metric not found")), Header: make(http.Header)}, nil
					}
					reply.Metrics = append(reply.Metrics, &protov3.MetricsInfoResponse{Name: name, Retentions: []*protov3.Retention{{SecondsPerPoint: 60, NumberOfPoints: 10000}}})
				}
				data, _ := reply.MarshalVT()
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data)), Header: make(http.Header)}, nil
			})
			names := []string{"a", "b", "c", "d", "e", "f", "g"}
			for range 2 {
				got, err := b.Info(context.Background(), "http://slow:4242", names)
				if err != nil || len(got) != len(names) {
					t.Fatalf("info=%v err=%v", got, err)
				}
				for _, name := range names {
					if len(got[name]) != 1 {
						t.Fatalf("lost %q", name)
					}
				}
			}
			// 7 -> 3 -> 1 (floor-halving), then retain 1 for all future calls.
			if !reflect.DeepEqual(requests[0], names) || !reflect.DeepEqual(requests[1], names[:3]) || !reflect.DeepEqual(requests[2], names[:1]) {
				t.Fatalf("cursor moved: %v", requests)
			}
			for _, req := range requests[2:] {
				if len(req) != 1 {
					t.Fatalf("forgot cap: %v", requests)
				}
			}
			if _, err := b.Info(context.Background(), "http://healthy:4242", names); err != nil {
				t.Fatal(err)
			}
			if len(requests[len(requests)-1]) != 7 {
				t.Fatal("cap leaked to other store")
			}
			got, err := b.Info(context.Background(), "http://slow:4242", []string{"a", "absent", "b"})
			if err != nil || len(got) != 2 || got["a"] == nil || got["b"] == nil {
				t.Fatalf("absence changed: %v %v", got, err)
			}
		})
	}
}

type syncInfoRoundTrip func(*http.Request) (*http.Response, error)

func (f syncInfoRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSyncInfoAdaptiveSingleAndCancel(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		b := newSyncLiveBackend(syncLiveOptions{hostWait: -time.Second, carbonPort: 8080})
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		b.client.Transport = syncInfoRoundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			if canceled {
				cancel()
			}
			return nil, io.EOF
		})
		names := []string{"a"}
		if canceled {
			names = []string{"a", "b", "c"}
		}
		got, err := b.Info(ctx, "http://source:4242", names)
		cancel()
		if err == nil || len(got) != 0 || calls != 1 {
			t.Fatalf("got=%v err=%v calls=%d", got, err, calls)
		}
		if !canceled && !errors.Is(err, errSyncStoreDown) {
			t.Fatalf("lost host wait error: %v", err)
		}
		limit, _ := b.infoLimits.Load("http://source:4242")
		if limit.(*atomic.Int64).Load() != 0 {
			t.Fatal("reduced single-name request")
		}
	}
}
