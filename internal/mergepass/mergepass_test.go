package mergepass

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/buckyd"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

var testNow = time.Now().Unix() / 10 * 10

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open("/db", store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(testNow, 0) }}.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func serve(t *testing.T, s *store.Store, wrap func(http.Handler) http.Handler) string {
	t.Helper()
	service, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), MaxTransfers: 8, MaxBodyBytes: 1 << 20, TransferSources: []string{"127.0.0.1"}}, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop() })
	var h http.Handler = service.Handler()
	if wrap != nil {
		h = wrap(h)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server.URL
}

// put creates name with one 10 s archive; ages are seconds before testNow.
func put(t *testing.T, s *store.Store, name string, step int, points map[int64]float64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: step, Count: 128}}, AggregationMethod: store.Sum}); err != nil {
		t.Fatal(err)
	}
	var in []store.Point
	for age, v := range points {
		in = append(in, store.Point{Timestamp: testNow - age, Value: v})
	}
	if err := s.UpdateMany(ctx, name, in); err != nil {
		t.Fatal(err)
	}
}

// points returns age -> float64 bits, so comparisons are bit-exact.
func points(t *testing.T, s *store.Store, name string) map[int64]uint64 {
	t.Helper()
	snap, err := s.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	out := map[int64]uint64{}
	for _, a := range snap.Archives {
		for _, p := range a.Points {
			if !math.IsNaN(p.Value) {
				out[testNow-p.Timestamp] = math.Float64bits(p.Value)
			}
		}
	}
	return out
}

func bits(m map[int64]float64) map[int64]uint64 {
	out := map[int64]uint64{}
	for k, v := range m {
		out[k] = math.Float64bits(v)
	}
	return out
}

func equalPoints(t *testing.T, name string, got, want map[int64]uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v want %v", name, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s at age %d: got %v want %v (all %v)", name, k, math.Float64frombits(got[k]), math.Float64frombits(v), got)
		}
	}
}

func options(t *testing.T, to string, from ...string) Options {
	return Options{To: to, From: from, State: t.TempDir(), Streams: 2, Batch: 2, Sample: 10, Backoff: time.Millisecond, Retries: 2, WaitOut: 2 * time.Second, Log: io.Discard}
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return strings.Fields(strings.TrimSpace(string(data)))
}

func TestMergeTargetWinsAndSkipsFailingName(t *testing.T) {
	for _, noDigest := range []bool{false, true} {
		t.Run(fmt.Sprintf("noDigest=%v", noDigest), func(t *testing.T) {
			target, donor1, donor2 := newStore(t), newStore(t), newStore(t)
			put(t, target, "a", 10, map[int64]float64{30: 1, 20: 2})
			put(t, target, "c", 10, map[int64]float64{20: 5})
			put(t, target, "d", 10, map[int64]float64{30: 7, 20: 8})
			put(t, target, "e", 10, map[int64]float64{20: 9})
			put(t, donor1, "a", 10, map[int64]float64{40: 100, 30: 999})
			put(t, donor1, "b", 10, map[int64]float64{30: 3})
			put(t, donor1, "d", 10, map[int64]float64{30: 7, 20: 8})
			put(t, donor2, "a", 10, map[int64]float64{50: 50, 40: 4444})
			put(t, donor2, "e", 60, map[int64]float64{60: 1}) // other retention: 409
			before := map[string]map[int64]uint64{"a": points(t, target, "a"), "c": points(t, target, "c"), "d": points(t, target, "d"), "e": points(t, target, "e")}
			o := options(t, serve(t, target, nil), serve(t, donor1, nil), serve(t, donor2, nil))
			o.NoDigest = noDigest
			if err := os.WriteFile(filepath.Join(o.State, "streams"), []byte("3\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			r, err := Run(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			// Existing target points are unchanged; donor-only points are added;
			// between donors the earlier one wins (age 40 keeps donor1's 100).
			equalPoints(t, "a", points(t, target, "a"), bits(map[int64]float64{50: 50, 40: 100, 30: 1, 20: 2}))
			equalPoints(t, "b", points(t, target, "b"), bits(map[int64]float64{30: 3}))
			for _, name := range []string{"c", "d", "e"} {
				equalPoints(t, name, points(t, target, name), before[name])
			}
			if r.Names != 4 || r.Done != 3 || r.SkippedNames != 1 || len(r.Skipped) != 1 || r.Skipped[0].Name != "e" || !strings.Contains(r.Skipped[0].Reason, "409") {
				t.Fatalf("report %+v", r)
			}
			if !noDigest && (r.Identical != 1 || r.Pulled != 3) {
				t.Fatalf("digest gate: identical=%d pulled=%d", r.Identical, r.Pulled)
			}
			if noDigest && r.Identical != 0 {
				t.Fatalf("no-digest identical=%d", r.Identical)
			}
			if r.Streams != 3 {
				t.Fatalf("streams file ignored: %d", r.Streams)
			}
			if got := lines(t, filepath.Join(o.State, "done.log")); len(got) != 3 {
				t.Fatalf("done.log %v", got)
			}
			if data, _ := os.ReadFile(filepath.Join(o.State, "errors.log")); !bytes.HasPrefix(data, []byte("e\t"+o.From[1]+"\t")) {
				t.Fatalf("errors.log %q", data)
			}
			p1 := r.Parity[o.From[0]]
			if r.ParitySample != 4 || p1.Equal != 2 || p1.Differ != 1 || p1.MissingTarget != 0 {
				t.Fatalf("parity %+v sample %d", p1, r.ParitySample)
			}
			if _, err := os.Stat(filepath.Join(o.State, "report.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMergeKillAndResumeSkipsDoneNames(t *testing.T) {
	target, donor := newStore(t), newStore(t)
	for i := range 6 {
		put(t, donor, fmt.Sprintf("n%d", i), 10, map[int64]float64{20: float64(i)})
	}
	o := options(t, serve(t, target, nil), serve(t, donor, nil))
	o.Streams, o.Batch = 1, 1
	ctx, cancel := context.WithCancel(context.Background())
	o.afterBatch = func(done int64) {
		if done >= 2 {
			cancel()
		}
	}
	if _, err := Run(ctx, o); err == nil {
		t.Fatal("interrupted run returned no error")
	}
	first := lines(t, filepath.Join(o.State, "done.log"))
	if len(first) < 2 || len(first) >= 6 {
		t.Fatalf("done.log after kill: %v", first)
	}
	o.afterBatch = nil
	r, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Resumed != int64(len(first)) || r.Done != 6-r.Resumed || r.Pulled != 6-r.Resumed {
		t.Fatalf("resume report %+v (first %d)", r, len(first))
	}
	for i := range 6 {
		equalPoints(t, "n", points(t, target, fmt.Sprintf("n%d", i)), bits(map[int64]float64{20: float64(i)}))
	}
}

func TestMergeRetriesTransientAndFailedBatches(t *testing.T) {
	target, donor := newStore(t), newStore(t)
	for _, name := range []string{"ok.1", "flaky", "ok.2"} {
		put(t, donor, name, 10, map[int64]float64{20: 1})
	}
	var unavailable, flaky atomic.Int32
	wrap := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/transfer/pull" {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
				if unavailable.Add(1) <= 2 {
					http.Error(w, "restarting", 503) // a daemon restart: waited out
					return
				}
				if bytes.Contains(body, []byte("flaky")) && flaky.Add(1) <= 2 {
					http.Error(w, "boom", 500) // retried, then split, then retried
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	o := options(t, serve(t, target, wrap), serve(t, donor, nil))
	o.NoDigest, o.Batch = true, 3
	r, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Done != 3 || r.SkippedNames != 0 || r.Retried < 3 {
		t.Fatalf("report %+v", r)
	}
	if got := lines(t, filepath.Join(o.State, "errors.log")); len(got) != 0 {
		t.Fatalf("errors.log %v", got)
	}
}

func TestMergeFallsBackWhenSourceLacksSumDigest(t *testing.T) {
	target, donor := newStore(t), newStore(t)
	put(t, target, "a", 10, map[int64]float64{30: 1})
	put(t, donor, "a", 10, map[int64]float64{40: 4, 30: 3})
	put(t, donor, "b", 10, map[int64]float64{30: 2})
	var rejected atomic.Int32
	old := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/transfer/digest" {
				rejected.Add(1)
				http.Error(w, `json: unknown field "Mode"`, 400) // an older daemon
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	o := options(t, serve(t, target, nil), serve(t, donor, old))
	o.Batch, o.Streams, o.Sample = 1, 1, 2
	r, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Done != 2 || r.Pulled != 2 || r.SkippedNames != 0 || r.Parity[o.From[0]].Unsupported != 2 {
		t.Fatalf("report %+v rejected %d", r, rejected.Load())
	}
	equalPoints(t, "a", points(t, target, "a"), bits(map[int64]float64{40: 4, 30: 1}))
}

func TestMergeConfigErrors(t *testing.T) {
	for _, o := range []Options{{}, {To: "x", From: []string{"http://a:1"}, State: "s"}, {To: "http://a:1", State: "s"}} {
		if _, err := Run(context.Background(), o); err == nil {
			t.Fatalf("%+v accepted", o)
		}
	}
}
