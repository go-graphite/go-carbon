package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// nativeCopyFixture fills a donor with names of varied history and returns
// them byte-sorted. Every third name is also on the target beforehand.
func nativeCopyFixture(t *testing.T, donor *store.Store, targets ...*store.Store) (names, present []string) {
	t.Helper()
	ctx := context.Background()
	for i := range 40 {
		name := fmt.Sprintf("native.sync.%02d", i)
		names = append(names, name)
		config := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 64}}, AggregationMethod: store.Average}
		if _, err := donor.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		var points []store.Point
		for p := range 5 + i%7 {
			points = append(points, store.Point{Timestamp: testMetricNow - int64(10*(p+1)) - int64(60*(i%3)), Value: float64(i*100+p) / 3})
		}
		if err := donor.UpdateMany(ctx, name, points); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			present = append(present, name)
			for _, target := range targets {
				if _, err := target.Create(ctx, config); err != nil {
					t.Fatal(err)
				}
				if err := target.UpdateMany(ctx, name, []store.Point{{Timestamp: testMetricNow - 10, Value: 1000}}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return names, present
}

// countingHandler counts requests per path.
func countingHandler(h http.Handler, counts map[string]*atomic.Int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c := counts[r.URL.Path]; c != nil {
			c.Add(1)
		}
		h.ServeHTTP(w, r)
	})
}

func absentCopyContext() context.Context {
	return context.WithValue(context.Background(), syncAbsentCopyKey{}, true)
}

// comparableExport drops the target-local identity (ID, generation,
// revision) and keeps everything a read can observe: config, every stored
// key and value, point digests and counts.
func comparableExport(t *testing.T, s *store.Store, names []string) []byte {
	t.Helper()
	records, err := s.ExportTransfer(context.Background(), names)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	for _, r := range records {
		r.Version = store.TransferVersion{Name: r.Version.Name}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		out.Write(data)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// The rule for data-path changes: the native path must leave exactly the data
// the metric engine leaves. Absent names are copied natively, present names
// keep their target points through the metric engine, and both targets end
// byte-identical.
func TestSyncNativeCopyMatchesMetricEngine(t *testing.T) {
	t.Parallel()
	donor, metricTarget, nativeTarget := nativeControllerStore(t), nativeControllerStore(t), nativeControllerStore(t)
	names, present := nativeCopyFixture(t, donor, metricTarget, nativeTarget)
	donorServer := httptest.NewServer(nativeControllerService(t, donor))
	defer donorServer.Close()
	counts := map[string]*atomic.Int64{"/v1/native/pull": {}, "/v1/transfer/pull": {}}
	metricServer := httptest.NewServer(nativeControllerService(t, metricTarget))
	defer metricServer.Close()
	nativeServer := httptest.NewServer(countingHandler(nativeControllerService(t, nativeTarget), counts))
	defer nativeServer.Close()

	run := func(target, engine string) syncCopyResult {
		b := newSyncLiveBackend(syncLiveOptions{copyStreams: 2, copyAttempts: 2, copyEngine: engine})
		journal := filepath.Join(t.TempDir(), "copy.jsonl")
		res, err := b.Copy(absentCopyContext(), donorServer.URL, target, names, journal)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := run(metricServer.URL, syncCopyEngineMetric); res.Copied != len(names) || len(res.Failed) != 0 {
		t.Fatalf("metric copy: %+v", res)
	}
	if res := run(nativeServer.URL, syncCopyEngineAuto); res.Copied != len(names) || len(res.Failed) != 0 {
		t.Fatalf("native copy: %+v", res)
	}
	stats := nativeTarget.NativeStats()
	if stats.CopiedNames != uint64(len(names)-len(present)) || stats.DeferredNames != uint64(len(present)) {
		t.Fatalf("native path copied %d deferred %d, want %d/%d", stats.CopiedNames, stats.DeferredNames, len(names)-len(present), len(present))
	}
	if counts["/v1/native/pull"].Load() != 1 || counts["/v1/transfer/pull"].Load() == 0 {
		t.Fatalf("pulls native=%d metric=%d: present names must still use the metric engine", counts["/v1/native/pull"].Load(), counts["/v1/transfer/pull"].Load())
	}
	if metricTarget.NativeStats().CopiedNames != 0 {
		t.Fatal("--copy-engine metric used the native path")
	}
	want, got := comparableExport(t, metricTarget, names), comparableExport(t, nativeTarget, names)
	if !bytes.Equal(want, got) {
		t.Fatalf("native and metric copies differ:\nmetric %s\nnative %s", want, got)
	}
	// Present names kept their target points (target wins) on both.
	series, err := nativeTarget.Fetch(context.Background(), present[0], int(testMetricNow-100), int(testMetricNow))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range series.Values {
		found = found || v == 1000
	}
	if !found {
		t.Fatalf("target point lost for %s: %v", present[0], series.Values)
	}
}

// A resumed wave never copies credited names again: the journal covers them
// and no native or metric pull is sent.
func TestSyncNativeCopyResumeRedoesNothing(t *testing.T) {
	t.Parallel()
	donor, target := nativeControllerStore(t), nativeControllerStore(t)
	names, _ := nativeCopyFixture(t, donor)
	donorServer := httptest.NewServer(nativeControllerService(t, donor))
	defer donorServer.Close()
	counts := map[string]*atomic.Int64{"/v1/native/pull": {}, "/v1/transfer/pull": {}, "/v1/native/source": {}}
	targetServer := httptest.NewServer(countingHandler(nativeControllerService(t, target), counts))
	defer targetServer.Close()
	journal := filepath.Join(t.TempDir(), "copy.jsonl")
	for range 2 {
		b := newSyncLiveBackend(syncLiveOptions{copyStreams: 2, copyAttempts: 1})
		res, err := b.Copy(absentCopyContext(), donorServer.URL, targetServer.URL, names, journal)
		if err != nil {
			t.Fatal(err)
		}
		if res.Copied != len(names) || len(res.Failed) != 0 {
			t.Fatalf("copy: %+v", res)
		}
	}
	if n := counts["/v1/native/pull"].Load(); n != 1 {
		t.Fatalf("native pulls %d, want 1 (resume redid work)", n)
	}
	if n := counts["/v1/transfer/pull"].Load(); n != 0 {
		t.Fatalf("metric pulls %d, want 0 for an all-absent wave", n)
	}
}

// Fills and window repairs touch names the target holds: never native.
func TestSyncNativeCopyOnlyForAbsentPhase(t *testing.T) {
	t.Parallel()
	donor, target := nativeControllerStore(t), nativeControllerStore(t)
	names, _ := nativeCopyFixture(t, donor)
	donorServer := httptest.NewServer(nativeControllerService(t, donor))
	defer donorServer.Close()
	targetServer := httptest.NewServer(nativeControllerService(t, target))
	defer targetServer.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 2, copyAttempts: 1})
	if _, err := b.Copy(context.Background(), donorServer.URL, targetServer.URL, names, filepath.Join(t.TempDir(), "fill.jsonl")); err != nil {
		t.Fatal(err)
	}
	if target.NativeStats().CopiedNames != 0 {
		t.Fatal("a fill copy used the native path")
	}
}

// A store without the native endpoint keeps the whole wave on the metric
// engine, and the answer is cached for the run.
func TestSyncNativeCopyFallsBackWithoutEndpoint(t *testing.T) {
	t.Parallel()
	donor, target := nativeControllerStore(t), nativeControllerStore(t)
	names, _ := nativeCopyFixture(t, donor)
	donorHandler := nativeControllerService(t, donor)
	var capabilityCalls atomic.Int64
	donorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/native/") {
			if r.URL.Path == "/v1/native/capabilities" {
				capabilityCalls.Add(1)
			}
			http.NotFound(w, r)
			return
		}
		donorHandler.ServeHTTP(w, r)
	}))
	defer donorServer.Close()
	targetServer := httptest.NewServer(nativeControllerService(t, target))
	defer targetServer.Close()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 2, copyAttempts: 1})
	dir := t.TempDir()
	for i, wave := range [][]string{names[:20], names[20:]} {
		res, err := b.Copy(absentCopyContext(), donorServer.URL, targetServer.URL, wave, filepath.Join(dir, fmt.Sprintf("w%d.jsonl", i)))
		if err != nil {
			t.Fatal(err)
		}
		if res.Copied != len(wave) || len(res.Failed) != 0 {
			t.Fatalf("fallback copy: %+v", res)
		}
	}
	if target.NativeStats().CopiedNames != 0 || capabilityCalls.Load() != 1 {
		t.Fatalf("native copied %d, capability probes %d", target.NativeStats().CopiedNames, capabilityCalls.Load())
	}
}

func TestNativeRangesRespectReceiverLimits(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 300<<10)
	names := []string{"a." + long, "b." + long, "c." + long, "d." + long, "e"}
	got := nativeRanges(names)
	want := [][2]int{{0, 3}, {3, 5}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ranges %v, want %v", got, want)
	}
	many := copyNames("n.", int(nativeRangeNames)*2+1)
	got = nativeRanges(many)
	if len(got) != 3 || got[0][1] != int(nativeRangeNames) || got[2][1] != len(many) {
		t.Fatalf("name-count cut: %v", got)
	}
}

func TestSyncCopyWavePerSource(t *testing.T) {
	t.Parallel()
	b := newSyncLiveBackend(syncLiveOptions{copyStreams: 16, sourceStreams: map[string]int{"http://big:4242": 64}, copyEngine: syncCopyEngineMetric})
	if b.copyStreams("http://big:4242") != 64 || b.copyStreams("http://small:4242") != 16 {
		t.Fatal("per-source copy streams")
	}
	if b.copyWaveNames("http://big:4242", 500) != 32000 || b.copyWaveNames("http://small:4242", 500) != 8000 {
		t.Fatal("metric waves follow each donor's own budget")
	}
	b.o.copyEngine = syncCopyEngineAuto
	if w := b.copyWaveNames("http://small:4242", 500); w != syncNativeWaveRanges*int(nativeRangeNames) {
		t.Fatalf("native wave %d", w)
	}
}
