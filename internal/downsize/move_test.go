package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func fixturePlan(t *testing.T, source, target string, names []string) string {
	t.Helper()
	root := t.TempDir()
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(root)))
	var data bytes.Buffer
	e := json.NewEncoder(&data)
	for _, row := range []planRow{{Type: "header", Format: "go-carbon-ring", Version: 1, PlanID: id, Source: "old", Destination: "new"}} {
		if err := e.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range names {
		if err := e.Encode(planRow{Type: "move", Metric: name, EstimatedBytes: 1024}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Encode(planRow{Type: "end", Metrics: int64(len(names)), EstimatedBytes: int64(len(names) * 1024)}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "moves.jsonl"), data.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	p := manifest{Format: "go-carbon-ring", Version: 1, PlanID: id, Spec: specification{Version: 1, Cluster: "local", Site: "test", Old: topology{Replicas: 1, Nodes: []member{{ID: "old", Endpoint: source}}}, New: topology{Replicas: 1, Nodes: []member{{ID: "new", Endpoint: target}}}}, Streams: []route{{Path: "moves.jsonl", Source: "old", Destination: "new", Metrics: int64(len(names)), EstimatedBytes: int64(len(names) * 1024), SHA256: fmt.Sprintf("%x", sha256.Sum256(data.Bytes()))}}}
	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	b := barrier{PlanID: id, ExpiresAt: time.Now().Add(time.Hour).Unix(), RelaySwitched: true, OldWritersDrained: true, Reconciled: true, Sources: []string{"old"}}
	encoded, err = json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "barrier.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func testOptions(root string) options {
	return options{plan: root, state: filepath.Join(root, "state"), barrier: filepath.Join(root, "barrier.json"), apply: true, delete: true, deleteMode: "batch", localLab: true, streams: 2, batch: 8, deleteRate: 100000, maxBytes: 1 << 20, tick: time.Second, p99: 100}
}

var testMetricNow = time.Now().Unix() / 10 * 10

func localStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func localService(t *testing.T, s *store.Store) http.Handler {
	t.Helper()
	service, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), MaxTransfers: 8, MaxBodyBytes: 1 << 20, TransferSources: []string{"127.0.0.1"}}, s)
	if err != nil {
		t.Fatal(err)
	}
	return service.Handler()
}
func addPoints(t *testing.T, s *store.Store, name string, value float64) {
	t.Helper()
	ctx := context.Background()
	config := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 128}}, AggregationMethod: store.Sum}
	now := testMetricNow
	if _, err := s.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMany(ctx, name, []store.Point{{Timestamp: now - 20, Value: 1}, {Timestamp: now - 10, Value: value}}); err != nil {
		t.Fatal(err)
	}
}

func TestMoverEndToEndResumeLostDeleteResponse(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	var names []string
	for i := range 19 {
		name := fmt.Sprintf("move.%02d", i)
		names = append(names, name)
		addPoints(t, source, name, float64(i+2))
	}
	addPoints(t, target, names[0], 999)
	handler := localService(t, source)
	var lost atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/delete" && !lost.Swap(true) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, r)
			if rec.Code != 200 {
				t.Errorf("delete: %d %s", rec.Code, rec.Body.String())
			}
			http.Error(w, "simulated lost acknowledgment", 500)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer src.Close()
	dst := httptest.NewServer(localService(t, target))
	defer dst.Close()
	root := fixturePlan(t, src.URL, dst.URL, names)
	o := testOptions(root)
	if err := run(ctx, o); err == nil {
		t.Fatal("lost delete acknowledgment must interrupt")
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("resume", err)
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("completed resume", err)
	}
	for _, name := range names {
		if _, err := source.Metadata(ctx, name); !errors.Is(err, store.ErrNotFound) {
			t.Fatal("source retains", name, err)
		}
		if _, err := target.Metadata(ctx, name); err != nil {
			t.Fatal("target absent", name)
		}
	}
	snapshot, err := target.Snapshot(ctx, names[0])
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Archives[0].Points[1].Value != 999 {
		t.Fatal("target overlap was lost")
	}
}

func TestMoverResumeRechecksTargetBeforeCleanup(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	name := "resume.target"
	addPoints(t, source, name, 2)
	handler := localService(t, source)
	var interrupted atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/delete" && !interrupted.Swap(true) {
			http.Error(w, "interrupt before cleanup", 500)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer src.Close()
	dst := httptest.NewServer(localService(t, target))
	defer dst.Close()
	o := testOptions(fixturePlan(t, src.URL, dst.URL, []string{name}))
	if err := run(ctx, o); err == nil {
		t.Fatal("cleanup did not interrupt")
	}
	if err := target.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, o); err == nil {
		t.Fatal("resume accepted an absent target generation")
	}
	if _, err := source.Metadata(ctx, name); err != nil {
		t.Fatal("source was deleted", err)
	}
}
func TestPlanRejectsTamperingTraversalAndIncomplete(t *testing.T) {
	for _, kind := range []string{"checksum", "truncated", "version", "traversal", "symlink", "missing manifest"} {
		t.Run(kind, func(t *testing.T) {
			root := fixturePlan(t, "http://127.0.0.1:1", "http://127.0.0.1:2", []string{"a"})
			manifestPath := filepath.Join(root, "manifest.json")
			data, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var p manifest
			if err := json.Unmarshal(data, &p); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "checksum":
				if err := os.WriteFile(filepath.Join(root, "moves.jsonl"), []byte("bad\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				path := filepath.Join(root, "moves.jsonl")
				raw, _ := os.ReadFile(path)
				if err := os.WriteFile(path, raw[:len(raw)-1], 0600); err != nil {
					t.Fatal(err)
				}
			case "version":
				p.Version = 2
			case "traversal":
				p.Streams[0].Path = "../outside"
			case "symlink":
				outside := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(outside, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
					t.Fatal(err)
				}
				p.Streams[0].Path = "escape"
			case "missing manifest":
				if err := os.Remove(manifestPath); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "missing manifest" {
				data, _ = json.Marshal(p)
				if err := os.WriteFile(manifestPath, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readPlan(root); err == nil {
				t.Fatal("bad plan accepted")
			}
		})
	}
}
func TestDryRunNeverContactsHosts(t *testing.T) {
	root := fixturePlan(t, "http://127.0.0.1:1", "http://127.0.0.1:2", []string{"dry"})
	o := testOptions(root)
	o.apply = false
	if err := run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(o.state); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("dry run created journal")
	}
}
func TestJournalLockTruncationAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	j, err := openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openJournal(path, "plan"); err == nil {
		t.Fatal("concurrent journal opened")
	}
	for i := range 300 {
		if err := j.save(task{Stream: "stream", Start: int64(i), End: int64(i + 1), Phase: "done"}); err != nil {
			t.Fatal(err)
		}
	}
	j.close()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{truncated")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	j, err = openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if len(j.tasks) != 300 {
		t.Fatal("checkpoint lost after compact/recover")
	}
}
func TestTunerRampBackoffAndHardStop(t *testing.T) {
	tune := newTuner([]string{"a", "b"}, capacity{8, 2048, 64 << 20, 65536}, true, 0, 100)
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, ReadSamples: 100}
	for range 4 {
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	if tune.hosts["a"].Streams <= 1 {
		t.Fatal("did not ramp")
	}
	sample.CPUCores = 90
	previous := tune.hosts["a"].Streams
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	if tune.hosts["a"].Streams >= previous {
		t.Fatal("did not back off")
	}
	sample.CPUCores = 0
	sample.Threads = 6100
	tune.hosts["a"].previous = &hostSample{Generation: "one", Threads: 6100}
	if hard, soft := guard(sample, storageSample{}, tune.hosts["a"].previous, nil, 100); hard != "" || soft != "" {
		t.Fatal("static high thread count throttles", hard, soft)
	}
	sample.Unlogged = 1
	if err := tune.observe("a", sample, storageSample{}); err == nil {
		t.Fatal("unlogged points did not hard stop")
	}
}

func TestMoverRetireAndRefreshLateSourceDelta(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	names := []string{"retire.a", "retire.b"}
	for _, name := range names {
		addPoints(t, source, name, 2)
	}
	sourceHandler := localService(t, source)
	var late atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/stage-delete" && !late.Swap(true) {
			if err := source.UpdateMany(ctx, names[0], []store.Point{{Timestamp: testMetricNow, Value: 777}}); err != nil {
				t.Error(err)
			}
		}
		sourceHandler.ServeHTTP(w, r)
	}))
	defer src.Close()
	dst := httptest.NewServer(localService(t, target))
	defer dst.Close()
	root := fixturePlan(t, src.URL, dst.URL, names)
	o := testOptions(root)
	o.deleteMode = "retire"
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if page, err := source.List(ctx, ""); err != nil || len(page) != 0 {
		t.Fatal("source not retired", err)
	}
	snapshot, err := target.Snapshot(ctx, names[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 3 || snapshot.Archives[0].Points[2].Value != 777 {
		t.Fatal("late source hole was not copied before retirement")
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("retired resume", err)
	}
}

func TestMoverSplitsOversizedBatches(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	var names []string
	for i := range 20 {
		name := fmt.Sprintf("split.%02d", i)
		names = append(names, name)
		addPoints(t, source, name, 2)
	}
	src := httptest.NewServer(localService(t, source))
	defer src.Close()
	handler := localService(t, target)
	var splits atomic.Int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var request struct{ Names []string }
			if err := json.Unmarshal(data, &request); err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			if len(request.Names) > 3 {
				splits.Add(1)
				http.Error(w, "test byte budget", 413)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer dst.Close()
	root := fixturePlan(t, src.URL, dst.URL, names)
	o := testOptions(root)
	o.deleteMode = "auto"
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if splits.Load() == 0 {
		t.Fatal("fixture did not exercise split")
	}
	if page, err := source.List(ctx, ""); err != nil || len(page) != 0 {
		t.Fatal("split source was not retired", err)
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("split resume", err)
	}
}

func TestTunerReadBaselineAndParkedAdmission(t *testing.T) {
	tune := newTuner([]string{"a", "b"}, capacity{8, 8192, 64 << 20, 65536}, true, 0, 100)
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, ReadSamples: 100, ReadP99MS: 2}
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	tune.captureReadBaselines()
	sample.ReadP99MS = 50
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	if tune.hosts["a"].Streams != 0 {
		t.Fatal("minimum concurrency did not park on read regression")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tune.admit(ctx, "a", "b"); !errors.Is(err, context.Canceled) {
		t.Fatal("parked admission did not cancel", err)
	}
	sample.ReadP99MS = 2
	for range 2 {
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	if tune.hosts["a"].Streams < 1 {
		t.Fatal("healthy host did not resume")
	}
}

func TestTunerBindingResourcesAndDuplexPacing(t *testing.T) {
	tune := newTuner([]string{"a", "b", "c"}, capacity{64, 8192, 64 << 20, 65536}, true, 1000, 100)
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, CPUCores: 24, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 3.125e9, DiskWriteCapacity: 2e9}
	for range 30 {
		if err := tune.observe("a", sample, storageSample{MaxTransfers: 64}); err != nil {
			t.Fatal(err)
		}
	}
	if tune.hosts["a"].Streams != 64 {
		t.Fatal("host capacity not reached", tune.hosts["a"].capacity)
	}
	for _, change := range []func(*hostSample){
		func(s *hostSample) { s.NICIngressBytes = 0.99e9 },
		func(s *hostSample) { s.NICEgressBytes = 0.99e9 },
		func(s *hostSample) { s.DiskWriteBytes = 1.99e9 },
		func(s *hostSample) { s.CPUCores = 90 },
	} {
		current := sample
		change(&current)
		if hard, soft := guard(current, storageSample{}, &sample, nil, 100); hard != "" || soft == "" {
			t.Fatal("binding resource missed", hard, soft)
		}
	}
	// Simultaneous 0.6 GB/s ingress and egress are below both independent caps.
	sample.NICIngressBytes, sample.NICEgressBytes = .6e9, .6e9
	if hard, soft := guard(sample, storageSample{}, nil, nil, 100); hard != "" || soft != "" {
		t.Fatal("duplex traffic was summed", hard, soft)
	}
	started := time.Now()
	tune.charge("a", "b", 1e9, started)
	if tune.hosts["a"].ingressAt.After(started) || tune.hosts["b"].egressAt.After(started) {
		t.Fatal("one direction charged the opposite direction")
	}
	tune.charge("b", "a", 1e9, started)
	if tune.hosts["a"].ingressAt != tune.hosts["a"].egressAt {
		t.Fatal("duplex traffic serializes")
	}
}

func TestTunerReadIsolationReleasesCPUCeiling(t *testing.T) {
	tune := newTuner([]string{"a"}, capacity{64, 8192, 64 << 20, 65536}, true, 1000, 100)
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, CPUCores: 75, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9}
	for range 30 {
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	if tune.hosts["a"].Streams != 13 {
		t.Fatal("foreground CPU not reserved", tune.hosts["a"].Streams)
	}
	sample.CPUCores, sample.ReadsBlocked = 10, true
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	tune.captureReadBaselines()
	for range 30 {
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	if tune.hosts["a"].Streams != 64 {
		t.Fatal("read isolation did not release capacity", tune.hosts["a"].Streams)
	}
}
