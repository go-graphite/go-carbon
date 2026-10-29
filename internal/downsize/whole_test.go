package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/carbon"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
	"github.com/go-graphite/go-carbon/internal/storecopy"
)

func replacementConfig(t *testing.T) (string, *carbon.Config) {
	t.Helper()
	root := t.TempDir()
	path := carbon.TestConfig(root)
	cfg, err := carbon.ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Common.MaxCPU = 4
	cfg.Common.MetricInterval = &carbon.Duration{Duration: time.Hour}
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, false
	cfg.Carbonserver.Enabled = false
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreDir = filepath.Join(root, "store")
	cfg.Whisper.StoreCacheSize = 1 << 20
	cfg.Whisper.StoreMemTableSize = 1 << 20
	cfg.Whisper.Workers = 2
	cfg.Cache.WriteoutMinPoints = 1000
	cfg.Cache.WriteoutMaxDelay = carbon.Duration{Duration: time.Hour}
	cfg.Buckyd.Enabled, cfg.Buckyd.Bind = true, "127.0.0.1:0"
	cfg.Buckyd.CopyMinFreeBytes = 1
	cfg.Buckyd.TransferSources = []string{"127.0.0.1"}
	cfg.InputLog.Enabled = true
	cfg.InputLog.Path = filepath.Join(root, "input-log")
	cfg.InputLog.SyncInterval = carbon.Duration{Duration: 10 * time.Millisecond}
	cfg.InputLog.SegmentSize = 16 << 20
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}
func replacementStart(t *testing.T, path string, cfg *carbon.Config) *carbon.App {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = toml.NewEncoder(f).Encode(cfg)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	app := carbon.New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	return app
}
func TestWholeReplacementResumeReplayAndLostResponses(t *testing.T) { replacementHandoff(t, false) }
func TestMixedResizeUsesSameEngine(t *testing.T)                    { replacementHandoff(t, true) }
func replacementHandoff(t *testing.T, mixed bool) {
	t.Setenv("RESIZE_TEST_CONTROL", "test-secret")
	sourcePath, cfg := replacementConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = listener.Addr().String()
	listener.Close()
	source := replacementStart(t, sourcePath, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := "copy.metric"
	var retainedURL, retiredURL, incoming string
	var retired *store.Store
	if mixed {
		retained := localStore(t)
		retired = localStore(t)
		rs := httptest.NewServer(localService(t, retained))
		defer rs.Close()
		retainedURL = rs.URL
		ds := httptest.NewServer(localService(t, retired))
		defer ds.Close()
		retiredURL = ds.URL
		var nodes []hashing.Node
		for _, node := range []string{"old:2003=000", "retained:2003=001", "retired:2003=002"} {
			n, err := hashing.NewNodeParser(node)
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		old, err := hashing.Compile("jump_fnv1a", nodes, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		next, err := hashing.Compile("jump_fnv1a", nodes[:2], 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		name = ""
		for i := 0; name == "" || incoming == ""; i++ {
			candidate := fmt.Sprintf("copy.metric.%04d", i)
			owner := old.Lookup(candidate, nil)[0]
			if owner == 0 {
				name = candidate
			}
			if owner == 2 && next.Lookup(candidate, nil)[0] == 0 {
				incoming = candidate
			}
		}
		addPoints(t, retired, incoming, 77)
	}
	now := time.Now().Unix()
	ret := store.Retention{Step: 1, Count: 3600}
	_, err = source.MetricStore.CreateFromSnapshot(ctx, store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: name, AggregationMethod: store.Average, Retentions: []store.Retention{ret}}}, Archives: []store.Archive{{Retention: ret, Points: []store.Point{{Timestamp: now - 20, Value: 1}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.MetricStore.Flush(); err != nil {
		t.Fatal(err)
	}
	var dropMu sync.Mutex
	dropBegin, dropRelease := true, true
	handler := source.Buckyd.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dropMu.Lock()
		drop := r.URL.Path == storecopy.Endpoint && r.Method == http.MethodPost && dropBegin || r.URL.Path == storecopy.Endpoint && r.Method == http.MethodDelete && dropRelease
		if r.URL.Path == storecopy.Endpoint && r.Method == http.MethodPost {
			dropBegin = false
		}
		if r.URL.Path == storecopy.Endpoint && r.Method == http.MethodDelete {
			dropRelease = false
		}
		dropMu.Unlock()
		if drop {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			http.Error(w, "reply lost after commit", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	var targetMu sync.RWMutex
	var targetHandler http.Handler
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetMu.RLock()
		h := targetHandler
		targetMu.RUnlock()
		if h == nil {
			http.Error(w, "offline", 503)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer target.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(root, "clone")
	wo := workerOptions{Directory: stage, Target: "new", ControlEnv: "RESIZE_TEST_CONTROL", LocalLab: true}
	workerOne, err := newWorker(wo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerOne.http.CloseIdleConnections)
	var workerMu sync.RWMutex
	current := workerOne
	workerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerMu.RLock()
		h := current
		workerMu.RUnlock()
		h.ServeHTTP(w, r)
	}))
	defer workerServer.Close()
	spec := map[string]any{"version": 1, "cluster": "test", "site": "local", "old": map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []any{map[string]any{"id": "old", "node": "old:2003=000", "endpoint": server.URL}}}, "new": map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []any{map[string]any{"id": "new", "node": "new:2003=000", "endpoint": target.URL}}}, "placements": map[string]any{"new": ringplan.Placement{Worker: workerServer.URL, Directory: stage, Preflight: filepath.Join(root, "preflight.json")}}}
	if mixed {
		old := spec["old"].(map[string]any)
		next := spec["new"].(map[string]any)
		retained := map[string]any{"id": "retained", "node": "retained:2003=001", "endpoint": retainedURL}
		old["nodes"] = append(old["nodes"].([]any), retained, map[string]any{"id": "retired", "node": "retired:2003=002", "endpoint": retiredURL})
		next["nodes"] = append(next["nodes"].([]any), retained)
	}
	specPath := filepath.Join(root, "spec.json")
	data, _ := json.Marshal(spec)
	if err := os.WriteFile(specPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(root, "plan")
	planned, err := ringplan.Resize(ctx, specPath, planPath, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Whole) != 1 || (!mixed && len(planned.Streams) != 0) || (mixed && len(planned.Streams) == 0) {
		t.Fatal("replacement should not inventory metrics", planned)
	}
	o := options{plan: planPath, state: filepath.Join(root, "state.jsonl"), apply: true, localLab: true, streams: 2, batch: 32, maxBytes: 1 << 20, deleteRate: 1000, tick: time.Second, mbps: 1000, p99: 100, controlTokenEnv: "RESIZE_TEST_CONTROL"}
	if err := run(ctx, o); err == nil || errors.Is(err, errInputFence) {
		t.Fatal("lost begin response must stop before worker", err)
	}
	if err := run(ctx, o); !errors.Is(err, errInputFence) {
		t.Fatal("resume baseline", err)
	}
	checkpoint := workerOne.snapshot().Request.Checkpoint
	if checkpoint == "" {
		t.Fatal("no checkpoint")
	}
	// The real receiver appends this point after the baseline checkpoint. It must
	// be recovered from the captured input log, whether or not persister ran.
	conn, err := net.Dial("tcp", cfg.Tcp.Listen)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "%s 2 %d\n", name, now-10); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	deadlineInput := time.NewTimer(3 * time.Second)
	defer deadlineInput.Stop()
	inputTick := time.NewTicker(10 * time.Millisecond)
	defer inputTick.Stop()
	for len(source.Cache.Get(name)) == 0 {
		select {
		case <-inputTick.C:
		case <-deadlineInput.C:
			t.Fatal("receiver did not accept gap point")
		}
	}
	// Restart the target worker with its durable state and the same fixed URL.
	workerTwo, err := newWorker(wo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerTwo.http.CloseIdleConnections)
	workerMu.Lock()
	current = workerTwo
	workerMu.Unlock()
	b := replacementBarrier{PlanID: planned.PlanID, ExpiresAt: time.Now().Add(time.Minute).Unix(), InputFenced: true, Sources: []string{"old"}}
	o.barrier = filepath.Join(root, "barrier.json")
	if err := atomicState(o.barrier, b); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, o); !errors.Is(err, errReplayReady) {
		t.Fatal("prepare", err)
	}
	if workerTwo.snapshot().Request.Checkpoint != checkpoint {
		t.Fatal("worker changed checkpoint")
	}
	if _, err := os.Stat(filepath.Join(stage, "input-log/REQUIRE-COMPLETE-REPLAY")); err != nil {
		t.Fatal(err)
	}
	targetPath, targetCfg := replacementConfig(t)
	targetCfg.Whisper.StoreDir = filepath.Join(stage, "store")
	targetCfg.InputLog.Path = filepath.Join(stage, "input-log")
	targetCfg.Cache.WriteoutMinPoints = 0
	targetCfg.Cache.WriteoutMaxDelay = carbon.Duration{Duration: 0}
	clone := replacementStart(t, targetPath, targetCfg)
	targetMu.Lock()
	targetHandler = clone.Buckyd.Handler()
	targetMu.Unlock()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := clone.MetricStore.Snapshot(ctx, name)
		if workerTwo.snapshot().ReplayComplete && err == nil && len(snap.Archives[0].Points) == 2 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("replay marker not cleared")
		}
	}
	b.TargetsReady = map[string]bool{"new": true}
	if err := atomicState(o.barrier, b); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, o); err == nil {
		t.Fatal("lost release reply must stop once")
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("release resume", err)
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("completed resume", err)
	}
	snapshot, err := clone.MetricStore.Snapshot(ctx, name)
	if err != nil || len(snapshot.Archives[0].Points) != 2 {
		t.Fatal("baseline read incomplete", snapshot, err)
	}
	if mixed {
		snapshot, err := clone.MetricStore.Snapshot(ctx, incoming)
		if err != nil || len(snapshot.Archives[0].Points) != 2 || snapshot.Archives[0].Points[1].Value != 77 {
			t.Fatal("dependent incoming metric missing", snapshot, err)
		}
		if _, err := retired.Snapshot(ctx, incoming); err != nil {
			t.Fatal("non-delete mixed source removed", err)
		}
	}
	if _, err := source.MetricStore.Snapshot(ctx, name); err != nil {
		t.Fatal("source was deleted", err)
	}
}

func TestWorkerLeaseCannotChangePlanOwnership(t *testing.T) {
	t.Setenv("RESIZE_TEST_CONTROL", "secret")
	w, err := newWorker(workerOptions{Directory: filepath.Join(t.TempDir(), "stage"), Target: "target", ControlEnv: "RESIZE_TEST_CONTROL", LocalLab: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.http.CloseIdleConnections()
	w.state.Request = workerRequest{PlanID: strings.Repeat("a", 64), Checkpoint: "checkpoint"}
	limits := workerLease{Limits: storecopy.Limits{Workers: 2, Chunk: 1 << 20, Rate: 1e9, Expires: time.Now().Add(30 * time.Second)}, PlanID: w.state.Request.PlanID, Checkpoint: "checkpoint"}
	send := func(lease workerLease) int {
		data, _ := json.Marshal(lease)
		r := httptest.NewRequest(http.MethodPost, "/limits", bytes.NewReader(data))
		r.Header.Set("X-Move-Control", "secret")
		out := httptest.NewRecorder()
		w.ServeHTTP(out, r)
		return out.Code
	}
	if code := send(limits); code != 200 {
		t.Fatal(code)
	}
	before, err := os.ReadFile(w.path("lease"))
	if err != nil {
		t.Fatal(err)
	}
	limits.PlanID = strings.Repeat("b", 64)
	if code := send(limits); code != 409 {
		t.Fatal("other plan overwrote admission lease", code)
	}
	after, err := os.ReadFile(w.path("lease"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("lease changed", err)
	}
}
