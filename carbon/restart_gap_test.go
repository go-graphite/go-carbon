package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lomik/zapwriter"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// slowFlusher holds flush completions until the test releases them, preserving
// the store's guarantee that a flush finishes after every earlier flush.
type slowFlusher struct {
	db          *store.Store
	delay       time.Duration
	started     chan struct{}
	release     chan struct{}
	inputClosed atomic.Bool
	mu          sync.Mutex
	flushes     []slowFlush
}

type slowFlush struct {
	done       <-chan struct{}
	afterInput bool
}

func (f *slowFlusher) AsyncFlush() (<-chan struct{}, error) {
	flushed, err := f.db.AsyncFlush()
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	f.mu.Lock()
	var previous <-chan struct{}
	if len(f.flushes) > 0 {
		previous = f.flushes[len(f.flushes)-1].done
	}
	f.flushes = append(f.flushes, slowFlush{done, f.inputClosed.Load()})
	f.mu.Unlock()
	go func() {
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		<-flushed
		if previous != nil {
			<-previous
		}
		if f.delay == 0 {
			<-f.release
		}
		close(done)
	}()
	if f.started != nil {
		f.started <- struct{}{}
	}
	return done, nil
}

func (f *slowFlusher) dumpStop(t *testing.T, app *App) {
	t.Helper()
	release := sync.OnceFunc(func() { close(f.release) })
	t.Cleanup(release)
	stopped := make(chan error, 1)
	go func() { stopped <- app.DumpStop() }()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for range 2 {
		select {
		case <-f.started:
		case err := <-stopped:
			t.Fatalf("stop finished before both early flushes started: %v", err)
		case <-timeout.C:
			t.Fatal("early flushes did not start")
		}
	}
	if f.inputClosed.Load() || f.pending() != 2 {
		t.Fatal("input closed or flushes completed before release")
	}
	release()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-timeout.C:
		t.Fatal("dump stop did not finish after releasing flushes")
	}
}

func (f *slowFlusher) WALBytes() uint64 { return f.db.WALBytes() }

// pending counts flushes not finished yet.
func (f *slowFlusher) pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, flush := range f.flushes {
		select {
		case <-flush.done:
		default:
			n++
		}
	}
	return n
}

func (f *slowFlusher) afterInput() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, flush := range f.flushes {
		if flush.afterInput {
			n++
		}
	}
	return n
}

// startSharedDumpApp starts a shared-store app with dumps enabled and one
// persisted point, with the stop's flushes slowed down.
func startSharedDumpApp(t *testing.T) (*App, *Config, *slowFlusher) {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMemTableSize = 64 << 20 // the point stays in a memtable
	cfg.Dump.Enabled, cfg.Dump.Path = true, filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	app.Cache.Add(points.OnePoint("shared.cpu", 42, time.Now().Unix()-2))
	waitForSharedPoint(t, app.MetricStore)
	flusher := &slowFlusher{db: app.MetricStore, started: make(chan struct{}, 4), release: make(chan struct{})}
	wrap := stopFlushStore
	stopFlushStore = func(*store.Store) stopFlusher { return flusher }
	t.Cleanup(func() { stopFlushStore = wrap })
	return app, cfg, flusher
}

func logHas(logs, field, value string) bool {
	return strings.Contains(logs, `"`+field+`": `+value) || strings.Contains(logs, field+`":`+value)
}

// The stop finishes its store flushes while input still flows, and flushes
// nothing once input is closed if nothing was written since.
func TestDumpStopFlushesBeforeInputCloses(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	app, cfg, flusher := startSharedDumpApp(t)
	var pendingAtClose atomic.Int64
	pendingAtClose.Store(-1)
	app.FlushTraces = func() {
		pendingAtClose.Store(int64(flusher.pending()))
		flusher.inputClosed.Store(true)
	}
	flusher.dumpStop(t, app)
	app.FlushTraces = nil
	logs := zapwriter.TestString()
	if got := pendingAtClose.Load(); got != 0 {
		t.Fatalf("%d store flushes still ran when input closed:\n%s", got, logs)
	}
	if n := flusher.afterInput(); n != 0 {
		t.Fatalf("stop flushed the store %d times with input closed, with nothing written:\n%s", n, logs)
	}
	if !strings.Contains(logs, "shared store flushed with input active") || !logHas(logs, "late_flush", "false") {
		t.Fatalf("stop did not report its flushes:\n%s", logs)
	}
	app.ReleaseForHandoff()
	app.Stop()
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if snap, err := db.Snapshot(context.Background(), "shared.cpu"); err != nil || snap.Archives[0].Points[0].Value != 42 {
		t.Fatal("flushed point lost", snap, err)
	}
}

// A write that reaches the store after the last flush is still flushed
// before reads stop.
func TestDumpStopFlushesLateWrites(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	app, cfg, flusher := startSharedDumpApp(t)
	db := app.MetricStore
	var lateErr error
	app.FlushTraces = func() {
		flusher.inputClosed.Store(true)
		_, lateErr = db.Create(context.Background(), store.MetricConfig{Name: "late.write",
			Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Average})
	}
	flusher.dumpStop(t, app)
	app.FlushTraces = nil
	if lateErr != nil {
		t.Fatal(lateErr)
	}
	logs := zapwriter.TestString()
	if n := flusher.afterInput(); n != 1 || !logHas(logs, "late_flush", "true") {
		t.Fatalf("a write after the last flush was flushed %d times after input closed:\n%s", n, logs)
	}
	app.ReleaseForHandoff()
	app.Stop()
	reopened, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Metadata(context.Background(), "late.write"); err != nil {
		t.Fatal("late write lost", err)
	}
}

// The store's background scans stop while input still flows, so its close,
// with input closed, does not wait for them.
func TestDumpStopStopsStoreBackgroundBeforeInputCloses(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Dump.Enabled, cfg.Dump.Path = true, filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	db := app.MetricStore
	var stoppedAtClose atomic.Int32
	stoppedAtClose.Store(-1)
	app.FlushTraces = func() {
		stoppedAtClose.Store(0)
		if db.BackgroundStopped() {
			stoppedAtClose.Store(1)
		}
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.FlushTraces = nil
	if stoppedAtClose.Load() != 1 {
		t.Fatalf("store background work still ran when input closed (%d):\n%s", stoppedAtClose.Load(), zapwriter.TestString())
	}
	app.ReleaseForHandoff()
}

// pendingCheckpointConfig is a whisper-file configuration whose dump stop
// publishes a compact pending checkpoint.
func pendingCheckpointConfig(t *testing.T) (string, *Config) {
	t.Helper()
	root := t.TempDir()
	path := testConfig(t, root)
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	cfg := app.Config
	cfg.Whisper.Compressed, cfg.Whisper.OutOfOrder = true, true
	cfg.Tags.Enabled = false
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "compact"
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Carbonserver.Enabled, cfg.Carbonserver.Listen = true, l.Addr().String()
	l.Close()
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.ConcurrentIndex = true, true
	cfg.Carbonserver.RealtimeIndex = 100
	cfg.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.QueryCacheEnabled, cfg.Carbonserver.FindCacheEnabled, cfg.Carbonserver.GlobCacheEnabled = false, false, false
	return path, cfg
}

// A start verifies the checkpoint while storage opens; a damaged index found
// there still sends it to the ordered restore, which serves the same points.
func TestEarlyCheckpointOpenFallsBackOnCorruptIndex(t *testing.T) {
	testEarlyCheckpointOpenFallsBackOnCorruptIndex(t, false)
}

func TestStartupGateOrderedRestoreFallback(t *testing.T) {
	testEarlyCheckpointOpenFallsBackOnCorruptIndex(t, true)
}

func testEarlyCheckpointOpenFallsBackOnCorruptIndex(t *testing.T, gated bool) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := pendingCheckpointConfig(t)
	if gated {
		cfg.Whisper.StorageBackend, cfg.Whisper.StoreDir = "pebble-chunk", t.TempDir()
		cfg.Whisper.StoreMirrorFiles = true
		cfg.Whisper.StoreEncoding = "exact"
		cfg.Whisper.StoreMetadataCacheSize = 4 << 20
		cfg.Whisper.StoreMetadataCachePrewarm = true
	}
	first := New(path)
	first.Config = cfg
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Unix() - 60
	first.Cache.Add(points.OnePoint("existing.metric", 1, stamp))
	deadline := time.Now().Add(5 * time.Second)
	for !first.Cache.IsEmpty() || !first.Carbonserver.HasMappedIndex() {
		if time.Now().After(deadline) {
			t.Fatal("first live snapshot did not complete", zapwriter.TestString())
		}
		time.Sleep(10 * time.Millisecond)
	}
	first.Lock()
	first.Persister.Stop()
	first.Persister = nil
	first.Unlock()
	first.Cache.Add(points.OnePoint("existing.metric", 2, stamp+1))
	first.Cache.Add(points.OnePoint("cache.only", 3, stamp+1))
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	first.Stop()
	raw, err := os.ReadFile(filepath.Join(cfg.Dump.Path, recovery.ManifestName))
	if err != nil {
		t.Fatal("no checkpoint published", err, zapwriter.TestString())
	}
	var m recovery.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if recovery.CacheFormat(m.Cache.Name) != recovery.FormatCompact {
		t.Fatal("checkpoint is not compact", m.Cache.Name)
	}
	index := filepath.Join(cfg.Dump.Path, m.Index.Name)
	data, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0x40
	if err := os.WriteFile(index, data, 0600); err != nil {
		t.Fatal(err)
	}

	if gated {
		cfg.Whisper.StoreLiveMigration = true // reopen the mirrored predecessor normally
		// The ordered fallback must drain even with an armed startup clock;
		// restored dump points still pass through the durable input log.
		cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "input-log")
		cfg.InputLog.SegmentSize = 16 << 20
		cfg.Cache.WriteoutMinPoints = 1
		cfg.Cache.WriteoutMaxDelay = Duration{time.Hour}
		cfg.Cache.WriteoutRestoreSpread, cfg.Cache.WriteoutStartupSpread = true, true
		cfg.Whisper.StoreMetadataCacheSize = 4 << 20
		cfg.Whisper.StoreMetadataCachePrewarm = true
	}
	next := New(path)
	next.Config = cfg
	cfg.Whisper.MaxUpdatesPerSecond = 1 // keep restored points in the cache
	if err := next.Start(); err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	if gated && (next.MetricStore == nil || next.inputLog.Load() == nil || next.metadataPrewarmDone.Load() == nil) {
		t.Fatal("gated shared/input-log restore path was not exercised")
	}
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "pending read checkpoint unavailable; using ordered restore") || strings.Contains(logs, "serving reads from pending checkpoint") {
		t.Fatalf("damaged checkpoint index was not refused:\n%s", logs)
	}
	// Zero means the checkpoint was opened only when restoreWithPendingReads
	// asked for it.
	if !strings.Contains(logs, "checkpoint_opened_early") || regexp.MustCompile(`checkpoint_opened_early"?: ?0[,}]`).MatchString(logs) {
		t.Fatalf("checkpoint was not opened while storage opened:\n%s", logs)
	}
	url := fmt.Sprintf("http://%s/render/?target=existing.metric&target=cache.only&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, stamp-1, stamp+3)
	want := map[string]float64{fmt.Sprintf("existing.metric/%d", stamp): 1, fmt.Sprintf("existing.metric/%d", stamp+1): 2, fmt.Sprintf("cache.only/%d", stamp+1): 3}
	values, raw, err := renderValues(url)
	if err != nil {
		t.Fatal(err)
	}
	for key, v := range want {
		if got, ok := values[key]; !ok || got != v {
			t.Fatalf("ordered restore serves %s = %v/%t, want %v; %s", key, got, ok, v, raw)
		}
	}
}

// renderValues reads a carbonserver JSON render as "metric/timestamp" values.
func renderValues(url string) (map[string]float64, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, nil, err
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, raw, fmt.Errorf("read %d: %w", resp.StatusCode, err)
	}
	var body struct {
		Metrics []struct {
			Name                string
			StartTime, StepTime int64
			Values              []float64
			IsAbsent            []bool
		}
	}
	if err = json.Unmarshal(raw, &body); err != nil {
		return nil, raw, err
	}
	values := map[string]float64{}
	for _, m := range body.Metrics {
		for i, v := range m.Values {
			if !m.IsAbsent[i] {
				values[fmt.Sprintf("%s/%d", m.Name, m.StartTime+int64(i)*m.StepTime)] = v
			}
		}
	}
	return values, raw, nil
}
