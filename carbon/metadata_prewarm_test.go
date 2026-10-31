package carbon

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	tcp "github.com/go-graphite/go-carbon/receiver/tcp"
)

func TestMetadataPrewarmConfig(t *testing.T) {
	t.Parallel()
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMetadataCachePrewarmWorkers = -1
	if err := validateStorageConfig(cfg); err == nil || !strings.Contains(err.Error(), "prewarm") {
		t.Fatalf("negative prewarm workers accepted: %v", err)
	}
	cfg.Whisper.StoreMetadataCachePrewarmWorkers, cfg.Whisper.StoreMetadataCachePrewarmRate = 0, -1
	if err := validateStorageConfig(cfg); err == nil || !strings.Contains(err.Error(), "prewarm") {
		t.Fatalf("negative prewarm rate accepted: %v", err)
	}
	cfg.Whisper.StoreMetadataCachePrewarmRate = 0
	cfg.Whisper.StoreMetadataCachePrewarmByteRate = -1
	if err := validateStorageConfig(cfg); err == nil || !strings.Contains(err.Error(), "prewarm") {
		t.Fatalf("negative prewarm byte rate accepted: %v", err)
	}
	cfg.Whisper.StoreMetadataCachePrewarmByteRate = 0
	cfg.Whisper.StoreMetadataCachePrewarmWait = &Duration{-time.Second}
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("negative wait accepted")
	}
	cfg.Whisper.StoreMetadataCachePrewarmWait = nil
	if got := metadataPrewarmWait(cfg.Whisper); got != 600*time.Second {
		t.Fatal("adaptive cap", got)
	}
	cfg.Whisper.StoreMetadataCachePrewarmWaitCap = &Duration{-time.Second}
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("negative wait cap accepted")
	}
	cfg.Whisper.StoreMetadataCachePrewarmWaitCap = &Duration{900 * time.Second}
	if got := metadataPrewarmWait(cfg.Whisper); got != 900*time.Second {
		t.Fatal("configured cap", got)
	}
	cfg.Whisper.StoreMetadataCachePrewarmWait = &Duration{300 * time.Second}
	if got := metadataPrewarmWait(cfg.Whisper); got != 300*time.Second {
		t.Fatal("fixed wait changed", got)
	}
	cfg.Whisper.StoreMetadataCachePrewarmWait, cfg.Whisper.StoreMetadataCachePrewarmWaitCap = nil, nil
	next := *cfg
	next.Whisper.StoreMetadataCachePrewarm = true
	next.Whisper.StoreMetadataCachePrewarmWorkers = 8
	next.Whisper.StoreRestartCPUHeadroom = 1.5
	// Read at start only: reload accepts changes for the next process start.
	if storageSettingsChanged(cfg, &next) {
		t.Fatal("prewarm settings require a restart")
	}
}

// TestMetadataPrewarmAfterDumpRestart restarts an incrementally migrating
// store (the continuation codec) whose cache held unpersisted points of
// converted metrics, and checks that the start prewarms their metadata from
// the pending checkpoint.
func TestMetadataPrewarmAfterDumpRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	checkpoint := false // Exercise the catalog fallback independently of saved caches.
	cfg.Whisper.StoreMetadataCacheCheckpoint = &checkpoint
	cfg.Dump.Enabled, cfg.Dump.Path = true, filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.StoreMetadataCacheSize = 16 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	cfg.Whisper.StoreMetadataCachePrewarmWorkers = 2
	// Scans map the read index a pending checkpoint is written against.
	cfg.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	ctx := context.Background()
	if done := app.metadataPrewarmDone.Load(); done != nil {
		<-*done // the first start restored nothing
	}
	names := []string{"first", "second", "third"}
	now := time.Now().Unix()
	for _, name := range names {
		if _, err := app.Persister.ConvertFile(ctx, name); err != nil {
			t.Fatal(err)
		}
		app.Cache.Add(points.OnePoint(name, 1, now-10))
	}
	waitMigration(t, func() bool { return app.Cache.IsEmpty() && app.Carbonserver.HasMappedIndex() })
	// Points the persister no longer writes stay in the cache for the dump.
	app.Lock()
	app.Persister.Stop()
	app.Persister = nil
	app.Unlock()
	for _, name := range names {
		app.Cache.Add(points.OnePoint(name, 2, now-5))
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.ReleaseForHandoff()
	app.Stop()

	next := startIncrementalApp(t, path)
	done := next.metadataPrewarmDone.Load()
	if done == nil {
		t.Fatal("no metadata prewarm started", zapwriter.TestString())
	}
	select {
	case <-*done:
	case <-time.After(10 * time.Second):
		t.Fatal("metadata prewarm did not finish")
	}
	logs := zapwriter.TestString()
	if !logHas(logs, "source", `"pending checkpoint"`) || !logHas(logs, "names", "3") || !logHas(logs, "failed", "0") {
		t.Fatalf("prewarm log:\n%s", logs)
	}
	stats := next.MetricStore.Stats().StateCache
	// The persister writes the restored points meanwhile: a metric it wrote
	// first is skipped as cached, never read twice into the cache.
	if stats.Prewarmed == 0 || stats.Prewarmed+stats.PrewarmSkipped != 3 || stats.PrewarmFailed != 0 {
		t.Fatalf("prewarm counters %+v\n%s", stats, logs)
	}
	waitMigration(t, func() bool { return next.Cache.IsEmpty() })
	for _, name := range names {
		snapshot, err := next.MetricStore.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range snapshot.Archives[0].Points {
			found = found || p.Timestamp == now-5 && p.Value == 2
		}
		if !found {
			t.Fatalf("%s: restored point lost", name)
		}
	}
}

func TestMetadataPrewarmWithoutDump(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := sharedAppConfig(t)
	cfg.Dump.Enabled = false
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "input-log")
	cfg.InputLog.SyncInterval = Duration{time.Minute}
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "catalog.only", Retentions: []store.Retention{{Step: 60, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	defer store.SetPrewarmBeforeFillForTest(func(string) { close(entered); <-release })()
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { once.Do(func() { close(release) }); app.Stop() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("prewarm did not start after input readiness")
	}
	// Exercise the real receiver and durable intake while writeout waits.
	var addr string
	for _, r := range app.Receivers {
		if receiver, ok := r.Receiver.(*tcp.TCP); ok {
			addr = receiver.Addr().String()
		}
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if _, err := fmt.Fprintf(conn, "catalog.only 7 %d\ncatalog.only 8 %d\n", now-120, now-60); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, func() bool { return app.Cache.Size() == 2 })
	log := app.inputLog.Load()
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if metrics := log.Metrics(); metrics.PointsWritten != 2 || metrics.Syncs == 0 || metrics.WriteErrors != 0 {
		t.Fatalf("input log blocked behind prewarm: %+v", metrics)
	}
	if app.Cache.Size() != 2 {
		t.Fatal("persister drained while prewarm gate was held")
	}
	once.Do(func() { close(release) })
	done := app.metadataPrewarmDone.Load()
	if done == nil {
		t.Fatal("no catalog prewarm without dump")
	}
	select {
	case <-*done:
	case <-time.After(time.Second):
		t.Fatal("catalog prewarm did not finish")
	}
	waitMigration(t, func() bool { return app.Cache.IsEmpty() })
	stats := app.MetricStore.Stats().StateCache
	if stats.Prewarmed != 1 || stats.PrewarmBytes == 0 || stats.PrewarmNanos == 0 {
		t.Fatalf("prewarm %+v", stats)
	}
	snapshot, err := app.MetricStore.Snapshot(context.Background(), "catalog.only")
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[float64]bool)
	for _, point := range snapshot.Archives[0].Points {
		found[point.Value] = true
	}
	if !found[7] || !found[8] {
		t.Fatal("gated receiver points lost", snapshot.Archives[0].Points)
	}
}

func TestMetadataPrewarmWaitBudget(t *testing.T) {
	for _, budget := range []string{"points", "bytes", "unlimited"} {
		t.Run(budget, func(t *testing.T) {
			core := cache.New()
			var maxSize, maxBytes uint64
			if budget == "points" {
				maxSize = 4
			}
			if budget == "bytes" {
				maxBytes = uint64(core.EstimatedBytes()) + 1
				core.SetMaxBytes(maxBytes)
			}
			full := metadataPrewarmFull(core, maxSize, maxBytes)
			if full() {
				t.Fatal("empty cache full")
			}
			core.Add(points.OnePoint("pending", 1, 1))
			if full() != (budget == "bytes") {
				t.Fatal("wrong first-point byte threshold")
			}
			core.Add(points.OnePoint("pending", 2, 2))
			if full() != (budget != "unlimited") {
				t.Fatal("wrong half-cache threshold")
			}
			core.Stop()
		})
	}
}

func TestMetadataPrewarmWaitsForFullCatalog(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	db, err := store.Open(t.TempDir(), store.Options{MetadataCacheSize: 4 << 20, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"pending", "remaining"} {
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 100}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	db.ForgetMetadataCacheForTest()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	restore := store.SetPrewarmBeforeFillForTest(func(name string) {
		if name == "remaining" {
			close(entered)
			<-release
		}
	})
	defer restore()
	ready, gate := make(chan struct{}), make(chan struct{})
	close(ready)
	names := func(yield func(string) bool) { yield("pending") }
	done := startMetadataPrewarm(db, whisperConfig{StoreMetadataCachePrewarm: true, StoreMetadataCacheWarmFirst: new(bool), StoreMetadataCachePrewarmWorkers: 1}, "test", names, ready, nil, func() { close(gate) }, nil)
	defer func() { once.Do(func() { close(release) }); <-done }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remaining catalog did not start")
	}
	select {
	case <-gate:
		t.Fatal("gate released after pending pass before full catalog")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full catalog did not finish")
	}
	select {
	case <-gate:
	default:
		t.Fatal("full catalog did not release gate")
	}
}

func TestMetadataPrewarmStopWhileGated(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Dump.Enabled = false
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	// Hold the scan in its cancellable pacer rather than a blocking test hook.
	cfg.Whisper.StoreMetadataCachePrewarmByteRate = 1
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "paced", Retentions: []store.Retention{{Step: 60, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
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
	defer app.Stop()
	select {
	case <-app.metadataPrewarmGate:
		t.Fatal("paced scan already released gate")
	default:
	}
	stopped := make(chan struct{})
	go func() { app.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown waited for prewarm or gate deadline")
	}
	if done := app.metadataPrewarmDone.Load(); done == nil {
		t.Fatal("no prewarm started")
	} else {
		select {
		case <-*done:
		case <-time.After(time.Second):
			t.Fatal("shutdown left prewarm running")
		}
	}
}

func TestMetadataPrewarmInputLogSafety(t *testing.T) {
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	cfg.InputLog.Enabled = true
	cfg.InputLog.MaxAge = Duration{}
	cfg.Cache.WriteoutMaxDelay = Duration{960 * time.Second}
	cfg.Whisper.StoreMetadataCachePrewarmWaitCap = &Duration{30 * time.Minute}
	if got := metadataPrewarmLimit(cfg); got != 480*time.Second {
		t.Fatal(got)
	}
	cfg.Cache.WriteoutMaxDelay = Duration{1800 * time.Second}
	if got := metadataPrewarmLimit(cfg); got != 900*time.Second {
		t.Fatal(got)
	}
	cfg.InputLog.MaxAge = Duration{1900 * time.Second}
	if got := metadataPrewarmLimit(cfg); got != 0 {
		t.Fatal("unsafe gate remained armed", got)
	}
	cfg.Whisper.StoreMetadataCachePrewarmCPUCores = -1
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("negative CPU allowance accepted")
	}
}

func TestMetadataPrewarmCPUAllowsInput(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Dump.Enabled = false
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "input-log")
	cfg.InputLog.SyncInterval = Duration{time.Minute}
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	cfg.Whisper.StoreMetadataCachePrewarmCPUCores = .000001
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "cpu.paced", Retentions: []store.Retention{{Step: 1, Count: 600}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
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
	defer app.Stop()
	var addr string
	for _, r := range app.Receivers {
		if receiver, ok := r.Receiver.(*tcp.TCP); ok {
			addr = receiver.Addr().String()
		}
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "cpu.paced 7 %d\n", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, func() bool { return app.Cache.Size() == 1 })
	log := app.inputLog.Load()
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if log.Metrics().PointsWritten != 1 {
		t.Fatal("input log waited for CPU allowance")
	}
	select {
	case <-app.metadataPrewarmGate:
		t.Fatal("CPU-gated scan already finished")
	default:
	}
	done := make(chan struct{})
	go func() { app.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CPU allowance blocked shutdown")
	}
}

func TestMetadataPrewarmCheckpointMatchesReads(t *testing.T) {
	testMetadataPrewarmCheckpointMatchesReads(t, false)
}

func TestMetadataPrewarmLegacyCheckpointMatchesReads(t *testing.T) {
	testMetadataPrewarmCheckpointMatchesReads(t, true)
}

func testMetadataPrewarmCheckpointMatchesReads(t *testing.T, legacy bool) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := sharedAppConfig(t)
	cfg.Dump.Enabled = false
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	checkpoint := true
	cfg.Whisper.StoreMetadataCacheCheckpoint = &checkpoint
	cfg.Whisper.StoreMetadataCacheCheckpointLazy = &legacy
	cfg.Whisper.StoreMetadataCachePrewarmCPUCores = 1
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{MetadataCacheSize: 4 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	const name = "checkpoint.read"
	stamp := time.Now().Unix() - 2
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: stamp, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if legacy {
		checkpointPath := filepath.Join(cfg.Whisper.StoreDir, "metadata-cache-v1")
		data, err := os.ReadFile(checkpointPath)
		if err != nil {
			t.Fatal(err)
		}
		copy(data[:8], "GCSTATE1")
		binary.LittleEndian.PutUint32(data[48:52], crc32.ChecksumIEEE(data[:48]))
		if err := os.WriteFile(checkpointPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	done := app.metadataPrewarmDone.Load()
	if done == nil {
		t.Fatal("no metadata checkpoint load")
	}
	select {
	case <-*done:
	case <-time.After(time.Second):
		t.Fatal("checkpoint load blocked")
	}
	logs := zapwriter.TestString()
	field := "fenced"
	if legacy {
		field = "validated"
	}
	if !logHas(logs, "valid", "true") || !logHas(logs, field, "1") {
		t.Fatal("checkpoint did not validate", logs)
	}
	snap, err := app.MetricStore.Snapshot(context.Background(), name)
	if err != nil || len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Value != 42 {
		t.Fatal(snap, err)
	}
}

func TestMetadataPrewarmCheckpointSavePressure(t *testing.T) {
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCacheWarmFirst = new(bool)
	checkpoint := true
	cfg.Whisper.StoreMetadataCacheCheckpoint = &checkpoint
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Cache.MaxSize = 2
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{MetadataCacheSize: 4 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "pressure", Retentions: []store.Retention{{Step: 1, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	core := cache.New()
	defer core.Stop()
	core.SetMaxSize(2)
	core.Add(points.OnePoint("pressure", 1, 1))
	app := &App{Config: cfg, Cache: core, MetricStore: db}
	app.prepareMetadataCheckpoint(zap.NewNop())
	if err := db.MetadataCheckpointError(); !errors.Is(err, context.Canceled) {
		t.Fatal("snapshot held pending points under pressure", err)
	}
	core.Add(points.OnePoint("pressure", 2, 2))
	if core.Size() != 2 {
		t.Fatal("intake stopped after snapshot cancellation")
	}
}

func TestMetadataCheckpointDefaults(t *testing.T) {
	conf := whisperConfig{StoreMetadataCachePrewarm: true, StoreMetadataCacheSize: 1 << 20}
	if !metadataCheckpointEnabled(conf) {
		t.Fatal("default checkpoint disabled")
	}
	enabled := false
	conf.StoreMetadataCacheCheckpoint = &enabled
	if metadataCheckpointEnabled(conf) {
		t.Fatal("checkpoint opt-out ignored")
	}
	enabled = true
	conf.StoreMetadataCacheCheckpoint = &enabled
	if !metadataCheckpointEnabled(conf) {
		t.Fatal("explicit checkpoint ignored")
	}
	conf.StoreMetadataCachePrewarm = false
	if metadataCheckpointEnabled(conf) {
		t.Fatal("checkpoint enabled without prewarm")
	}
}

func TestMetadataWarmFirstDefaultsKeepWriteoutRunning(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Dump.Enabled = false
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCachePrewarmCPUCores = 4
	cfg.Cache.WriteoutMinPoints = 1
	cfg.Cache.WriteoutMaxDelay = Duration{time.Second}
	cfg.Cache.WriteoutStartupSpread = false
	cfg.Cache.WriteoutRestoreSpread = false
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if !metadataCheckpointEnabled(app.Config.Whisper) || !metadataWarmFirst(app.Config.Whisper) || metadataPrewarmLimit(app.Config) != 0 {
		t.Fatal("warm-first defaults not enabled")
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	stamp := time.Now().Unix() - 2
	app.Cache.Add(points.OnePoint("warm.first", 42, stamp))
	waitMigration(t, func() bool { return app.Cache.IsEmpty() })
	snapshot, err := app.MetricStore.Snapshot(context.Background(), "warm.first")
	if err != nil || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 42 {
		t.Fatal("ungated default lost input", snapshot, err)
	}
}
