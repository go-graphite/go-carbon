package carbon

import (
	"context"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// A receiver-gating drain must be able to use the saved metadata that makes its
// cold writes fast, without waiting for the input readiness it is itself gating.
func TestOrderedRestoreLoadsMetadataBeforeInputReadiness(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	dir := t.TempDir()
	opts := store.Options{MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const name = "ordered.restore.metadata"
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Metadata(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil || db.MetadataCheckpointError() != nil {
		t.Fatal(err, db.MetadataCheckpointError())
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	app := New("")
	app.Config = NewConfig()
	app.Config.Dump.Path = t.TempDir()
	app.Config.Whisper.StoreMetadataCachePrewarm = true
	app.MetricStore = db
	app.metadataPrewarmReady = make(chan struct{})
	core := cache.New()
	core.SetMaxSize(0)
	app.Cache = core
	core.AddRestoredBeforeInput(points.OnePoint(name, 1, 1700000000))
	p := persister.NewWhisper("", nil, nil, core.WriteoutQueue().Get, core.PopNotConfirmed, core.Confirm, core.Pop)
	p.SetWorkers(1)
	release := make(chan struct{})
	p.SetMockStore(func() (persister.StoreFunc, func()) {
		return func(name string) {
			for !db.MetadataCached(name) {
				select {
				case <-release:
					goto persist
				case <-time.After(time.Millisecond):
				}
			}
		persist:
			batch, ok := core.PopNotConfirmed(name)
			if !ok {
				t.Error("missing restore receipt")
				return
			}
			core.Confirm(batch)
		}, nil
	})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	app.Persister = p
	done := make(chan struct{})
	go func() { app.restoreBeforeReceivers(core); close(done) }()
	defer func() { close(release); <-done; p.Stop(); db.StopBackground() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ordered drain waited for input readiness before loading its metadata checkpoint")
	}
	if !db.MetadataCached(name) {
		t.Fatal("restore bypassed the checkpoint")
	}
	select {
	case <-app.metadataPrewarmReady:
		t.Fatal("test opened the input gate to unblock metadata")
	default:
	}
}

func TestCaptureHandoffPreservesMetadataCheckpoint(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	_, app, core, _ := directCaptureFixture(t)
	app.Cache = core
	app.Config.Cache.MaxSize, app.Config.Cache.MaxBytes = 0, 0
	app.Config.Whisper.StoreMetadataCacheSize = 1 << 20
	app.Config.Whisper.StoreMetadataCachePrewarm = true
	dir := t.TempDir()
	opts := store.Options{MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	app.MetricStore = db
	const name = "capture.checkpoint"
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 100}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Metadata(context.Background(), name); err != nil {
		t.Fatal(err)
	}
	if err := app.stopStreamingInputForHandoff(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.CloseForExit(); err != nil || db.MetadataCheckpointError() != nil {
		t.Fatal(err, db.MetadataCheckpointError())
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
	if err != nil || !stats.Valid || !db.MetadataCached(name) {
		t.Fatal("capture handoff discarded its metadata accelerator", stats, err)
	}
}
