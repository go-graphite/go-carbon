package carbonserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestMirroredStorePreservesMappedFileIndexAndSharedReads(t *testing.T) {
	t.Parallel()
	cachePath, root, entries := snapshotFixture(t)
	predecessor := overlayListener(t, cachePath, root)
	checkpointID, err := predecessor.CheckpointReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	listener := unstartedOverlayListener(t, cachePath, root)
	listener.cacheGet = cache.New().Get
	now := time.Now().Unix()
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	metric := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(entries[0].Path, "/"), ".wsp"), "/", ".")
	for _, name := range []string{metric, "catalog_only"} {
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, name, []store.Point{{Timestamp: now - 2, Value: 7}}); err != nil {
			t.Fatal(err)
		}
	}
	listener.SetMetricStore(db)
	listener.SetMetricStoreFileIndex(true)
	listener.WarmupIndex()
	listener.WaitForWarmup()
	if !listener.HasMappedIndex() {
		t.Fatal("mirrored canary did not reuse the mapped file index")
	}
	id := listener.RecoveryIndexID()
	if id == "" || id != checkpointID {
		t.Fatal("mapped checkpoint identity missing")
	}
	if !listener.MetricExists(metric) || listener.MetricExists("catalog_only") {
		t.Fatal("discovery is not using the original file index")
	}
	series, err := listener.fetchFromDisk(metric, int32(now-5), int32(now))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, value := range series.Timeseries.Values() {
		if value == 7 {
			found = true
		}
	}
	if !found {
		t.Fatal("render did not read shared values")
	}
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if !listener.HasMappedIndex() || listener.RecoveryIndexID() != id || listener.MetricExists("catalog_only") {
		t.Fatal("catalog refresh replaced the mirrored file index")
	}
}

func TestMirroredHandoffDrainsSharedReadsBeforeReleasingStore(t *testing.T) {
	t.Parallel()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	listener := NewCarbonserverListener(cache.New().Get)
	listener.SetMetricStore(db)
	if err := listener.UseMirroredFilesForHandoff(); err == nil || listener.getMetricStore() != db {
		t.Fatal("unmirrored store was released")
	}
	listener.SetMetricStoreFileIndex(true)
	accepted, locked := listener.beginSharedStoreRequest()
	if !accepted || !locked {
		t.Fatal("shared read was not admitted")
	}
	done := make(chan error, 1)
	go func() { done <- listener.UseMirroredFilesForHandoff() }()
	select {
	case err := <-done:
		t.Fatalf("released store with a shared read in flight: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if listener.getMetricStore() != db {
		t.Fatal("in-flight reader lost its store")
	}
	listener.endSharedStoreRequest(locked)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mirrored handoff did not drain")
	}
	if listener.getMetricStore() != nil {
		t.Fatal("new reader can still acquire the released store")
	}
	if accepted, locked := listener.beginSharedStoreRequest(); !accepted || locked {
		t.Fatal("mirrored file reads are unavailable after the store drained")
	}
}
