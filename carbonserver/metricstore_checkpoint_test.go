package carbonserver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

func TestMetricStoreCheckpointPreservesCatalogAndPendingNames(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped=%v", mapped), func(t *testing.T) {
			db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Create(context.Background(), store.MetricConfig{Name: "stored.metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
			root, cache := t.TempDir(), filepath.Join(t.TempDir(), "files.gzip")
			listener := func() *CarbonserverListener {
				l := NewCarbonserverListener(nil)
				l.logger = zap.NewNop()
				l.SetWhisperData(root)
				l.SetMetricStore(db)
				l.SetTrieIndex(true)
				l.SetConcurrentIndex(true)
				l.SetFileListCache(cache)
				l.SetFileListCacheVersion(2)
				l.SetScanFrequency(time.Hour)
				t.Cleanup(func() { _ = l.Stop() })
				return l
			}
			first := listener()
			if !mapped {
				first.SetFileListCacheVersion(1)
			}
			if err := first.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			first.SetFileListCacheVersion(2)
			index := first.CurrentFileIndex()
			if first.HasMappedIndex() != mapped || !first.PrepareReadCheckpoint() || first.HasMappedIndex() != mapped || first.CurrentFileIndex() != index {
				t.Fatal("checkpoint preparation replaced the live catalog index")
			}
			if _, err := db.Create(context.Background(), store.MetricConfig{Name: "after.freeze", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
			if err := first.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			if first.MetricExists("after.freeze") {
				t.Fatal("external catalog refresh changed the frozen checkpoint generation")
			}
			factory, finish := first.WarmCheckpointMetricLookups()
			known := factory()
			if !known("stored.metric") || !known("pending.only") {
				t.Fatal("checkpoint omitted stored or pending names")
			}
			finish()
			id, err := first.CheckpointReadIndex()
			if err != nil || id == "" {
				t.Fatal(id, err)
			}
			overlay, err := readOverlayManifest(overlayManifestPath(cache))
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(2)
			if mapped {
				want = 1
			}
			if overlay.Records != want {
				t.Fatalf("checkpoint overlay has %d names, want %d", overlay.Records, want)
			}
			next := listener()
			next.WarmupIndexWithPending(func(got string, visit func(string) error) error {
				if got != id {
					t.Fatalf("checkpoint ID %q want %q", got, id)
				}
				return visit("arrived.after.warmup")
			})
			next.WaitForWarmup()
			if !next.HasMappedIndex() || next.RecoveryIndexID() != id {
				t.Fatal("catalog restart did not restore the validated checkpoint")
			}
			for _, name := range []string{"stored.metric", "pending.only", "arrived.after.warmup"} {
				if !next.MetricExists(name) {
					t.Fatalf("checkpoint lost %s", name)
				}
			}
			rejected := listener()
			rejected.WarmupIndexWithPending(func(string, func(string) error) error { return errors.New("invalid checkpoint") })
			rejected.WaitForWarmup()
			if rejected.CurrentFileIndex() != nil {
				t.Fatal("invalid checkpoint published a catalog")
			}
			if err := rejected.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			if !rejected.MetricExists("stored.metric") || rejected.MetricExists("pending.only") {
				t.Fatal("fallback did not use the authoritative store catalog")
			}
		})
	}
}
