package carbonserver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

// The store accepts names up to 1 MiB, but the mapped discovery index writes
// file-list cache runs whose reader rejects paths above flcv2MaxPathLen. One
// such name used to fail every rebuild, so no new name became discoverable.
func TestMetricStoreMappedRebuildSkipsUnindexableNames(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l, _ := newScanTestListener(root, filepath.Join(t.TempDir(), "files.gz"), 1)
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	l.SetEstimateSize(nil)
	l.SetIncrementalIndexScratch(t.TempDir())
	l.SetMetricStoreFileIndex(false)
	defer l.Stop()

	long := "payments.by_uri." + strings.Repeat("queryParam-x_", 8192/13+1) + "count"
	trailing := "trailing.dot."
	for _, name := range []string{"ok.before", long, trailing, "ok.after"} {
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatalf("rebuild failed on an unindexable name: %v", err)
	}
	if !l.HasMappedIndex() {
		t.Fatal("expected the mapped discovery index")
	}
	for _, name := range []string{"ok.before", "ok.after"} {
		if !l.MetricExists(name) {
			t.Fatalf("indexable name %q missing", name)
		}
	}
	if l.MetricExists(long) {
		t.Fatal("over-long name entered the mapped index")
	}
	if records := l.CurrentFileIndex().trieIdx.snapshot.manifest.Records; records != 2 {
		t.Fatalf("mapped records = %d, want 2", records)
	}

	// Realtime inserts follow the same rule, so the overlay never holds a
	// name the next rebuild drops, nor checkpoints an unreadable path.
	trie := l.CurrentFileIndex().trieIdx
	if node := l.insertRealtimeMetric(trie, long); node != nil {
		t.Fatal("realtime index accepted an over-long name")
	}
	if l.pendingMetricStorable(long) || l.pendingMetricStorable(trailing) {
		t.Fatal("pending recovery accepted an unindexable name")
	}
	if node := l.insertRealtimeMetric(trie, "ok.realtime"); node == nil {
		t.Fatal("realtime index rejected an ordinary name")
	}

	// Later names still become discoverable after the next full rebuild.
	if _, err := db.Create(ctx, store.MetricConfig{Name: "ok.later", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Sum}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(ctx, "ok.before"); err != nil {
		t.Fatal(err)
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatalf("second rebuild failed: %v", err)
	}
	if !l.MetricExists("ok.later") || l.MetricExists("ok.before") {
		t.Fatal("full rebuild after a delete did not publish the catalog")
	}
}
