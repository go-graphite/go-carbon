package carbonserver

import (
	"context"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

func TestMetricStoreRefreshOnlyNewNames(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := NewCarbonserverListener(nil)
	l.logger = zap.NewNop()
	l.SetTrieIndex(true)
	l.SetMaxGlobs(100)
	l.SetMetricStore(db)
	create := func(name string) {
		t.Helper()
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	create("old.metric")
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	initial := l.CurrentFileIndex()
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if l.CurrentFileIndex().trieIdx != initial.trieIdx {
		t.Fatal("unchanged catalog rebuilt")
	}
	create("new.metric")
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if l.CurrentFileIndex().trieIdx != initial.trieIdx {
		t.Fatal("new metric rebuilt the whole index")
	}
	assertStoreGlob(t, l, "new.metric", true)
	if err := db.Delete(ctx, "old.metric"); err != nil {
		t.Fatal(err)
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if l.CurrentFileIndex().trieIdx == initial.trieIdx {
		t.Fatal("deletion must rebuild")
	}
	assertStoreGlob(t, l, "old.metric", false)
}

func TestMetricStoreRefreshRebuildsChangedPolicy(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(t.TempDir(), store.Options{SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := NewCarbonserverListener(nil)
	l.logger = zap.NewNop()
	l.SetTrieIndex(true)
	l.SetMetricStore(db)
	config := store.MetricConfig{Name: "changed.metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	initial := l.CurrentFileIndex()
	config.Retentions = []store.Retention{{Step: 1, Count: 120}}
	if _, err := db.Replace(ctx, store.Snapshot{Metadata: store.Metadata{MetricConfig: config}, Archives: []store.Archive{{Retention: config.Retentions[0]}}}); err != nil {
		t.Fatal(err)
	}
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if l.CurrentFileIndex().trieIdx == initial.trieIdx || l.CurrentFileIndex().details[config.Name].Size != 16+12+120*12 {
		t.Fatal("policy change left old logical size")
	}
}
