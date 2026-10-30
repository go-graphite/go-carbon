package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestCatalogPageDoesNotReadRevisions(t *testing.T) {
	ctx := context.Background()
	s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	for _, name := range []string{"page.a", "page.b", "other.c"} {
		createTestMetric(t, s, name)
		if err := s.UpdateMany(ctx, name, []Point{{Timestamp: int64(now), Value: 42}}); err != nil {
			t.Fatal(err)
		}
	}
	full, err := s.ListPage(ctx, "page.", "", 10)
	if err != nil || len(full) != 2 {
		t.Fatalf("full page: %v %v", full, err)
	}
	for i, after := range []string{"", "page.a"} {
		page, err := s.ListCatalogPage(ctx, "page.", after, 1)
		if err != nil || len(page) != 1 || !reflect.DeepEqual(page[0].MetricConfig, full[i].MetricConfig) || page[0].ID != full[i].ID || page[0].Generation != full[i].Generation {
			t.Fatalf("catalog page after %q: %v %v", after, page, err)
		}
	}
	// A discovery traversal must not consult mutable revision keys at all.
	if err := s.db.Delete(revisionKey(full[0]), nil); err != nil {
		t.Fatal(err)
	}
	if page, err := s.ListCatalogPage(ctx, "page.", "", 10); err != nil || len(page) != 2 {
		t.Fatalf("catalog depends on revisions: %v %v", page, err)
	}
	if _, err := s.ListPage(ctx, "page.", "", 10); err == nil {
		t.Fatal("conditional page ignored missing revision")
	}
}

func TestCatalogChangesFollowCommittedMutations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir(), Options{SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, cursor, reset, err := db.CatalogChanges(ctx, 0)
	if err != nil || !reset {
		t.Fatalf("initial cursor %d reset %v: %v", cursor, reset, err)
	}
	config := MetricConfig{Name: "new.metric", Retentions: []Retention{{Step: 1, Count: 60}}, AggregationMethod: Average}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	changes, next, reset, err := db.CatalogChanges(ctx, cursor)
	if err != nil || reset || len(changes) != 1 || changes[0].Name != config.Name {
		t.Fatalf("create: %v %v %v", changes, reset, err)
	}
	if err := db.UpdateMany(ctx, config.Name, []Point{{Timestamp: time.Now().Unix() - 1, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	changes, cursor, reset, err = db.CatalogChanges(ctx, next)
	if err != nil || reset || len(changes) != 0 || cursor != next {
		t.Fatalf("point update entered journal: %v %v %v", changes, reset, err)
	}
	if _, err := db.Create(ctx, config); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if err := db.Delete(ctx, config.Name); err != nil {
		t.Fatal(err)
	}
	_, _, reset, err = db.CatalogChanges(ctx, cursor)
	if err != nil || !reset {
		t.Fatalf("delete must rebuild: reset %v %v", reset, err)
	}
}

func TestCatalogChangesBoundAndDeduplicate(t *testing.T) {
	ctx := context.Background()
	db, err := Open(t.TempDir(), Options{SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, cursor, _, _ := db.CatalogChanges(ctx, 0)
	config := MetricConfig{Name: "new.metric", Retentions: []Retention{{Step: 1, Count: 60}}, AggregationMethod: Average}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		db.catalogChanged(config.Name)
	}
	changes, _, reset, err := db.CatalogChanges(ctx, cursor)
	if err != nil || reset || len(changes) != 1 {
		t.Fatalf("duplicates: %v %v %v", changes, reset, err)
	}
	c := db.catalogChanges.Load()
	c.mu.Lock()
	c.bytes = catalogChangeBytes
	c.mu.Unlock()
	db.catalogChanged(fmt.Sprint("overflow.metric"))
	_, _, reset, err = db.CatalogChanges(ctx, cursor)
	if err != nil || !reset {
		t.Fatalf("overflow must rebuild: %v %v", reset, err)
	}
}

func TestCatalogChangesTrackFileOwnership(t *testing.T) {
	ctx := context.Background()
	db := openFileMigrationStore(t, vfs.NewMem())
	defer db.Close()
	_, cursor, _, err := db.CatalogChanges(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := fileMigrationSnapshot()
	staged, err := db.StageFileSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	_, cursor, reset, err := db.CatalogChanges(ctx, cursor)
	if err != nil || !reset {
		t.Fatalf("unowned stage: reset=%v %v", reset, err)
	}
	owned, err := db.PublishFileOwnership(ctx, snapshot, staged)
	if err != nil {
		t.Fatal(err)
	}
	entries, cursor, reset, err := db.CatalogChanges(ctx, cursor)
	if err != nil || reset || len(entries) != 1 || entries[0].FileMigration != FileCleanupPending {
		t.Fatalf("published stage: %v reset=%v %v", entries, reset, err)
	}
	if err := db.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatal(err)
	}
	entries, _, reset, err = db.CatalogChanges(ctx, cursor)
	if err != nil || reset || len(entries) != 1 || entries[0].FileMigration != FileReclaimed {
		t.Fatalf("reclaimed: %v reset=%v %v", entries, reset, err)
	}
}
