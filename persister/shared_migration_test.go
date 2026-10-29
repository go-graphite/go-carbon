package persister

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestLiveMirrorImportsExistingOriginalBeforePop(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary-rejects", true: "migration-imports"}[enabled], func(t *testing.T) {
			root := t.TempDir()
			cache := &fakeCache{}
			p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
			now := time.Now().Unix()
			cache.add("metric", now-3, 1.25)
			p.store("metric")
			db, err := store.Open(filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			p.SetMetricStore(db)
			p.SetMirrorFiles(true)
			p.SetMirrorImportMissing(enabled)
			cache.add("metric", now-1, .1)
			p.store("metric")
			if !enabled {
				if cache.confirmedPoints() != 1 {
					t.Fatal("ordinary mirrored writer confirmed an unconverted original")
				}
				if _, err = db.Metadata(context.Background(), "metric"); !errors.Is(err, store.ErrNotFound) {
					t.Fatal("ordinary writer imported the old original")
				}
				return
			}
			if cache.confirmedPoints() != 2 {
				t.Fatal("migration lost or failed to confirm the new batch")
			}
			source, err := whisperio.ReadSnapshot(context.Background(), "metric", filepath.Join(root, "original", "metric.wsp"))
			if err != nil {
				t.Fatal(err)
			}
			actual, err := db.Snapshot(context.Background(), "metric")
			if err != nil {
				t.Fatal(err)
			}
			if err = store.EqualSnapshots(source, actual); err != nil {
				t.Fatal(err)
			}
			if len(actual.Archives[0].Points) != 2 {
				t.Fatal("import failed to retain the old archive point")
			}
		})
	}
}

func TestLiveMirrorAuditDeletesOriginalVanishedAfterWalk(t *testing.T) {
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	db, err := store.Open(filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	cache.add("metric", time.Now().Unix()-2, 3.25)
	p.store("metric")
	metadata, _, err := p.ReconcileMirrorMetric(context.Background(), "metric", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(root, "original", "metric.wsp")); err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.ReconcileMirrorMetric(context.Background(), "metric", metadata.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Metadata(context.Background(), "metric"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("visited ID hid a source deletion: %v", err)
	}
}

func TestLiveMirrorRepairsPolicyChangedAfterBaseline(t *testing.T) {
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	now := time.Now().Unix()
	cache.add("metric", now-3, 1.25)
	p.store("metric")
	db, err := store.Open(filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stale := store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Sum, XFilesFactor: .9}
	if _, err = db.Create(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	p.SetMirrorImportMissing(true)
	cache.add("metric", now-1, .1)
	p.store("metric")
	if cache.confirmedPoints() != 2 {
		t.Fatal("live migration stalled a write behind stale baseline policy")
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "metric", filepath.Join(root, "original", "metric.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
}

func TestLiveMirrorRecreatedOriginalDropsObsoleteSharedHistory(t *testing.T) {
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	db, err := store.Open(filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	now := time.Now().Unix()
	cache.add("metric", now-3, 1.25)
	p.store("metric")
	if err = os.Remove(filepath.Join(root, "original", "metric.wsp")); err != nil {
		t.Fatal(err)
	}
	p.SetMirrorImportMissing(true)
	cache.add("metric", now-1, 73.5)
	p.store("metric")
	if cache.confirmedPoints() != 2 {
		t.Fatal("recreated original stalled new writes")
	}
	actual, err := db.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Archives[0].Points) != 1 || actual.Archives[0].Points[0].Value != 73.5 {
		t.Fatalf("obsolete source generation survived recreation: %+v", actual.Archives[0].Points)
	}
}

// A recreated row starts at generation/revision 1 again. Its new ID must not
// acquire the first baseline's trust merely because the original stayed put.
func TestInterruptedBaselineRecreatedIDGetsFullVerification(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	cache.add("metric", time.Now().Unix()-3, 1.25)
	p.store("metric")
	source := filepath.Join(root, "original", "metric.wsp")
	// Initialize the complete main file so native XFS can prove shared extents.
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(source, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(root, "baseline", "metric.wsp")
	if err = os.MkdirAll(filepath.Dir(capture), 0700); err != nil {
		t.Fatal(err)
	}
	if err = whisperio.Capture(ctx, source, capture, false); err != nil {
		t.Fatal(err)
	}
	same, err := whisperio.SameCapture(ctx, source, capture)
	if err != nil {
		t.Fatal(err)
	}
	if !same {
		t.Skip("positive shared-extent proof requires native reflink filesystem")
	}
	want, err := whisperio.ReadSnapshot(ctx, "metric", source)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old, err := db.CreateFromSnapshot(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Delete(ctx, "metric"); err != nil {
		t.Fatal(err)
	}
	stale := want
	stale.Archives = append([]store.Archive(nil), want.Archives...)
	stale.Archives[0].Points = append([]store.Point(nil), want.Archives[0].Points...)
	stale.Archives[0].Points[0].Value = 999
	fresh, err := db.CreateFromSnapshot(ctx, stale)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ID <= old.ID || fresh.Generation != 1 || fresh.Revision != 1 {
		t.Fatalf("invalid recreation fixture: %+v -> %+v", old, fresh)
	}
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	p.SetMirrorState(&MirrorState{BaselineCopy: filepath.Dir(capture), BaselineIDLimit: old.ID})
	if _, _, err = p.ReconcileMirrorMetric(ctx, "metric", 0); err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	if p.mirrorState.BaselineSkipped.Load() != 0 {
		t.Fatal("recreated ID reused old verification")
	}
}
