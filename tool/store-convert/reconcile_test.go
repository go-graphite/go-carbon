package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestReconcileRefreshesLiveCopyAndDeletes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source, copyRoot, dest := filepath.Join(root, "source"), filepath.Join(root, "copy"), filepath.Join(root, "exact")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	source, err := physicalPath(source)
	if err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(dest, chunkstore.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conv := converter{db: db, source: source, copyRoot: copyRoot, verify: true, live: true}
	now := int(time.Now().Unix())
	write := func(name string, input []*whisper.TimeSeriesPoint) {
		t.Helper()
		path := filepath.Join(source, name+".wsp")
		w, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true})
		if os.IsNotExist(err) {
			w, err = whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:1h,60s:1d"), whisper.Average, 0.5, &whisper.Options{Compressed: true, OutOfOrder: true})
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany(input); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write("metric", []*whisper.TimeSeriesPoint{{Time: now - 60, Value: 1}, {Time: now - 59, Value: 2}})
	job := entry{Metric: "metric", Path: "metric.wsp"}
	if _, _, err := conv.convert(ctx, job); err != nil {
		t.Fatal(err)
	}
	write("deleted", []*whisper.TimeSeriesPoint{{Time: now - 60, Value: 8}})
	if _, _, err := conv.convert(ctx, entry{Metric: "deleted", Path: "deleted.wsp"}); err != nil {
		t.Fatal(err)
	}
	// A live resume verifies the original captured copy, which is deliberately
	// stale. Final reconciliation must refresh both files before replacing it.
	write("metric", []*whisper.TimeSeriesPoint{{Time: now - 60, Value: 111.25}, {Time: now - 58, Value: 3}})
	write("created", []*whisper.TimeSeriesPoint{{Time: now - 60, Value: 9}})
	if err := os.Remove(filepath.Join(source, "deleted.wsp")); err != nil {
		t.Fatal(err)
	}
	conv.live, conv.reconcile = false, true
	conv.visited = &visitedMetrics{}
	if err := walk(source, func(e entry) error { _, _, err := conv.convert(ctx, e); return err }); err != nil {
		t.Fatal(err)
	}
	deleted, err := conv.reconcileDeletions(ctx)
	if err != nil || deleted != 1 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if got := conv.deletionChecksSkipped.Load(); got != 2 {
		t.Fatalf("successful fenced visits skipped %d checks, want 2", got)
	}
	if _, err := db.Metadata(ctx, "deleted"); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("deleted metric survived: %v", err)
	}
	// Repeating after interruption is safe and uses the current full source.
	if err := walk(source, func(e entry) error { _, _, err := conv.convert(ctx, e); return err }); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metric", "created"} {
		want, err := whisperio.ReadSnapshot(ctx, name, filepath.Join(source, name+".wsp"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := db.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := equalSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReconcileRejectsIdentityChangesAndNoncanonicalNames(t *testing.T) {
	dir := t.TempDir()
	previous := identity{Source: "/source", Copy: "/copy", SourceID: "immutable-id", Destination: dir, Live: true}
	data, _ := json.Marshal(previous)
	if err := os.WriteFile(filepath.Join(dir, "CONVERSION.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	requested := previous
	requested.Live = false
	if got, err := reconciliationIdentity(dir, requested); err != nil || got != previous {
		t.Fatalf("baseline identity not retained: %+v %v", got, err)
	}
	requested.SourceID = "another-dataset"
	if _, err := reconciliationIdentity(dir, requested); err == nil {
		t.Fatal("different source identity adopted")
	}
	for _, e := range []entry{{"a.b", "a.b.wsp"}, {"a;b=x", "a;b=x.wsp"}, {"a..b", "a//b.wsp"}, {"a.b", "../a/b.wsp"}, {"a/b", "a/b.wsp"}} {
		if canonicalEntry(e) {
			t.Fatalf("noncanonical mapping accepted: %+v", e)
		}
	}
	if !canonicalEntry(entry{"a.b", "a/b.wsp"}) {
		t.Fatal("canonical mapping rejected")
	}
}
