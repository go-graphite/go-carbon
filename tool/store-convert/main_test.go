package main

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestConvertResumesAndRejectsChangedSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	source, err := physicalPath(source)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(source, "metric.wsp")
	retentions := whisper.MustParseRetentionDefs("1s:1h")
	w, err := whisper.Create(path, retentions, whisper.Last, 0)
	if err != nil {
		t.Fatal(err)
	}
	ts := int(time.Now().Unix()) - 10
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: ts, Value: math.Float64frombits(0x8000000000000000)}, {Time: ts - 1, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(filepath.Join(root, "destination"), chunkstore.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conv := converter{db: db, source: source, verify: true}
	job := entry{Metric: "metric", Path: "metric.wsp"}
	for i := 0; i < 2; i++ {
		n, resumed, err := conv.convert(ctx, job)
		if err != nil || n != 2 || resumed != (i == 1) {
			t.Fatalf("pass %d: points=%d resumed=%v err=%v", i, n, resumed, err)
		}
	}
	w, err = whisper.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: ts, Value: 99}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conv.convert(ctx, job); err == nil {
		t.Fatal("resumed a changed immutable source")
	}
	if _, _, err := conv.convert(ctx, entry{Metric: "escape", Path: "../metric.wsp"}); err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestPhysicalPathDetectsSourceAliases(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	source, err := physicalPath(source)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := physicalPath(filepath.Join(link, "not-created", "yet"))
	if err != nil {
		t.Fatal(err)
	}
	if !within(source, resolved) {
		t.Fatalf("missed alias %s", resolved)
	}
}

func TestLiveConverterDiscardsOnlyConfirmedMissingSource(t *testing.T) {
	root := t.TempDir()
	db, err := chunkstore.Open(filepath.Join(root, "store"), chunkstore.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conv := converter{db: db, source: root, live: true}
	job := entry{Metric: "metric", Path: "metric.wsp"}
	config := chunkstore.MetricConfig{Name: job.Metric, Retentions: []chunkstore.Retention{{Step: 1, Count: 60}}, AggregationMethod: chunkstore.Last}
	if _, err := db.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if skipped, err := conv.discardVanished(ctx, job, os.ErrPermission); err != nil || skipped {
		t.Fatalf("permission error hidden: %v %v", skipped, err)
	}
	if err := os.WriteFile(filepath.Join(root, job.Path), []byte("still present"), 0600); err != nil {
		t.Fatal(err)
	}
	if skipped, err := conv.discardVanished(ctx, job, os.ErrNotExist); err != nil || skipped {
		t.Fatalf("existing source discarded: %v %v", skipped, err)
	}
	if err := os.Remove(filepath.Join(root, job.Path)); err != nil {
		t.Fatal(err)
	}
	conv.live = false
	if skipped, err := conv.discardVanished(ctx, job, os.ErrNotExist); err != nil || skipped {
		t.Fatalf("immutable inventory failure hidden: %v %v", skipped, err)
	}
	conv.live = true
	if skipped, err := conv.discardVanished(ctx, job, os.ErrNotExist); err != nil || !skipped {
		t.Fatalf("vanished source not reconciled: %v %v", skipped, err)
	}
	if _, err := db.Metadata(ctx, job.Metric); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("vanished metric retained: %v", err)
	}
}
