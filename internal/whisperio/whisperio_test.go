package whisperio

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

func TestClassicRoundTrip(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100_000, 0)
	source, err := chunkstore.Open(filepath.Join(t.TempDir(), "source"), chunkstore.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	config := chunkstore.MetricConfig{Name: "metric", Retentions: []chunkstore.Retention{{Step: 60, Count: 256}}, AggregationMethod: chunkstore.Average}
	if _, err := source.CreateFromSnapshot(ctx, chunkstore.Snapshot{Metadata: chunkstore.Metadata{MetricConfig: config}, Archives: []chunkstore.Archive{{Retention: config.Retentions[0], Points: []chunkstore.Point{{Timestamp: 99900, Value: math.Float64frombits(0x7ff8000000000042)}}}}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "metric.wsp")
	if err := New(source).ExportWSP(ctx, "metric", path); err != nil {
		t.Fatal(err)
	}
	destination, err := chunkstore.Open(filepath.Join(t.TempDir(), "destination"), chunkstore.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if _, err := New(destination).ImportWSP(ctx, "metric", path, false); err != nil {
		t.Fatal(err)
	}
	got, err := destination.Snapshot(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 1 || math.Float64bits(got.Archives[0].Points[0].Value) != 0x7ff8000000000042 {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestCompressedOutOfOrderImport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.wsp")
	retentions := []whisper.Retention{whisper.NewRetention(60, 256)}
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer(retentions), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	pointTime := int(time.Now().Unix()/60) * 60
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: pointTime, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: pointTime - 60, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := chunkstore.Open(filepath.Join(t.TempDir(), "store"), chunkstore.Options{Now: func() time.Time { return time.Unix(100_000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := New(store).ImportWSP(context.Background(), "metric", path, false); err != nil {
		t.Fatal(err)
	}
	got, err := store.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 2 || got.Archives[0].Points[0].Value != 2 || got.Archives[0].Points[1].Value != 3 {
		t.Fatalf("import = %#v", got)
	}
}

func TestImportRejectsTruncatedWhisper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.wsp")
	if err := os.WriteFile(path, []byte("not a whisper file"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := chunkstore.Open(filepath.Join(t.TempDir(), "store"), chunkstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := New(store).ImportWSP(context.Background(), "metric", path, false); err == nil {
		t.Fatal("truncated Whisper imported successfully")
	}
	if _, err := store.Metadata(context.Background(), "metric"); !errors.Is(err, chunkstore.ErrNotFound) {
		t.Fatalf("truncated import published data: %v", err)
	}
}

func TestExportSkipsTimestampTruncation(t *testing.T) {
	for _, timestamp := range []int64{-1, 0, int64(math.MaxUint32) + 1} {
		if _, ok := exportTimestamp(timestamp); ok {
			t.Fatalf("accepted timestamp %d outside the Whisper field", timestamp)
		}
	}
	if _, ok := exportTimestamp(1); !ok {
		t.Fatal("rejected a representable timestamp")
	}
}
