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
	source, err := openTestStore(filepath.Join(t.TempDir(), "source"), chunkstore.Options{Now: func() time.Time { return now }})
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
	destination, err := openTestStore(filepath.Join(t.TempDir(), "destination"), chunkstore.Options{Now: func() time.Time { return now }})
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
	store, err := openTestStore(filepath.Join(t.TempDir(), "store"), chunkstore.Options{Now: func() time.Time { return time.Unix(100_000, 0) }})
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
	store, err := openTestStore(filepath.Join(t.TempDir(), "store"), chunkstore.Options{})
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

func TestCompressedBufferedRollupImportMatchesFetch(t *testing.T) {
	const epoch = 1700006400
	previous := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(epoch, 0) }
	defer func() { whisper.Now = previous }()
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		t.Run(method.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.wsp")
			rets := whisper.MustParseRetentionDefs("1s:10m,10s:1h,60s:1d")
			w, err := whisper.CreateWithOptions(path, rets, method, 0.5, &whisper.Options{Compressed: true, OutOfOrder: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, interval := range [][2]int{{epoch - 59, epoch - 2}, {epoch - 12, epoch - 7}} {
				var input []*whisper.TimeSeriesPoint
				for ts := interval[0]; ts <= interval[1]; ts++ {
					input = append(input, &whisper.TimeSeriesPoint{Time: ts, Value: float64(ts % 13)})
				}
				if err := w.UpdateMany(input); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = whisper.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			db, err := openTestStore(filepath.Join(t.TempDir(), "destination"), chunkstore.Options{ExactBlocks: true, Now: whisper.Now})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			captured := filepath.Join(t.TempDir(), "copy.wsp")
			if err := Capture(context.Background(), path, captured, false); err != nil {
				t.Fatal(err)
			}
			if err := Capture(context.Background(), path, path, false); err == nil {
				t.Fatal("self capture accepted")
			}
			snapshot, err := ReadSnapshot(context.Background(), "metric", captured)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.CreateFromSnapshot(context.Background(), snapshot); err != nil {
				t.Fatal(err)
			}
			for _, retention := range rets {
				from := epoch - retention.MaxRetention() + 1
				want, err := w.Fetch(from, epoch)
				if err != nil {
					t.Fatal(err)
				}
				got, err := db.Fetch(context.Background(), "metric", from, epoch)
				if err != nil {
					t.Fatal(err)
				}
				if want.FromTime() != got.FromTime || want.Step() != got.Step || len(want.Values()) != len(got.Values) {
					t.Fatal("different query grid")
				}
				for i, value := range want.Values() {
					if math.IsNaN(value) && math.IsNaN(got.Values[i]) {
						continue
					}
					if math.Float64bits(value) != math.Float64bits(got.Values[i]) {
						t.Fatalf("step=%d timestamp=%d source=%g destination=%g", got.Step, got.FromTime+i*got.Step, value, got.Values[i])
					}
				}
			}
		})
	}
}
