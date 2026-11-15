package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Compare actual compressed/classic Whisper images with the complete Snapshot
// oracle, including legacy images without the optional classic seek index.
func TestNativeWindowHistoricalWhisperDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC) }
	const day = 86400
	for _, compressed := range []bool{false, true} {
		for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
			for _, xff := range []float32{0, .5, 1} {
				t.Run(fmt.Sprintf("compressed=%v/method=%d/xff=%g", compressed, method, xff), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "historical.wsp")
					w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1m:30d,1h:1y,1d:10y"), method, xff, &whisper.Options{Compressed: compressed, Now: clock, PointsPerBlock: 16})
					if err != nil {
						t.Fatal(err)
					}
					var points []*whisper.TimeSeriesPoint
					for _, age := range []int{800 * day, 400 * day, 40 * day, 10 * day, day} {
						for i := range 60 {
							v := float64(i) + .125
							if i%7 == 0 {
								v = math.Copysign(0, -1)
							}
							points = append(points, &whisper.TimeSeriesPoint{Time: int(clock().Unix()) - age + i*60, Value: v})
						}
					}
					if err := w.UpdateMany(points); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					meta, err := ReadSnapshot(ctx, "historical", path)
					if err != nil {
						t.Fatal(err)
					}
					codec := &ContinuationCodec{}
					kv := mapContinuationIO{}
					if err := codec.Import(ctx, kv, path, meta.Metadata, clock); err != nil {
						t.Fatal(err)
					}
					raw, err := codec.Snapshot(ctx, kv, meta.Metadata, clock)
					if err != nil {
						t.Fatal(err)
					}
					for _, window := range []store.NativeWindow{
						{From: clock().Unix() - 900*day, Until: clock().Unix() - 700*day},
						{From: clock().Unix() - 900*day, Until: clock().Unix()},
						{From: clock().Unix() - 40*day, Until: clock().Unix() - 39*day + 1},
						{From: clock().Unix() - 4000*day, Until: clock().Unix() - 3999*day},
					} {
						if compressed && window.From == clock().Unix()-900*day {
							// The frozen compressed exporter remains fail-closed;
							// it cannot silently fall back to an unscoped full copy.
							_, _, oldErr := codec.legacyExportWindowSnapshot(ctx, kv, meta.Metadata, clock, window)
							if !errors.Is(oldErr, store.ErrConflict) {
								t.Fatal("legacy exporter unexpectedly accepted history", oldErr)
							}
						}
						want, ws, err := store.SelectNativeWindow(raw, window, clock().Unix())
						if err != nil {
							t.Fatal(err)
						}
						got, gs, err := codec.NativeWindowSnapshot(ctx, kv, meta.Metadata, clock, window)
						if err != nil || !reflect.DeepEqual(ws, gs) {
							t.Fatal(window, ws, gs, err)
						}
						if err := store.EqualSnapshots(want, got); err != nil {
							t.Fatal(window, err)
						}
					}
				})
			}
		}
	}
}
