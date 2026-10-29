package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationEmptyUpdateMaintainsSidecar(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			t.Run(fmt.Sprintf("v7=%v/invalid=%v", buckets, invalid), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1792432631, 0)
				clock := func() time.Time { return now }
				root := t.TempDir()
				codec := &ContinuationCodec{}
				options := store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: codec,
					Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
				db, err := store.Open(filepath.Join(root, "shared"), options)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if db != nil {
						db.Close()
					}
				}()
				if _, err := db.Create(ctx, store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 1, Count: 60}, {Step: 5, Count: 120}},
					AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(root, "oracle.wsp")
				w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:1m,5s:10m"), whisper.Average, .5,
					&whisper.Options{Compressed: true, OutOfOrder: true, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				var seed []store.Point
				for age := int64(18); age > 0; age-- {
					seed = append(seed, store.Point{Timestamp: now.Unix() - age, Value: float64(age) + .1})
				}
				for _, batch := range [][]store.Point{seed, {{Timestamp: now.Unix() - 10, Value: 42.25}}} {
					if err := db.UpdateMany(ctx, "metric", batch); err != nil {
						t.Fatal(err)
					}
					var points []*whisper.TimeSeriesPoint
					for _, p := range batch {
						points = append(points, &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value})
					}
					if err := w.UpdateMany(points); err != nil {
						t.Fatal(err)
					}
				}
				if w.OutOfOrderPath() == "" {
					t.Fatal("fixture has no sidecar")
				}
				if err := db.VerifyContinuationSource(ctx, "metric", path); err != nil {
					t.Fatal(err)
				}
				before, err := db.Snapshot(ctx, "metric")
				if err != nil {
					t.Fatal(err)
				}
				var input []store.Point
				if invalid {
					input = []store.Point{{Timestamp: -1, Value: 99}}
				}
				// No maintenance means an empty or entirely invalid batch is a no-op.
				if err := db.UpdateMany(ctx, "metric", input); err != nil {
					t.Fatal(err)
				}
				m, err := db.Metadata(ctx, "metric")
				if err != nil || m.Revision != before.Metadata.Revision {
					t.Fatalf("empty update changed revision: %+v, %v", m, err)
				}
				// Enabling direct writes must not fold the sidecar outside the
				// maintenance budget when there are no valid points to write.
				codec.DirectLateWrites = true
				if err := db.UpdateManyMaintained(ctx, "metric", input, store.ContinuationMaintenance{Allow: func() bool { return false }}); err != nil {
					t.Fatal(err)
				}
				m, err = db.Metadata(ctx, "metric")
				if err != nil || m.Revision != before.Metadata.Revision {
					t.Fatalf("denied maintenance changed revision: %+v, %v", m, err)
				}
				merged := false
				if err := db.UpdateManyMaintained(ctx, "metric", input, store.ContinuationMaintenance{Allow: func() bool { return true },
					Result: func(done bool, err error) { merged = done && err == nil }}); err != nil {
					t.Fatal(err)
				}
				if !merged {
					t.Fatal("empty update skipped requested sidecar maintenance")
				}
				if err := w.MergeOutOfOrder(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = store.Open(filepath.Join(root, "shared"), options)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.VerifyContinuationSource(ctx, "metric", path); err != nil {
					t.Fatal("maintained state differs from merged Whisper after reopen:", err)
				}
				after, err := db.Snapshot(ctx, "metric")
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(before, after); err != nil {
					t.Fatal("maintenance changed archive values:", err)
				}
			})
		}
	}
}
