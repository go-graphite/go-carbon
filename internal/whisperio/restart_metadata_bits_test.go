package whisperio

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Loading the accelerator before replay must make exactly the same raw
// continuation mutations as cold disk reads, including unusual float bits and
// duplicate/late timestamps across the restart boundary.
func TestRestartMetadataCheckpointPreservesFloatBits(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			const base = int64(1700000000)
			now := time.Unix(base+100, 0)
			var dbs [2]*store.Store
			for side := range dbs {
				dir := filepath.Join(t.TempDir(), "store")
				opts := store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: &ContinuationCodec{}, Now: func() time.Time { return now },
					CacheSize: 4 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 8 << 20, MetadataCacheCheckpoint: true}
				db, err := store.Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Create(ctx, store.MetricConfig{Name: "restart.bits", Retentions: []store.Retention{{Step: 1, Count: 600}, {Step: 10, Count: 360}}, AggregationMethod: store.Last, XFilesFactor: .5}); err != nil {
					t.Fatal(err)
				}
				if err := db.UpdateMany(ctx, "restart.bits", []store.Point{{Timestamp: base, Value: 1}, {Timestamp: base + 10, Value: math.Copysign(0, -1)}}); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil || db.MetadataCheckpointError() != nil {
					t.Fatal(err, db.MetadataCheckpointError())
				}
				db, err = store.Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				if side == 0 {
					stats, err := db.LoadMetadataCheckpoint(ctx, nil)
					if err != nil || !stats.Valid || stats.Fenced != 1 {
						t.Fatal(stats, err)
					}
				} else {
					t.Cleanup(db.DisableMetadataCacheForTest())
				}
				dbs[side] = db
			}
			bits := [...]uint64{0, 1 << 63, 0x7ff8000000000012, 0x7ff0000000000001, 1, 0x3ff0000000000001, 0x7ff0000000000000, 0xfff0000000000000}
			for round := range 16 {
				now = time.Unix(base+100+int64(round), 0)
				input := []store.Point{
					{Timestamp: base + int64(round), Value: math.Float64frombits(bits[round%len(bits)])},
					{Timestamp: base + int64(round/2), Value: math.Float64frombits(bits[(round+1)%len(bits)])},
					{Timestamp: base + 90 + int64(round), Value: math.Float64frombits(bits[(round+2)%len(bits)])},
				}
				for _, db := range dbs {
					if err := db.UpdateMany(ctx, "restart.bits", input); err != nil {
						t.Fatal(err)
					}
				}
				got, err := dbs[0].ExportTransfer(ctx, []string{"restart.bits"})
				if err != nil {
					t.Fatal(err)
				}
				want, err := dbs[1].ExportTransfer(ctx, []string{"restart.bits"})
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal("raw continuation bytes changed after early metadata load", round, err)
				}
			}
		})
	}
}
