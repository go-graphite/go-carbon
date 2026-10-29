package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Drives the actual indexed-batch backend, including an imported OOO sidecar,
// source compactions, database compactions and process-style close/reopen.
func TestContinuationStoreMatchesCWhisperAfterImport(t *testing.T) {
	for _, config := range []struct {
		name, retentions string
		seed, block      int
		integers         bool
		// reuse overwrites rewritten blocks' groups in place
		// (ContinuationCodec.ReuseBlockKeys).
		reuse bool
	}{
		{"wrap", "1s:20s,5s:2m,30s:10m", 18, 16, false, false},
		{"wrap-reuse", "1s:20s,5s:2m,30s:10m", 18, 16, false, true},
		// Two-group blocks in a two-block ring: a block that wraps restarts
		// with fewer groups than its previous incarnation left behind.
		{"groups", "1s:5m,10s:1h", 100, 150, false, false},
		{"groups-reuse", "1s:5m,10s:1h", 100, 150, false, true},
		{"production-float", "1s:2d,60s:28d,3600s:2y", 1000, 7200, false, false},
		{"production-integer", "1s:2d,60s:28d,3600s:2y", 1000, 7200, true, false},
	} {
		for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
			for _, xff := range []float32{0, .5, 1} {
				t.Run(fmt.Sprintf("%s/%s/xff%g", config.name, method, xff), func(t *testing.T) {
					t.Parallel()
					ctx := context.Background()
					const base = int64(1792432631)
					now := time.Unix(base+2, 0)
					clock := func() time.Time { return now }
					root := t.TempDir()
					sourceDir := filepath.Join(root, "source")
					if err := os.Mkdir(sourceDir, 0700); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(sourceDir, "oracle.wsp")
					w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(config.retentions), method, xff,
						&whisper.Options{Compressed: true, OutOfOrder: true, PointsPerBlock: config.block, Now: clock})
					if err != nil {
						t.Fatal(err)
					}
					var seed []*whisper.TimeSeriesPoint
					for age := config.seed; age > 0; age-- {
						v := float64(age)
						if !config.integers {
							v += .1
						}
						seed = append(seed, &whisper.TimeSeriesPoint{Time: int(base) - age, Value: v})
					}
					if err := w.UpdateMany(seed); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					dir := filepath.Join(root, "shared")
					// The shared fetch iterator is opt-in; cover both read paths.
					options := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: config.reuse}, IncrementalSource: sourceDir, Now: clock,
						CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, TimeBuckets: os.Getenv("GO_CARBON_TEST_TIME_BUCKETS") == "1", FetchSharedIterator: xff == .5}
					var db *store.Store
					defer func() {
						if db != nil {
							if err := db.Close(); err != nil {
								t.Error(err)
							}
						}
					}()
					merges := 0
					reusedBefore := SkippedBlockDeletes()
					defer func() {
						// Only the v6 layout rewrites blocks under their old keys; v7
						// moves a rewritten block to its new time bucket.
						if config.reuse && !options.TimeBuckets && SkippedBlockDeletes() == reusedBefore {
							t.Error("no block rewrite reused its keys")
						}
					}()
					for seq := 0; seq < 140; seq++ {
						stamp := base + int64(3*(seq+1))
						now = time.Unix(stamp+2, 0)
						var input []store.Point
						for _, age := range []int64{3, 2, 1} {
							v := float64(seq)*4 + float64(age)
							if !config.integers {
								v = float64(seq) + float64(age)/4 + .1
							}
							input = append(input, store.Point{Timestamp: stamp - age, Value: v})
						}
						if seq >= 3 {
							age := int64(17)
							if config.seed > 128 {
								age = int64(config.seed / 2)
							}
							for _, v := range []float64{111, 222} {
								if !config.integers {
									v += .25
								}
								input = append(input, store.Point{Timestamp: stamp - age, Value: v + float64(seq)})
							}
						}
						w, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, Now: clock})
						if err != nil {
							t.Fatal(err)
						}
						points := make([]*whisper.TimeSeriesPoint, len(input))
						for i, p := range input {
							points[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
						}
						if err := w.UpdateMany(points); err != nil {
							t.Fatal(err)
						}
						compact := seq%19 == 18
						if compact {
							if err := w.MergeOutOfOrder(); err != nil {
								t.Fatal(err)
							}
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						if seq == 3 {
							db, err = openMemoryTestStore(t, dir, options)
							if err != nil {
								t.Fatal(err)
							}
							capture, err := ReadSnapshotIn(ctx, "metric", path, root)
							if err != nil {
								t.Fatal(err)
							}
							staged, err := db.StageFileSnapshot(ctx, capture)
							if err != nil {
								t.Fatal(err)
							}
							if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
								t.Fatal(err)
							}
							// Production imports fold sidecars. Put the filesystem
							// oracle at that same boundary before both resume writes.
							oracle, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, Now: func() time.Time { return importFoldTime }})
							if err != nil {
								t.Fatal(err)
							}
							if _, err := foldSidecar(oracle, importFoldTime); err != nil {
								oracle.Close()
								t.Fatal(err)
							}
							if err := oracle.Close(); err != nil {
								t.Fatal(err)
							}
						} else if db != nil {
							err := db.UpdateManyMaintained(ctx, "metric", input, store.ContinuationMaintenance{Allow: func() bool { return compact }, Result: func(merged bool, err error) {
								if merged {
									merges++
								}
							}})
							if err != nil {
								t.Fatalf("write round%d: %v", seq, err)
							}
						}
						if db == nil {
							continue
						}
						if seq%23 == 22 {
							if err := db.Compact(); err != nil {
								t.Fatal(err)
							}
						}
						if seq%47 == 46 {
							if err := db.Close(); err != nil {
								t.Fatal(err)
							}
							db = nil
							db, err = openMemoryTestStore(t, dir, options)
							if err != nil {
								t.Fatal(err)
							}
						}
						now = time.Unix(stamp+3, 0)
						w, err = whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, Now: clock})
						if err != nil {
							t.Fatal(err)
						}
						ages := []int{8, 20, 100, 240, 4000}
						// The full fine-retention window allocates 172,800 values
						// per side. Check it at every rewrite/reopen boundary and
						// both ends of the trace; keep bounded live-window reads
						// after every write, including every late duplicate.
						if seq == 3 || compact || seq%47 == 46 || seq == 139 {
							ages = append(ages, 172800, 2419200)
							if err := db.VerifyContinuationSource(ctx, "metric", path); err != nil {
								t.Fatalf("physical state round%d: %v", seq, err)
							}
						}
						for _, age := range ages {
							from, until := int(now.Unix())-age, int(now.Unix())-1
							want, err := w.Fetch(from, until)
							if err != nil {
								t.Fatal(err)
							}
							got, err := db.Fetch(ctx, "metric", from, until)
							if err != nil {
								t.Fatalf("fetch round%d age%d: %v", seq, age, err)
							}
							if want == nil || got == nil {
								if want != nil || got != nil {
									t.Fatal("fetch presence mismatch")
								}
								continue
							}
							if want.FromTime() != got.FromTime || want.UntilTime() != got.UntilTime || want.Step() != got.Step || len(want.Values()) != len(got.Values) {
								t.Fatalf("fetch shape round%d age%d", seq, age)
							}
							for i, v := range want.Values() {
								if math.IsNaN(v) && math.IsNaN(got.Values[i]) {
									continue
								}
								if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
									t.Fatalf("round%d age%d timestamp%d: source=%g shared=%g", seq, age, got.FromTime+i*got.Step, v, got.Values[i])
								}
							}
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if merges == 0 {
						t.Fatal("trace did not compact an OOO sidecar")
					}
				})
			}
		}
	}
}
