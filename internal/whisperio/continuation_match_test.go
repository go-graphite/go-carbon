package whisperio

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func TestStoredPointMatchNeedsExactValuesAndPresence(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprint(sparse), func(t *testing.T) {
			kv := appendTailSeed(t, 0)
			f := appendTailFile(kv)
			original := appendTailPoints(0, 12000)
			if sparse {
				for i := range original {
					original[i].Time = 1000 + 2*i
				}
			}
			block := appendTailBlock(len(original))
			block.Last = original[len(original)-1].Time
			if err := f.AppendStorageBlock(block, 0, original); err != nil {
				t.Fatal(err)
			}
			exact := append([]whisper.TimeSeriesPoint(nil), original[10000:10096]...)
			if ok, err := f.MatchesStoragePoints(block, exact); err != nil || !ok {
				t.Fatal(ok, err)
			}
			changed := append([]whisper.TimeSeriesPoint(nil), exact...)
			changed[0].Value++
			// Incrementing a very large float may not change its bits.
			changed[0].Value = math.Nextafter(changed[0].Value, math.Inf(1))
			if ok, err := f.MatchesStoragePoints(block, changed); err != nil || ok {
				t.Fatal("changed late data matched", ok, err)
			}
			if sparse {
				hole := []whisper.TimeSeriesPoint{{Time: 1001, Value: 42}}
				if ok, err := f.MatchesStoragePoints(block, hole); err != nil || ok {
					t.Fatal("hole below watermark matched", ok, err)
				}
			}
			// Signed zero is a correction even though ordinary float equality agrees.
			p := original[10005]
			if !math.Signbit(p.Value) {
				t.Fatal("fixture has no negative zero")
			}
			p.Value = 0
			if ok, err := f.MatchesStoragePoints(block, []whisper.TimeSeriesPoint{p}); err != nil || ok {
				t.Fatal("signed-zero correction matched", ok, err)
			}
		})
	}
}

func TestStoredPointMatchAcrossSealedGroups(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprint(sparse), func(t *testing.T) {
			kv := appendTailSeed(t, 0)
			f := appendTailFile(kv)
			input := appendTailPoints(0, 2300)
			if sparse {
				for i := range input {
					input[i].Time = 1000 + 2*i
				}
			}
			desc := blockDescriptor{count: len(input), bucketed: true, packed: 1024}
			for first := 0; first < len(input); first += desc.packed {
				var points []store.Point
				for _, p := range input[first:min(first+desc.packed, len(input))] {
					points = append(points, store.Point{Timestamp: int64(p.Time), Value: p.Value})
				}
				value, err := store.EncodeSealedPoints(points, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := kv.Set(desc.groupKey(1, 0, 0, first/desc.packed), value); err != nil {
					t.Fatal(err)
				}
			}
			if err := kv.Set(continuationBlockPrefix(1, 'b', 0, 0), desc.encode()); err != nil {
				t.Fatal(err)
			}
			block := appendTailBlock(len(input))
			block.Last = input[len(input)-1].Time
			probe := []whisper.TimeSeriesPoint{input[0], input[1023], input[1024], input[2047], input[2048], input[2299]}
			if ok, err := f.MatchesStoragePoints(block, probe); err != nil || !ok {
				t.Fatal("sealed boundary or partial tail did not match", ok, err)
			}
			probe[len(probe)-1].Value = math.Pi
			if ok, err := f.MatchesStoragePoints(block, probe); err != nil || ok {
				t.Fatal("sealed correction matched", ok, err)
			}
		})
	}
}

func TestStoredPointMatchRejectsUnalignedNeighbors(t *testing.T) {
	for _, at := range []int{63, 127} {
		t.Run(fmt.Sprint(at), func(t *testing.T) {
			kv := appendTailSeed(t, 0)
			f := appendTailFile(kv)
			f.data.layout.Archives[0].Step = 60
			input := appendTailPoints(0, 256)
			for i := range input {
				input[i].Time = 600 + i*60
			}
			block := appendTailBlock(len(input))
			block.Step, block.First, block.Last = 60, input[0].Time, input[len(input)-1].Time
			if err := f.AppendStorageBlock(block, 0, input); err != nil {
				t.Fatal(err)
			}
			desc, err := f.descriptor(0, 0)
			if err != nil {
				t.Fatal(err)
			}
			key := desc.groupKey(1, 0, 0, (at+1)/128)
			value, err := kv.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			var group store.PointBlock
			if err := store.DecodePointBlock(value, &group); err != nil {
				t.Fatal(err)
			}
			// Imported histories can contain off-grid points. The next point
			// now shares the candidate's logical slot, despite different raw time.
			group.Points[(at+1)%128].Timestamp = int64(input[at].Time + 17)
			if err := kv.Set(key, store.EncodePointBlock(&group)); err != nil {
				t.Fatal(err)
			}
			if ok, err := f.MatchesStoragePoints(block, input[at:at+1]); err != nil || ok {
				t.Fatal("raw equality hid an unaligned neighbor", ok, err)
			}
		})
	}
}

// A confirmed metric can be retained behind an unconfirmed generation, then
// replayed after a crash. Compare every archive with the original planner,
// including new corrections and holes older than the persisted maximum.
func TestReplayDuplicatesAndGenuineLateDataMatchPlanningOracle(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%t", buckets), func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1700000040, 0)
			rets := []store.Retention{{Step: 60, Count: 14400}, {Step: 3600, Count: 720}, {Step: 86400, Count: 90}}
			paths := []string{filepath.Join(t.TempDir(), "fast"), filepath.Join(t.TempDir(), "oracle")}
			dbs := make([]*store.Store, 2)
			open := func(i int) *store.Store {
				db, err := openMemoryTestStore(t, paths[i], store.Options{ExactBlocks: true, TimeBuckets: buckets, MetadataCacheSize: 4 << 20, Continuation: &ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true, keepDuplicatePlanning: i == 1}, Now: func() time.Time { return now }, SyncInterval: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return db
			}
			for i := range dbs {
				dbs[i] = open(i)
			}
			defer func() {
				for _, db := range dbs {
					db.Close()
				}
			}()
			start := now.Unix() - 8000*60
			hole := start + 1200*60
			config := store.MetricConfig{Name: "series", Retentions: rets, AggregationMethod: store.Average}
			var seed []store.Point
			for i := range 8000 {
				if i != 1200 {
					seed = append(seed, store.Point{Timestamp: start + int64(i)*60, Value: float64(i % 19)})
				}
			}
			for _, db := range dbs {
				if _, err := db.Create(ctx, config); err != nil {
					t.Fatal(err)
				}
				if err := db.UpdateMany(ctx, "series", seed); err != nil {
					t.Fatal(err)
				}
			}
			apply := func(p *points.Points) {
				input := make([]store.Point, len(p.Data))
				for i, x := range p.Data {
					input[i] = store.Point{Timestamp: x.Timestamp, Value: x.Value}
				}
				for _, db := range dbs {
					if err := db.UpdateMany(ctx, p.Metric, input); err != nil {
						t.Fatal(err)
					}
				}
			}
			core := cache.New()
			defer core.Stop()
			core.EnableInputLog()
			logdir := t.TempDir()
			opts := inputlog.Options{SyncInterval: time.Hour, MaxAge: 24 * time.Hour, SegmentSize: 64 << 10}
			log, err := inputlog.Open(logdir, opts)
			if err != nil {
				t.Fatal(err)
			}
			log.Start(core, cache.ShardCount)
			core.Add(points.OnePoint("held", 1, start))
			late := start + 1000*60
			core.Add(points.OnePoint("series", 777, late))
			if err := log.Sync(); err != nil {
				t.Fatal(err)
			}
			batch, ok := core.PopNotConfirmed("series")
			if !ok {
				t.Fatal("missing persisted batch")
			}
			apply(batch)
			core.Confirm(batch)
			if err := log.Sync(); err != nil {
				t.Fatal(err)
			}
			log.Kill()
			reopened, err := inputlog.Open(logdir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			replayed := false
			before := whisper.InPlaceBlocks(true)
			if _, err := reopened.Replay(func(p *points.Points) {
				if p.Metric == "series" {
					replayed = true
					apply(p)
				}
			}); err != nil {
				t.Fatal(err)
			}
			if !replayed {
				t.Fatal("fixture did not replay the already-confirmed metric")
			}
			if whisper.InPlaceBlocks(true) == before {
				t.Fatal("replayed points did not avoid block planning")
			}
			apply(points.OnePoint("series", 999, late).Add(333, hole))
			// Exercise last-in-batch precedence and a genuinely late correction again.
			apply(points.OnePoint("series", 1, late).Add(2, late))
			for i, db := range dbs {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				dbs[i] = open(i)
			}
			fast, err := dbs[0].Snapshot(ctx, "series")
			if err != nil {
				t.Fatal(err)
			}
			oracle, err := dbs[1].Snapshot(ctx, "series")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(fast.Archives, oracle.Archives) {
				t.Fatal("replay/correction changed archive contents vs reference planner")
			}
			values := map[int64]float64{}
			for _, p := range fast.Archives[0].Points {
				values[p.Timestamp] = p.Value
			}
			if values[late] != 2 || values[hole] != 333 {
				t.Fatal("genuine late data lost", values[late], values[hole])
			}
		})
	}
}

func BenchmarkContinuationDuplicatePlanning(b *testing.B) {
	for _, archives := range []int{1, 2} {
		for _, reference := range []bool{true, false} {
			for _, changed := range []bool{false, true} {
				b.Run(fmt.Sprintf("archives=%d/reference=%t/changed=%t", archives, reference, changed), func(b *testing.B) {
					ctx := context.Background()
					clock := time.Unix(1700000040, 0)
					db, err := store.Open(filepath.Join(b.TempDir(), "store"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true, keepDuplicatePlanning: reference}, Now: func() time.Time { return clock }, SyncInterval: time.Hour, CacheSize: 4 << 20, MemTableSize: 4 << 20})
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()
					cfg := store.MetricConfig{Name: "bench", Retentions: []store.Retention{{Step: 60, Count: 14400}, {Step: 3600, Count: 720}}, AggregationMethod: store.Average}
					cfg.Retentions = cfg.Retentions[:archives]
					if _, err := db.Create(ctx, cfg); err != nil {
						b.Fatal(err)
					}
					var seed []store.Point
					for i := range 8000 {
						seed = append(seed, store.Point{Timestamp: clock.Unix() - int64(8000-i)*60, Value: float64(i % 19)})
					}
					if err := db.UpdateMany(ctx, cfg.Name, seed); err != nil {
						b.Fatal(err)
					}
					input := append([]store.Point(nil), seed[1000:1096]...)
					// Materialize consistent coarse windows once; benchmark stable replay.
					if err := db.UpdateMany(ctx, cfg.Name, input); err != nil {
						b.Fatal(err)
					}
					before := whisper.InPlaceBlocks(true)
					b.ReportAllocs()
					b.ResetTimer()
					cpuBefore := restartcpu.CPU()
					for i := 0; i < b.N; i++ {
						if changed {
							input[0].Value = float64(i%2 + 100)
						}
						if err := db.UpdateMany(ctx, cfg.Name, input); err != nil {
							b.Fatal(err)
						}
					}
					cpuUsed := restartcpu.CPU() - cpuBefore
					b.StopTimer()
					b.ReportMetric(cpuUsed*1e6/float64(b.N), "cpu-us/op")
					b.ReportMetric(float64(whisper.InPlaceBlocks(true)-before)/float64(b.N), "skipped_blocks/op")
				})
			}
		}
	}
}
