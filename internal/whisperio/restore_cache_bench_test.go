package whisperio

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Restart-shaped first writes: random metric order, cold metadata, old points
// overlapping stored history, and more SST blocks than the data cache holds.
// Each case starts from an identical store in a memory filesystem, isolating
// cache/CPU work from filesystem sync latency; setup is outside timing. Timing
// includes loading the optional metadata checkpoint and all update commits.
func BenchmarkRestoreCacheThrash(b *testing.B) {
	source := filepath.Join(b.TempDir(), "source")
	names, base := buildRestoreCacheFixture(b, source)
	order := slices.Clone(names)
	rand.New(rand.NewSource(8144)).Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for _, checkpoint := range []bool{false, true} {
		b.Run(fmt.Sprintf("checkpoint=%v", checkpoint), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				fs := vfs.NewMem()
				const dir = "/store"
				if ok, err := vfs.Clone(vfs.Default, fs, source, dir); err != nil || !ok {
					b.Fatal("copy fixture", ok, err)
				}
				db, err := store.Open(dir, (store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: true,
					Now: func() time.Time { return time.Unix(base+4096+60, 0) }, SyncInterval: time.Hour,
					CacheSize: 512 << 10, CacheShards: 4, MemTableSize: 64 << 10, MetadataCacheSize: 32 << 20, MetadataCacheCheckpoint: true}).WithFS(fs))
				if err != nil {
					b.Fatal(err)
				}
				input := make([]store.Point, 32)
				for i := range input {
					input[i] = store.Point{Timestamp: base + 4000 + int64(i), Value: math.Float64frombits(0x3ff0000000000000 + uint64(i))}
				}
				started := time.Now()
				before := db.Stats()
				b.StartTimer()
				if checkpoint {
					stats, err := db.LoadMetadataCheckpoint(context.Background(), nil)
					if err != nil || !stats.Valid || stats.Entries < int64(len(order))*9/10 {
						b.Fatal("fixture checkpoint unavailable", stats, err)
					}
				}
				for _, name := range order {
					if _, err := db.Metadata(context.Background(), name); err != nil {
						b.Fatal(err)
					}
					if err := db.UpdateMany(context.Background(), name, input); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(order))/time.Since(started).Seconds(), "updates/s")
				after := db.Stats()
				n := float64(len(order))
				b.ReportMetric(float64(after.CacheMisses-before.CacheMisses)/n, "cache-misses/update")
				b.ReportMetric(float64(after.FS.Ops[store.FSRead][store.FSTable]-before.FS.Ops[store.FSRead][store.FSTable])/n, "table-reads/update")
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func buildRestoreCacheFixture(b *testing.B, dir string) ([]string, int64) {
	b.Helper()
	const metrics, history = 2048, 4096
	const base = int64(1_790_000_000)
	db, err := store.Open(dir, store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: true,
		Now: func() time.Time { return time.Unix(base+history+1, 0) }, SyncInterval: time.Hour, CacheSize: 32 << 20, MemTableSize: 4 << 20,
		MetadataCacheSize: 32 << 20, MetadataCacheCheckpoint: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	names := make([]string, metrics)
	input := make([]store.Point, history)
	rng := rand.New(rand.NewSource(8144))
	for i := range names {
		names[i] = fmt.Sprintf("restore.host%04d.cpu%02d", i/16, i%16)
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 8192}, {Step: 60, Count: 1440}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
		for j := range input {
			input[j] = store.Point{Timestamp: base + int64(j), Value: math.Float64frombits(0x3ff0000000000000 | rng.Uint64()&((1<<52)-1))}
		}
		if err := db.UpdateMany(context.Background(), names[i], input); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	if size := db.Stats().DiskBytes; size <= 8<<20 {
		b.Fatalf("history fixture %d bytes must exceed the read cache", size)
	}
	return names, base
}
