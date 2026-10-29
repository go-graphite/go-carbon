package whisperio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

const prewarmBenchMetrics = 20000

// buildPrewarmBenchStore builds in dir a store of prewarmBenchMetrics metrics
// whose state is spread over the LSM like a production store's after a
// restart: created in an order unrelated to their names (so ID order is not
// name order), written for a while, compacted into the bottom level, then
// written once more and flushed, so the newest state keys sit in L0 above
// older versions.
func buildPrewarmBenchStore(b *testing.B, dir string, buckets bool) (names []string, base int64) {
	b.Helper()
	base = 1_790_000_000
	names = make([]string, prewarmBenchMetrics)
	for i := range names {
		names[i] = fmt.Sprintf("bench.prewarm.host%04d.cpu%02d", i/20, i%20)
	}
	ctx := context.Background()
	now := time.Unix(base, 0)
	db, err := store.Open(dir, store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: buckets,
		Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.DisableMetadataCacheForTest()()
	order := rand.New(rand.NewSource(1)).Perm(len(names))
	for _, i := range order {
		if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 600, Count: 8640}},
			AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
	}
	write := func(round int64) {
		now = time.Unix(base+round*60, 0)
		for _, i := range order {
			points := []store.Point{{Timestamp: now.Unix(), Value: float64(i)}}
			if i%8 == 0 && round > 2 { // some late points, hence sidecars
				points = append(points, store.Point{Timestamp: now.Unix() - 600, Value: 1})
			}
			if err := db.UpdateMany(ctx, names[i], points); err != nil {
				b.Fatal(err)
			}
		}
	}
	for round := range int64(4) {
		write(round)
	}
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	write(4)
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	return names, base
}

func copyStoreDir(b *testing.B, from, to string) {
	b.Helper()
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		target := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0750)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		return errors.Join(err, out.Close())
	})
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkPrewarmThenUpdates measures the first update of every metric of a
// freshly opened store (cold metadata cache, cold block cache), as after a
// restart: each update is preceded by the persister's route lookup
// (Store.Metadata). "cold" updates with an empty metadata cache; the others
// first prewarm every name, sorted by name or shuffled, in batches of 32 per
// read view or one name each, on one worker (or four), then run the same
// updates. "blocks-only" prewarms, then forgets the metadata cache: its
// updates read the same keys as "cold" through a Pebble block cache the
// prewarm warmed, which separates the two caches' share of the warm-up. The
// block cache holds 8 MiB, less than the state keys' blocks, as in
// production; the "-1g" variants hold them all.
// Reported per metric: the prewarm's wall time and LSM reads, and the
// updates' time and reads.
func BenchmarkPrewarmThenUpdates(b *testing.B) {
	type variant struct {
		name     string
		prewarm  bool
		shuffled bool
		batch    int
		workers  int
		// forget drops the prewarmed states before the updates.
		forget bool
		// blockCache overrides the Pebble block cache size.
		blockCache int64
	}
	variants := []variant{
		{name: "cold"},
		{name: "prewarm-sorted-batch32", prewarm: true, batch: 32},
		{name: "prewarm-sorted-batch32-workers4", prewarm: true, batch: 32, workers: 4},
		{name: "prewarm-shuffled-batch32", prewarm: true, shuffled: true, batch: 32},
		{name: "prewarm-sorted-batch1", prewarm: true, batch: 1},
		{name: "prewarm-shuffled-batch1", prewarm: true, shuffled: true, batch: 1},
		{name: "blocks-only", prewarm: true, batch: 32, forget: true},
		{name: "cold-1g", blockCache: 1 << 30},
		{name: "blocks-only-1g", prewarm: true, batch: 32, forget: true, blockCache: 1 << 30},
		{name: "prewarm-sorted-batch32-1g", prewarm: true, batch: 32, blockCache: 1 << 30},
	}
	for _, buckets := range []bool{false, true} {
		// Built once per layout, outside the measured sub-benchmarks.
		parent := b
		var source string
		var names []string
		var base int64
		for _, v := range variants {
			b.Run(fmt.Sprintf("buckets=%v/%s", buckets, v.name), func(b *testing.B) {
				if source == "" {
					dir := filepath.Join(parent.TempDir(), "source")
					names, base = buildPrewarmBenchStore(b, dir, buckets)
					source = dir
				}
				ctx := context.Background()
				// The persister writes in its own order, unrelated to names.
				writeOrder := slices.Clone(names)
				rand.New(rand.NewSource(2)).Shuffle(len(writeOrder), func(i, j int) { writeOrder[i], writeOrder[j] = writeOrder[j], writeOrder[i] })
				prewarmOrder := slices.Clone(names)
				if v.shuffled {
					rand.New(rand.NewSource(3)).Shuffle(len(prewarmOrder), func(i, j int) { prewarmOrder[i], prewarmOrder[j] = prewarmOrder[j], prewarmOrder[i] })
				}
				var prewarmTime, updateTime time.Duration
				var prewarmReads, updateReads, prewarmed uint64
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					dir := filepath.Join(b.TempDir(), "store")
					copyStoreDir(b, source, dir)
					now := time.Unix(base+10*60, 0)
					blockCache := int64(8 << 20)
					if v.blockCache > 0 {
						blockCache = v.blockCache
					}
					db, err := store.Open(dir, store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: buckets,
						Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: blockCache, MemTableSize: 16 << 20,
						MetadataCacheSize: 1 << 30})
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					if v.prewarm {
						started := time.Now()
						reads := store.CountStoreReadsForTest()
						stats, err := db.PrewarmMetadata(ctx, slices.Values(prewarmOrder), store.PrewarmOptions{Workers: max(v.workers, 1), Batch: v.batch})
						prewarmReads += reads()
						prewarmTime += time.Since(started)
						if err != nil || stats.Failed != 0 {
							b.Fatalf("prewarm: %+v, %v", stats, err)
						}
						prewarmed += uint64(stats.Prewarmed)
						if v.forget {
							db.ForgetMetadataCacheForTest()
						}
					}
					started := time.Now()
					reads := store.CountStoreReadsForTest()
					point := []store.Point{{Timestamp: now.Unix(), Value: 1}}
					for _, name := range writeOrder {
						if _, err := db.Metadata(ctx, name); err != nil {
							b.Fatal(err)
						}
						if err := db.UpdateMany(ctx, name, point); err != nil {
							b.Fatal(err)
						}
					}
					updateReads += reads()
					updateTime += time.Since(started)
					b.StopTimer()
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				n := float64(b.N) * float64(len(names))
				if v.prewarm {
					b.ReportMetric(float64(prewarmTime.Nanoseconds())/n, "prewarm-ns/metric")
					b.ReportMetric(float64(prewarmReads)/n, "prewarm-reads/metric")
					b.ReportMetric(float64(prewarmed)/n, "prewarmed/metric")
				}
				b.ReportMetric(float64(updateTime.Nanoseconds())/n, "update-ns/metric")
				b.ReportMetric(float64(updateReads)/n, "update-reads/metric")
				b.ReportMetric(float64((prewarmTime+updateTime).Nanoseconds())/n, "total-ns/metric")
			})
		}
	}
}
