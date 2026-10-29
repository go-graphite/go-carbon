package whisperio

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A levels store spreads its metrics' keys over the LSM the way a busy host's
// are: history and names in the bottom level, the rewrites of later rounds in
// the level above it and in six L0 sublevels, and half of the metrics with an
// OOO sidecar. Each layout's store is built once into the temporary
// directory, kept for later runs, and copied for each benchmark.
type levelsTemplate struct {
	once sync.Once
	dir  string
	now  int64
	err  error
}

var levelsTemplates [2]levelsTemplate

const levelsBenchMetrics = 2000

func levelsBenchName(i int) string { return fmt.Sprintf("bench.levels.%04d", i) }

// levelsBenchOptions describes the store. While it is built, small level
// targets push the rewrites of each round through the levels; afterwards
// (settled) L0 is left alone, keeping one sublevel per flush.
func levelsBenchOptions(dir string, buckets bool, clock func() time.Time, settled bool) (string, store.Options) {
	options := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true}, TimeBuckets: buckets,
		Now: clock, SyncInterval: time.Hour, CacheSize: 256 << 20, MemTableSize: 64 << 20, BloomFilterBits: 10,
		L0TargetFileSize: 256 << 10, TargetFileSize: 512 << 10, LBaseMaxBytes: 256 << 10}
	if settled {
		options.LBaseMaxBytes, options.L0CompactionThreshold, options.L0StopWritesThreshold = 1<<30, 32, 64
	}
	return filepath.Join(dir, "shared"), options
}

func buildLevelsBench(root string, buckets bool) (int64, error) {
	ctx := context.Background()
	stamp := int64(1_792_368_000)
	clock := func() time.Time { return time.Unix(stamp+1, 0) }
	dir, options := levelsBenchOptions(root, buckets, clock, false)
	db, err := store.Open(dir, options)
	if err != nil {
		return 0, err
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	for i := 0; i < levelsBenchMetrics; i++ {
		if _, err := db.Create(ctx, store.MetricConfig{Name: levelsBenchName(i), Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}},
			AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			return 0, err
		}
	}
	// rounds appends points to every metric per round, flushing each round
	// when flush is set. Late points open and then refill each odd metric's
	// sidecar in every tenth round.
	rounds := func(n, points int, flush bool) error {
		input := make([]store.Point, 0, points+2)
		for round := 0; round < n; round++ {
			stamp += int64(points)
			for i := 0; i < levelsBenchMetrics; i++ {
				input = input[:0]
				for j := 0; j < points; j++ {
					input = append(input, store.Point{Timestamp: stamp - int64(points-j), Value: float64((i + j) % 97)})
				}
				if i%2 == 1 && round%10 == 0 {
					input = append(input, store.Point{Timestamp: stamp - 1800, Value: 1}, store.Point{Timestamp: stamp - 1807, Value: 2})
				}
				if err := db.UpdateMany(ctx, levelsBenchName(i), input); err != nil {
					return err
				}
			}
			if flush {
				if err := db.Flush(); err != nil {
					return err
				}
			}
		}
		for db.Stats().CompactionsInProgress > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}
	// Two hours of history, compacted into the bottom level with the names,
	// then an hour of persister-sized rounds spread over the levels above it.
	if err := rounds(6, 1200, false); err != nil {
		return 0, err
	}
	if err := db.Compact(); err != nil {
		return 0, err
	}
	if err := rounds(60, 60, true); err != nil {
		return 0, err
	}
	if err := db.Close(); err != nil {
		return 0, err
	}
	dir, options = levelsBenchOptions(root, buckets, clock, true)
	if db, err = store.Open(dir, options); err != nil {
		return 0, err
	}
	// The last minutes stay in L0.
	if err := rounds(6, 60, true); err != nil {
		return 0, err
	}
	err = db.Close()
	db = nil
	return stamp, err
}

// template returns the built store of a layout and the time of its last write.
func (t *levelsTemplate) template(buckets bool) (string, int64, error) {
	t.once.Do(func() {
		t.dir = filepath.Join(os.TempDir(), fmt.Sprintf("go-carbon-levels-bench-v1-buckets-%v", buckets))
		marker := filepath.Join(t.dir, "built")
		if data, err := os.ReadFile(marker); err == nil {
			t.now, t.err = strconv.ParseInt(string(data), 10, 64)
			return
		}
		if t.err = os.RemoveAll(t.dir); t.err != nil {
			return
		}
		if t.now, t.err = buildLevelsBench(t.dir, buckets); t.err == nil {
			t.err = os.WriteFile(marker, []byte(strconv.FormatInt(t.now, 10)), 0600)
		}
	})
	return filepath.Join(t.dir, "shared"), t.now, t.err
}

// openLevelsBench opens a private copy of a levels store with a block cache
// of cache bytes and returns its clock, which reports the time after its
// last write.
func openLevelsBench(b *testing.B, buckets bool, cache int64, shared bool) (*store.Store, *int64) {
	b.Helper()
	index := 0
	if buckets {
		index = 1
	}
	template, now, err := levelsTemplates[index].template(buckets)
	if err != nil {
		b.Fatal(err)
	}
	dir, options := levelsBenchOptions(b.TempDir(), buckets, func() time.Time { return time.Unix(now+1, 0) }, true)
	options.CacheSize, options.UpdatePointLookups = cache, !shared
	// As on a busy host, a memtable fills between two updates of a metric, so
	// each update finds its keys' latest versions in tables; L0 compacts into
	// the level below at eight sublevels.
	options.MemTableSize, options.L0CompactionThreshold = 4<<20, 8
	if err := os.CopyFS(dir, os.DirFS(template)); err != nil {
		b.Fatal(err)
	}
	db, err := store.Open(dir, options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	return db, &now
}

// BenchmarkContinuationUpdateLevels appends a batch of 1s points to each
// metric of a levels store in turn, in both layouts: 30 points, and 300 as
// the persister's cache accumulates between writes on a busy host. The block
// cache holds the working set or a fraction of it. It reports Pebble table
// cache lookups (one per table iterator a lookup or iterator opens) and block
// cache lookups and misses per update.
func BenchmarkContinuationUpdateLevels(b *testing.B) {
	for _, buckets := range []bool{false, true} {
		for _, points := range []int{30, 300} {
			for _, cache := range []int64{256 << 20, 32 << 20} {
				for _, shared := range []bool{false, true} {
					b.Run(fmt.Sprintf("buckets=%v/points=%d/cache=%dMiB/shared=%v", buckets, points, cache>>20, shared), func(b *testing.B) {
						benchmarkContinuationUpdateLevels(b, buckets, points, cache, shared)
					})
				}
			}
		}
	}
}

func benchmarkContinuationUpdateLevels(b *testing.B, buckets bool, points int, cache int64, shared bool) {
	ctx := context.Background()
	db, now := openLevelsBench(b, buckets, cache, shared)
	input := make([]store.Point, points)
	before := db.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%levelsBenchMetrics == 0 {
			*now += int64(points)
		}
		for j := range input {
			input[j] = store.Point{Timestamp: *now - int64(points-j), Value: float64(i + j)}
		}
		if err := db.UpdateMany(ctx, levelsBenchName(i%levelsBenchMetrics), input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after := db.Stats()
	n := float64(b.N)
	b.ReportMetric(float64(after.TableCacheHits+after.TableCacheMisses-before.TableCacheHits-before.TableCacheMisses)/n, "tables/op")
	b.ReportMetric(float64(after.CacheHits+after.CacheMisses-before.CacheHits-before.CacheMisses)/n, "blocks/op")
	b.ReportMetric(float64(after.CacheMisses-before.CacheMisses)/n, "misses/op")
	if after.Flushes != before.Flushes || after.Compactions != before.Compactions {
		b.Logf("%d flushes and %d compactions during the run", after.Flushes-before.Flushes, after.Compactions-before.Compactions)
	}
}
