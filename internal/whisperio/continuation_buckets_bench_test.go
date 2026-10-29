package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// GO_CARBON_BENCH_TIME_BUCKETS=1 compares the v6 and v7 layouts on converted
// history followed by live updates across simulated days, reporting flush and
// compaction writes and the resulting store size. Small memtables and levels
// make a few hundred metrics behave like a large store.
func TestTimeBucketsCompactionWrites(t *testing.T) {
	if os.Getenv("GO_CARBON_BENCH_TIME_BUCKETS") != "1" {
		t.Skip("set GO_CARBON_BENCH_TIME_BUCKETS=1")
	}
	metrics, days := 200, 3
	if n, err := strconv.Atoi(os.Getenv("GO_CARBON_BENCH_METRICS")); err == nil {
		metrics = n
	}
	if n, err := strconv.Atoi(os.Getenv("GO_CARBON_BENCH_DAYS")); err == nil {
		days = n
	}
	// Points per update, one per simulated minute: the cache's batching.
	batch := 10
	if n, err := strconv.Atoi(os.Getenv("GO_CARBON_BENCH_BATCH")); err == nil && n > 0 {
		batch = n
	}
	ctx := context.Background()
	const base = int64(1792368000) // midnight UTC
	retentions := whisper.MustParseRetentionDefs("1m:3d,10m:60d,1h:2y")
	sourceRoot := t.TempDir()
	// Identical sources for both layouts: hourly history for most of two
	// years, ten-minute and one-minute history for the finer archives.
	now := time.Unix(base, 0)
	clock := func() time.Time { return now }
	for i := 0; i < metrics; i++ {
		path := filepath.Join(sourceRoot, fmt.Sprintf("m%04d.wsp", i))
		w, err := whisper.CreateWithOptions(path, retentions, whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true, Now: clock})
		if err != nil {
			t.Fatal(err)
		}
		var points []*whisper.TimeSeriesPoint
		for ts := base - 700*86400; ts < base; {
			step := int64(3600)
			if ts >= base-55*86400 {
				step = 600
			}
			if ts >= base-2*86400 {
				step = 60
			}
			points = append(points, &whisper.TimeSeriesPoint{Time: int(ts), Value: math.Sin(float64(ts)/7919+float64(i)) * 1e4})
			ts += step
		}
		if err := w.UpdateMany(points); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, run := range []struct {
		buckets bool
		repeat  int
	}{{false, 1}, {true, 1}, {false, 2}, {true, 2}} {
		buckets := run.buckets
		t.Run(fmt.Sprintf("buckets=%v/%d", buckets, run.repeat), func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.CopyFS(source, os.DirFS(sourceRoot)); err != nil {
				t.Fatal(err)
			}
			now = time.Unix(base, 0)
			db, err := store.Open(filepath.Join(root, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{},
				IncrementalSource: source, TimeBuckets: buckets, Now: clock, SyncInterval: time.Hour,
				CacheSize: 64 << 20, MemTableSize: 1 << 20, LBaseMaxBytes: 4 << 20, TargetFileSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i := 0; i < metrics; i++ {
				name := fmt.Sprintf("m%04d", i)
				capture, err := ReadSnapshotIn(ctx, name, filepath.Join(source, name+".wsp"), root)
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
			}
			// Settle the converted history first so the measurement holds only
			// the live updates' own flushes and compactions.
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			before := db.Stats()
			start := time.Now()
			rounds := days * 1440 / batch
			for round := 1; round <= rounds; round++ {
				stamp := base + int64(round*batch)*60
				now = time.Unix(stamp+1, 0)
				for i := 0; i < metrics; i++ {
					input := make([]store.Point, batch)
					for j := range input {
						ts := stamp - int64(batch-j)*60
						input[j] = store.Point{Timestamp: ts, Value: math.Sin(float64(ts)/7919+float64(i)) * 1e4}
					}
					if err := db.UpdateMany(ctx, fmt.Sprintf("m%04d", i), input); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			after := db.Stats()
			var perLevel [7]float64
			for i := range perLevel {
				perLevel[i] = math.Round(float64(after.LevelCompactedBytes[i]-before.LevelCompactedBytes[i])/(1<<20)*10) / 10
			}
			t.Logf("buckets=%v updates=%d in %s: flush %.1f MiB, compaction write %.1f MiB (per level %v), read %.1f MiB, disk %.1f MiB",
				buckets, rounds*metrics, time.Since(start).Round(time.Second),
				float64(after.FlushWriteBytes-before.FlushWriteBytes)/(1<<20),
				float64(after.CompactionWriteBytes-before.CompactionWriteBytes)/(1<<20), perLevel,
				float64(after.CompactionReadBytes-before.CompactionReadBytes)/(1<<20),
				float64(after.DiskBytes)/(1<<20))
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			t.Logf("buckets=%v fully compacted disk %.1f MiB", buckets, float64(db.Stats().DiskBytes)/(1<<20))
		})
	}
}
