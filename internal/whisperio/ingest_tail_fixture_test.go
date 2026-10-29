package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type tailFixtureCodec struct {
	*ContinuationCodec
	versions          map[string]int
	sets, repeats     uint64
	deletes, prefixes uint64
	bytes             uint64
}

type tailFixtureIO struct {
	store.ContinuationIO
	codec *tailFixtureCodec
	name  string
}

func (io tailFixtureIO) Set(key, value []byte) error {
	c := io.codec
	c.sets++
	c.bytes += uint64(len(key) + len(value))
	k := io.name + string(key)
	if c.versions[k] > 0 {
		c.repeats++
	}
	c.versions[k]++
	return io.ContinuationIO.Set(key, value)
}
func (io tailFixtureIO) Delete(key []byte) error {
	io.codec.deletes++
	return io.ContinuationIO.Delete(key)
}
func (io tailFixtureIO) DeletePrefix(key []byte) error {
	io.codec.prefixes++
	return io.ContinuationIO.DeletePrefix(key)
}
func (io tailFixtureIO) TimeBuckets() bool {
	return io.ContinuationIO.(store.ContinuationTimeBuckets).TimeBuckets()
}
func (io tailFixtureIO) Probe(key []byte) ([]byte, error) {
	if prober, ok := io.ContinuationIO.(store.ContinuationProber); ok {
		return prober.Probe(key)
	}
	return io.Get(key)
}
func (c *tailFixtureCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p []store.Point, retention int, now func() time.Time, maintenance *store.ContinuationMaintenance) error {
	return c.ContinuationCodec.Update(ctx, tailFixtureIO{ContinuationIO: kv, codec: c, name: m.Name}, m, p, retention, now, maintenance)
}

func tailFixtureCPU() float64 {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

// Opt-in measurement of the real continuation writer over local Pebble. Both
// batch sizes ingest identical points; no production coalescing policy changes.
func TestIngestTailCompactionFixture(t *testing.T) {
	if os.Getenv("INGEST_TAIL_FIXTURE") == "" {
		t.Skip("set INGEST_TAIL_FIXTURE=1")
	}
	const metrics, rounds, batch, base = 256, 64, 32, 1790000040
	ctx := context.Background()
	var oracle []*store.Series
	for _, perWrite := range []int{batch, 4 * batch} {
		for _, reuse := range []bool{false, true} {
			t.Run(fmt.Sprintf("points=%d/reuse=%t", perWrite, reuse), func(t *testing.T) {
				stamp := int64(base)
				clock := func() time.Time { return time.Unix(stamp+1, 0) }
				codec := &tailFixtureCodec{ContinuationCodec: &ContinuationCodec{ReuseBlockKeys: reuse}, versions: make(map[string]int)}
				db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: clock,
					SyncInterval: time.Hour, CacheSize: 4 << 20, MemTableSize: 8 << 20, LBaseMaxBytes: 1 << 30,
					L0CompactionThreshold: 1000, L0StopWritesThreshold: 2000})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				names := make([]string, metrics)
				for i := range names {
					names[i] = fmt.Sprintf("fixture.sparse.host-%06d.value", i)
					if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				before := db.Stats()
				reuseBefore := SkippedBlockDeletes()
				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)
				start, cpu := time.Now(), tailFixtureCPU()
				input := make([]store.Point, perWrite)
				for first := 0; first < rounds*batch; first += perWrite {
					stamp = int64(base + (first+perWrite)*60)
					for i := range names {
						for j := range input {
							input[j] = store.Point{Timestamp: int64(base + (first+j+1)*60), Value: float64((i+first+j)%97) / 10}
						}
						if err := db.UpdateMany(ctx, names[i], input); err != nil {
							t.Fatal(err)
						}
					}
					if err := db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				updatesCPU, updateWall := tailFixtureCPU()-cpu, time.Since(start)
				compactCPU, compactStart := tailFixtureCPU(), time.Now()
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				compactCPU = tailFixtureCPU() - compactCPU
				compactWall := time.Since(compactStart)
				runtime.ReadMemStats(&memAfter)
				after := db.Stats()
				points := float64(metrics * rounds * batch)
				t.Logf("points=%d updates=%d sets=%d repeated_sets=%d deletes=%d prefixes=%d reused=%d adapter_bytes=%.0f B/point alloc=%.0f B/point update_wall=%s update_cpu=%.3fs compact_wall=%s compact_cpu=%.3fs compact_read=%d compact_write=%d flush_write=%d",
					int(points), metrics*rounds*batch/perWrite, codec.sets, codec.repeats, codec.deletes, codec.prefixes, SkippedBlockDeletes()-reuseBefore,
					float64(codec.bytes)/points, float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/points, updateWall, updatesCPU, compactWall, compactCPU,
					after.CompactionReadBytes-before.CompactionReadBytes, after.CompactionWriteBytes-before.CompactionWriteBytes, after.FlushWriteBytes-before.FlushWriteBytes)
				var got []*store.Series
				for _, name := range names[:8] {
					series, err := db.Fetch(ctx, name, base, base+rounds*batch*60)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, series)
				}
				if oracle == nil {
					oracle = got
				} else {
					for i := range got {
						if got[i].FromTime != oracle[i].FromTime || len(got[i].Values) != len(oracle[i].Values) {
							t.Fatal("coalescing changed the fetched range")
						}
						for j, value := range got[i].Values {
							if math.Float64bits(value) != math.Float64bits(oracle[i].Values[j]) {
								t.Fatalf("metric %d point %d changed", i, j)
							}
						}
					}
				}
			})
		}
	}
}
