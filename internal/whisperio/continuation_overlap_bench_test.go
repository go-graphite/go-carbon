package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// overlapCycleRetentions are sparse store schemas whose files take the classic
// replay path under the shapes BenchmarkContinuationOverlapUpdate drives.
var overlapCycleRetentions = map[string][]store.Retention{
	"1m:30d,1h:1y,1d:10y": {{Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}},
	"1s:2d,1m:28d,1h:2y":  {{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}},
	"1m:14d,30m:2y":       {{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}},
}

type overlapBenchMetric struct {
	db   *store.Store
	name string
	now  *time.Time
	ts   int64
}

func newOverlapBenchMetric(tb testing.TB, schema string) *overlapBenchMetric {
	tb.Helper()
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	m := &overlapBenchMetric{name: "bench.overlap", now: &now}
	db, err := store.Open(filepath.Join(tb.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true},
		Now: func() time.Time { return *m.now }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	m.db = db
	rets := overlapCycleRetentions[schema]
	if _, err := db.Create(ctx, store.MetricConfig{Name: m.name, Retentions: rets, AggregationMethod: store.Average}); err != nil {
		tb.Fatal(err)
	}
	// Wrap the base archive once, as converted production files have.
	start := int64(1_700_000_000)
	step := int64(rets[0].Step)
	from := start - int64(rets[0].Step*rets[0].Count) - 86400
	for ts := from - from%step; ts < start; ts += 86400 {
		var pts []store.Point
		for x := ts; x < ts+86400 && x < start; x += step {
			pts = append(pts, store.Point{Timestamp: x, Value: float64(x % 997)})
		}
		m.update(tb, ts+86400, pts)
	}
	m.ts = start + 25 // mid-minute, as a 10 s emitter cut by batching is
	return m
}

func (m *overlapBenchMetric) update(tb testing.TB, now int64, pts []store.Point) {
	tb.Helper()
	*m.now = time.Unix(now, 0)
	if err := m.db.UpdateManyMaintained(context.Background(), m.name, pts,
		store.ContinuationMaintenance{MinBytes: 65536, MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute, Allow: func() bool { return true }}); err != nil {
		tb.Fatal(err)
	}
}

// batch returns the next 960 s of a 10 s emitter, ending at the flush time.
func (m *overlapBenchMetric) batch() ([]store.Point, int64) {
	var pts []store.Point
	end := m.ts + 960
	for ; m.ts < end; m.ts += 10 {
		pts = append(pts, store.Point{Timestamp: m.ts, Value: float64(m.ts % 1009)})
	}
	return pts, end
}

func overlapCount() uint64 {
	var n uint64
	for _, k := range whisper.OverlapKinds() {
		n += whisper.OverlapUpdates(k)
	}
	return n
}

// BenchmarkContinuationOverlapUpdate measures updates that take the classic
// replay path, through the shared store as the persister drives it:
//
//   - rollup: a 10 s emitter cut mid-minute after one replay encoded an
//     unfinished coarse window. Without the tail correction every coarse
//     window completion replayed again.
//   - rollup+sidecar: the same, with a late point diverted every hour so a
//     sidecar is present, as on most sparse metrics.
//   - future: a batch carrying one point 30 s ahead of the clock.
//
// The rollup shapes report time, bytes and replays per update of the stream;
// future reports time, bytes and allocations per replayed update.
func BenchmarkContinuationOverlapUpdate(b *testing.B) {
	for _, schema := range []string{"1m:30d,1h:1y,1d:10y", "1s:2d,1m:28d,1h:2y", "1m:14d,30m:2y"} {
		for _, shape := range []string{"rollup", "rollup+sidecar", "future"} {
			if shape != "future" && schema != "1m:30d,1h:1y,1d:10y" {
				continue
			}
			b.Run(fmt.Sprintf("%s/%s", schema, shape), func(b *testing.B) {
				m := newOverlapBenchMetric(b, schema)
				var elapsed time.Duration
				var replays, bytes, allocs uint64
				var before, after runtime.MemStats
				// One replay starts the rollup cycle.
				pts, end := m.batch()
				m.update(b, end, append(pts, store.Point{Timestamp: end + 30, Value: 1}))
				idle := 0
				// The rollup shapes measure every update of the stream, since
				// the tail correction now ends their replay cycle.
				perUpdate := shape != "future"
				var updates uint64
				b.ResetTimer()
				for (perUpdate && updates < uint64(b.N)) || (!perUpdate && replays < uint64(b.N)) {
					if idle++; !perUpdate && idle > 200 {
						b.Fatal("no replay in 200 batches")
					}
					pts, end := m.batch()
					switch shape {
					case "future":
						pts = append(pts, store.Point{Timestamp: end + 30, Value: 1})
					case "rollup+sidecar":
						if end%3600 < 960 {
							// older than the base buffer: diverted to the sidecar
							pts = append(pts, store.Point{Timestamp: end - 3*3600, Value: 2})
						}
					}
					b.StopTimer()
					runtime.ReadMemStats(&before)
					count := overlapCount()
					b.StartTimer()
					started := time.Now()
					m.update(b, end, pts)
					took := time.Since(started)
					b.StopTimer()
					updates++
					if perUpdate {
						runtime.ReadMemStats(&after)
						elapsed += took
						bytes += after.TotalAlloc - before.TotalAlloc
						allocs += after.Mallocs - before.Mallocs
						if overlapCount() != count {
							replays++
						}
					} else if overlapCount() != count {
						runtime.ReadMemStats(&after)
						elapsed += took
						replays++
						bytes += after.TotalAlloc - before.TotalAlloc
						allocs += after.Mallocs - before.Mallocs
						idle = 0
					}
					b.StartTimer()
				}
				b.StopTimer()
				b.ReportMetric(0, "ns/op")
				if perUpdate {
					b.ReportMetric(float64(elapsed.Nanoseconds())/float64(updates), "ns/update")
					b.ReportMetric(float64(bytes)/float64(updates), "B/update")
					b.ReportMetric(1000*float64(replays)/float64(updates), "replays/1k-updates")
					return
				}
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(replays), "ns/replay")
				b.ReportMetric(float64(bytes)/float64(replays), "B/replay")
				b.ReportMetric(float64(allocs)/float64(replays), "allocs/replay")
			})
		}
	}
}
