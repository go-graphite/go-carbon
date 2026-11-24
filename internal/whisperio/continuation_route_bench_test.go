package whisperio

import (
	"context"
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// The durable working set exceeds a small, warmed block cache. Each write
// changes 124 points of one of 256 classic metrics, with cached writer state.
// Run with -benchtime=4096x to compare an identical write trace across revisions.
func BenchmarkRouteMutationUpdate(b *testing.B) {
	for _, tracked := range []bool{false, true} {
		b.Run(fmt.Sprintf("tracked=%v", tracked), func(b *testing.B) {
			ctx := context.Background()
			const now = int64(1700000000)
			clock := func() time.Time { return time.Unix(now, 0) }
			points := make([]store.Point, 8192)
			for i := range points {
				points[i] = store.Point{Timestamp: now - 8192 + int64(i), Value: math.Float64frombits(0x3ff0000000000000 | (uint64(i) * 11400714819323198485 & 0xfffffffffffff))}
			}
			kv, m := mutationClassicFixture(b, false, []store.Retention{{Step: 1, Count: 16384}, {Step: 8, Count: 4096}}, [][]store.Point{points, nil}, clock)
			db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock, SyncInterval: time.Hour, CacheSize: 8 << 20, CacheShards: 1, MemTableSize: 1 << 20, MetadataCacheSize: 64 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			names := make([]string, 256)
			input := make([]store.Point, 124)
			update := func(i int) {
				for j := range input {
					input[j] = store.Point{Timestamp: now - int64(len(input)) + int64(j), Value: float64(i + j)}
				}
				runCtx, timer := stagetime.Start(ctx)
				timer.Enter(stagetime.Route)
				if err := db.UpdateMany(runCtx, names[i%len(names)], input); err != nil {
					b.Fatal(err)
				}
				timer.Finish()
			}
			for i := range names {
				names[i] = fmt.Sprintf("bench.route.%03d", i)
				record := classicTransfer(kv, store.Snapshot{Metadata: m})
				record.Config.Name, record.Version.Name = names[i], names[i]
				if _, err := db.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
					b.Fatal(err)
				}
				if tracked {
					if _, _, err := db.MutationProof(ctx, names[i], now-8000, now-4000); err != nil {
						b.Fatal(err)
					}
				}
				update(i)
			}
			// Warm writer state, then settle the fixture's flush/compaction work.
			for i := range len(names) * 2 {
				update(i)
			}
			if err := db.Compact(); err != nil {
				b.Fatal(err)
			}
			// Fill the block cache with an incompressible durable working set
			// larger than its budget, while writer metadata remains cached.
			for _, name := range names {
				if _, err := db.Snapshot(ctx, name); err != nil {
					b.Fatal(err)
				}
			}
			stats := db.Stats()
			if stats.CacheBytes < 4<<20 {
				b.Fatal("fixture failed to warm the block cache")
			}
			stages := stagetime.Snapshot()
			reads := store.CountStoreReadsForTest()
			var startCPU, endCPU syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &startCPU); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				update(i)
			}
			b.StopTimer()
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &endCPU); err != nil {
				b.Fatal(err)
			}
			end := db.Stats()
			endStages := stagetime.Snapshot()
			cpu := func(r syscall.Rusage) int64 {
				return r.Utime.Sec*1e9 + int64(r.Utime.Usec)*1000 + r.Stime.Sec*1e9 + int64(r.Stime.Usec)*1000
			}
			b.ReportMetric(float64(cpu(endCPU)-cpu(startCPU))/float64(b.N), "cpu-ns/write")
			b.ReportMetric(float64(reads())/float64(b.N), "continuation-reads/write")
			b.ReportMetric(float64(end.CacheMisses-stats.CacheMisses)/float64(b.N), "block-misses/write")
			b.ReportMetric(float64(end.FS.Ops[store.FSRead][store.FSTable]-stats.FS.Ops[store.FSRead][store.FSTable])/float64(b.N), "table-reads/write")
			b.ReportMetric((endStages[stagetime.Route].Seconds-stages[stagetime.Route].Seconds)*1e9/float64(b.N), "route-wall-ns/write")
			b.Logf("warm block cache %d bytes; table working set exceeds 8MiB", stats.CacheBytes)
		})
	}
}
