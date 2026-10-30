package carbon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Run with -benchtime=1x against TestPrepareExpirationContentionStore's fixture.
// Each case copies the fixture before timing. Absolute scheduled arrival times
// include queueing and GC pauses in p99, without coordinated omission.
func BenchmarkExpirationContention(b *testing.B) {
	source := os.Getenv("EXPIRATION_CONTENTION_DIR")
	if source == "" {
		b.Skip("set EXPIRATION_CONTENTION_DIR to a prepared contention fixture")
	}
	setting := func(name string, fallback int) int {
		if n, _ := strconv.Atoi(os.Getenv(name)); n > 0 {
			return n
		}
		return fallback
	}
	for _, count := range []int{0, 1, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", count), func(b *testing.B) {
			if b.N != 1 {
				b.Fatal("use -benchtime=1x; each sweep consumes its fixture")
			}
			dir := filepath.Join(b.TempDir(), "store")
			if err := os.CopyFS(dir, os.DirFS(source)); err != nil {
				b.Fatal(err)
			}
			warm := os.Getenv("EXPIRATION_CONTENTION_WARM") == "1"
			if warm {
				// Read file pages before opening the store. The active working
				// set is warmed below, as for an already running persister.
				entries, err := os.ReadDir(dir)
				if err != nil {
					b.Fatal(err)
				}
				for _, entry := range entries {
					if !strings.HasSuffix(entry.Name(), ".sst") {
						continue
					}
					file, err := os.Open(filepath.Join(dir, entry.Name()))
					if err != nil {
						b.Fatal(err)
					}
					_, readErr := io.Copy(io.Discard, file)
					if err := errors.Join(readErr, file.Close()); err != nil {
						b.Fatal(err)
					}
				}
			}
			const base = 1790000000
			ctx := context.Background()
			opened := time.Now()
			clock := func() time.Time { return time.Unix(base+int64(time.Since(opened).Seconds()), 0) }
			options := store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true}, TimeBuckets: true,
				CacheSize: int64(setting("EXPIRATION_CONTENTION_CACHE_MB", 256)) << 20, MetadataCacheSize: 64 << 20,
				MemTableSize:      uint64(setting("EXPIRATION_CONTENTION_MEMTABLE_MB", 64)) << 20,
				CompactionWorkers: setting("EXPIRATION_CONTENTION_COMPACTORS", 2), L0CompactionConcurrency: setting("EXPIRATION_CONTENTION_L0_CONCURRENCY", 0), SyncInterval: time.Second, Now: clock}
			db, err := store.Open(dir, options)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			high, err := db.CatalogHighWatermark(ctx)
			if err != nil || high < 4096 {
				b.Fatalf("need >=4096 names: %d %v", high, err)
			}
			b.Logf("names=%d warm=%v cache=%dMiB memtable=%dMiB compactors=%d l0-concurrency=%d", high, warm, options.CacheSize>>20, options.MemTableSize>>20, options.CompactionWorkers, options.L0CompactionConcurrency)
			names := make([]string, min(high/4, 8192))
			for i := range names {
				id := uint64(i+1) * 4
				names[i] = fmt.Sprintf("services.api.pods.pod%010d.requests", id*15485863%4294967296)
			}
			if warm {
				for _, name := range names {
					until := int(clock().Unix())
					if err := db.UpdateMany(ctx, name, []store.Point{{Timestamp: int64(until) - 1, Value: 42}}); err != nil {
						b.Fatal(err)
					}
					if _, err := db.Fetch(ctx, name, until-300, until); err != nil {
						b.Fatal(err)
					}
				}
			}
			r := &metricExpirer{db: db, now: func() time.Time { return time.Unix(base, 0) }, expiration: 24 * time.Hour, rate: 1_000_000, deleteRate: 1_000_000, deleteWorkers: count, stats: &expirationStats{}}
			type job struct {
				at time.Time
				n  int
			}
			writes, reads := make(chan job, 131072), make(chan job, 8192)
			var workers sync.WaitGroup
			var written, read atomic.Uint64
			var firstErr error
			var failure sync.Once
			var mu sync.Mutex
			var writeDelays, readDelays []time.Duration
			load := func(jobs <-chan job, write bool) {
				for range 8 {
					workers.Go(func() {
						var delays []time.Duration
						for j := range jobs {
							name := names[j.n%len(names)]
							var err error
							if write {
								err = db.UpdateMany(ctx, name, []store.Point{{Timestamp: base + int64(j.at.Sub(opened).Seconds()), Value: float64(j.n)}})
								written.Add(1)
							} else {
								until := int(clock().Unix())
								var series *store.Series
								series, err = db.Fetch(ctx, name, until-300, until)
								if err == nil && (series == nil || len(series.Values) == 0) {
									err = errors.New("empty read")
								}
								read.Add(1)
							}
							if err != nil {
								failure.Do(func() { firstErr = err })
							}
							delays = append(delays, time.Since(j.at))
						}
						mu.Lock()
						if write {
							writeDelays = append(writeDelays, delays...)
						} else {
							readDelays = append(readDelays, delays...)
						}
						mu.Unlock()
					})
				}
			}
			load(writes, true)
			load(reads, false)
			var before, after syscall.Rusage
			b.ResetTimer()
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			started := time.Now()
			stop := make(chan struct{})
			deleteCtx, cancel := context.WithTimeout(ctx, time.Duration(setting("EXPIRATION_CONTENTION_SECONDS", 12))*time.Second)
			defer cancel()
			go func() {
				defer close(stop)
				if count == 0 {
					<-deleteCtx.Done()
					return
				}
				if err := r.sweep(deleteCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
					failure.Do(func() { firstErr = err })
				}
			}()
			ticker := time.NewTicker(10 * time.Millisecond)
			next := started.Add(10 * time.Millisecond)
			writeN, readN := 0, 0
		loop:
			for {
				select {
				case <-stop:
					break loop
				case <-ticker.C:
					// Catch up ticks lost to GC or descheduling at their original
					// arrival times: 5400 updates/s and 600 reads/s.
					for due := time.Now(); !next.After(due); next = next.Add(10 * time.Millisecond) {
						for range 54 {
							writes <- job{next, writeN}
							writeN++
						}
						for range 6 {
							reads <- job{next, readN}
							readN++
						}
					}
				}
			}
			ticker.Stop()
			active := time.Since(started)
			completed := written.Load()
			close(writes)
			close(reads)
			workers.Wait()
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			cpu := func(r syscall.Rusage) float64 {
				return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
			}
			cpuSeconds := cpu(after) - cpu(before)
			b.ReportMetric(active.Seconds(), "active-s")
			b.ReportMetric(cpuSeconds/active.Seconds(), "cpu-cores")
			b.ReportMetric(float64(r.stats.deleted.Load())/active.Seconds(), "deletes/s")
			b.ReportMetric(float64(r.stats.examined.Load())/active.Seconds(), "scanned/s")
			b.ReportMetric(float64(completed)/active.Seconds(), "updates/s")
			b.ReportMetric(float64(read.Load())/active.Seconds(), "reads/s")
			b.ReportMetric(float64(writeN)-float64(completed), "queued-writes")
			b.ReportMetric(float64(r.stats.guarded.Load()), "guarded")
			if n := r.stats.deleted.Load(); n > 0 {
				b.ReportMetric(cpuSeconds*1e6/float64(n), "cpu-us/delete")
			}
			for _, samples := range []struct {
				name   string
				values []time.Duration
			}{{"read", readDelays}, {"write", writeDelays}} {
				slices.Sort(samples.values)
				if n := len(samples.values); n > 0 {
					b.ReportMetric(float64(samples.values[(n-1)*99/100].Nanoseconds())/1e6, samples.name+"-p99-ms")
				}
			}
		})
	}
}
