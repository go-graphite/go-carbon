package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// TestTransferImportConcurrencyBench compares SST ingestion with batch
// commits for concurrent 256-name transfers into a populated LSM. It is a
// manual benchmark: CHUNKSTORE_IMPORT_BENCH=<scratch dir> enables it,
// CHUNKSTORE_IMPORT_BENCH_SEED=<transfers> sizes the pre-populated LSM,
// CHUNKSTORE_IMPORT_BENCH_RUN=<transfers> each measured run and
// CHUNKSTORE_IMPORT_BENCH_COMPACTIONS the target's compaction workers.
func TestTransferImportConcurrencyBench(t *testing.T) {
	dir := os.Getenv("CHUNKSTORE_IMPORT_BENCH")
	if dir == "" {
		t.Skip("set CHUNKSTORE_IMPORT_BENCH=<scratch dir>")
	}
	seedTransfers, _ := strconv.Atoi(os.Getenv("CHUNKSTORE_IMPORT_BENCH_SEED"))
	if seedTransfers == 0 {
		seedTransfers = 200
	}
	runTransfers, _ := strconv.Atoi(os.Getenv("CHUNKSTORE_IMPORT_BENCH_RUN"))
	if runTransfers == 0 {
		runTransfers = 64
	}
	const group = 256
	ctx := context.Background()
	now := time.Unix(1763000000, 0)
	compactions, _ := strconv.Atoi(os.Getenv("CHUNKSTORE_IMPORT_BENCH_COMPACTIONS"))
	if compactions == 0 {
		compactions = 8
	}
	open := func(name string, memtable uint64) *store.Store {
		root := filepath.Join(dir, name)
		_ = os.RemoveAll(root)
		if err := os.MkdirAll(filepath.Join(root, "originals"), 0750); err != nil {
			t.Fatal(err)
		}
		s, err := store.Open(filepath.Join(root, "pool"), store.Options{ExactBlocks: true, CompactionWorkers: compactions, CacheSize: 256 << 20, MemTableSize: memtable, SyncInterval: time.Hour, Now: func() time.Time { return now }, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Join(root, "originals"), TimeBuckets: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close(); _ = os.RemoveAll(root) })
		return s
	}
	// Representative source: 1-minute points for 2 days, 10-minute points for 30
	// days. Continuation blocks sit in hourly buckets of their write time, so
	// write old history once, then the last 90 hours hour by hour (production
	// transfers carry ~93 bucket tables per 256 names).
	var clock atomic.Int64
	clock.Store(now.Unix())
	sourceRoot := filepath.Join(dir, "source")
	_ = os.RemoveAll(sourceRoot)
	if err := os.MkdirAll(filepath.Join(sourceRoot, "originals"), 0750); err != nil {
		t.Fatal(err)
	}
	source, err := store.Open(filepath.Join(sourceRoot, "pool"), store.Options{ExactBlocks: true, CacheSize: 256 << 20, MemTableSize: 256 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(clock.Load(), 0) }, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Join(sourceRoot, "originals"), TimeBuckets: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close(); _ = os.RemoveAll(sourceRoot) })
	cfg := func(name string) store.MetricConfig {
		return store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 2880}, {Step: 600, Count: 4320}}, AggregationMethod: store.Average}
	}
	base := make([]string, group)
	series := make([][]store.Point, group)
	end := now.Unix() - now.Unix()%3600
	for i := range base {
		base[i] = fmt.Sprintf("bench.vm.host%04d.cpu.core%02d", i, i%32)
		r := rand.New(rand.NewSource(int64(i)))
		level := 100 * r.Float64()
		for ts := end - 30*86400; ts < end; ts += 60 {
			level = math.Max(0, level+r.NormFloat64())
			if ts < end-2*86400 && ts%600 != 0 {
				continue
			}
			series[i] = append(series[i], store.Point{Timestamp: ts, Value: math.Round(level*100) / 100})
		}
		if _, err := source.Create(ctx, cfg(base[i])); err != nil {
			t.Fatal(err)
		}
	}
	writeWindow := func(from, to int64) {
		var wg sync.WaitGroup
		for w := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w; i < group; i += 16 {
					lo, _ := slices.BinarySearchFunc(series[i], from, func(p store.Point, ts int64) int { return int(p.Timestamp - ts) })
					hi, _ := slices.BinarySearchFunc(series[i], to, func(p store.Point, ts int64) int { return int(p.Timestamp - ts) })
					if lo < hi {
						if err := source.UpdateMany(ctx, base[i], series[i][lo:hi]); err != nil {
							t.Error(err)
						}
					}
				}
			}()
		}
		wg.Wait()
	}
	clock.Store(end - 90*3600)
	writeWindow(0, end-90*3600)
	for h := end - 90*3600; h < end; h += 3600 {
		clock.Store(h + 3599)
		writeWindow(h, h+3600)
	}
	clock.Store(now.Unix())
	exported, err := source.ExportTransfer(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	payload := 0
	for _, r := range exported {
		for _, kv := range r.Keys {
			payload += len(kv.Key) + len(kv.Value)
		}
	}
	t.Logf("transfer: %d names, %.1f KB/name, %.1f MB/transfer, keys/name %.1f", len(exported), float64(payload)/group/1e3, float64(payload)/1e6, float64(func() int {
		n := 0
		for _, r := range exported {
			n += len(r.Keys)
		}
		return n
	}())/group)
	var serial atomic.Int64
	clone := func(prefix string) []store.TransferMetric {
		id := serial.Add(1)
		out := make([]store.TransferMetric, len(exported))
		for i, r := range exported {
			r.Config.Name = fmt.Sprintf("%s.t%05d.%s", prefix, id, r.Config.Name)
			r.Version.Name = r.Config.Name
			r.Keys = slices.Clone(r.Keys)
			out[i] = r
		}
		return out
	}
	memtable, _ := strconv.Atoi(os.Getenv("CHUNKSTORE_IMPORT_BENCH_MEMTABLE_MB"))
	if memtable == 0 {
		memtable = 256
	}
	target := open("target", uint64(memtable)<<20)
	importAll := func(streams, transfers int, prefix string) (time.Duration, []string) {
		var next atomic.Int64
		var mu sync.Mutex
		var names []string
		began := time.Now()
		var wg sync.WaitGroup
		for range streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for next.Add(1) <= int64(transfers) {
					records := clone(prefix)
					if _, err := target.ImportTransfer(ctx, records); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					names = append(names, records[0].Config.Name, records[len(records)/2].Config.Name)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		return time.Since(began), names
	}
	// Populate the LSM through the batch path (fast), then let it settle.
	elapsed, readable := importAll(32, seedTransfers, "seed")
	st := target.Stats()
	t.Logf("seeded %d names in %v (%.0f names/s), L0 sublevels %d", seedTransfers*group, elapsed.Round(time.Millisecond), float64(seedTransfers*group)/elapsed.Seconds(), st.L0Sublevels)
	time.Sleep(5 * time.Second)
	t.Logf("LSM after seed:\n%s", target.PebbleMetricsForTest())
	cpu := func() time.Duration {
		var ru syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	for _, streams := range []int{16, 32, 64} {
		for _, mode := range []struct {
			name  string
			limit int
		}{{"ingest", -1}, {"batch", 0}} {
			target.SetTransferBatchBytesForTest(mode.limit)
			stop := make(chan struct{})
			var latencies []time.Duration
			var readers sync.WaitGroup
			readers.Add(1)
			go func() {
				defer readers.Done()
				r := rand.New(rand.NewSource(7))
				for {
					select {
					case <-stop:
						return
					default:
					}
					began := time.Now()
					if _, err := target.Snapshot(ctx, readable[r.Intn(len(readable))]); err != nil {
						t.Error(err)
						return
					}
					latencies = append(latencies, time.Since(began))
					time.Sleep(2 * time.Millisecond)
				}
			}()
			before, cpu0 := target.Stats(), cpu()
			elapsed, _ := importAll(streams, runTransfers, fmt.Sprintf("%s%d", mode.name, streams))
			cpuUsed := cpu() - cpu0
			close(stop)
			readers.Wait()
			after := target.Stats()
			slices.Sort(latencies)
			pct := func(p float64) time.Duration {
				if len(latencies) == 0 {
					return 0
				}
				return latencies[min(len(latencies)-1, int(p*float64(len(latencies))))]
			}
			names := runTransfers * group
			t.Logf("RESULT mode=%s streams=%d names=%d wall=%.1fs names/s=%.0f MB/s=%.1f cores=%.1f ingest_tables/transfer=%.1f batch_commits/transfer=%.2f read_p50=%v read_p99=%v reads=%d L0sub=%d stalls=%d stall_s=%.1f",
				mode.name, streams, names, elapsed.Seconds(), float64(names)/elapsed.Seconds(), float64(payload)*float64(runTransfers)/elapsed.Seconds()/1e6,
				cpuUsed.Seconds()/elapsed.Seconds(), float64(after.IngestTables-before.IngestTables)/float64(runTransfers), float64(after.TransferBatchCommits-before.TransferBatchCommits)/float64(runTransfers),
				pct(.5).Round(time.Microsecond), pct(.99).Round(time.Microsecond), len(latencies), after.L0Sublevels, after.WriteStalls-before.WriteStalls, time.Duration(after.WriteStallNanos-before.WriteStallNanos).Seconds())
		}
	}
	t.Logf("LSM at end:\n%s", target.PebbleMetricsForTest())
}
