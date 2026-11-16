package main

import (
	"encoding/json"
	"fmt"
	whisper "github.com/go-graphite/go-whisper"
	"math"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Actual core queues + original batch hook + atomic raw/archive/source-frontier
// ingest. One producer, one owner/Pebble per allocated core. No durable input
// log is timed here; -benchtime=100x is a finite seal cost, not sustained proof.
func BenchmarkCoreRollupSeal(b *testing.B) {
	for _, density := range []int{1, 4, 16} {
		for _, archives := range []int{1, 4} {
			b.Run(fmt.Sprintf("density%d/archives%d", density, archives), func(b *testing.B) {
				cfg := coreConfig{Shards: runtime.GOMAXPROCS(0), Producers: 1, MaxRows: 1 << 18, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "core-rollup-benchmark"}
				e, _, err := openCoreRollups(b.TempDir(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				inputs := atomicBenchInput(256, density, archives)
				batches := make([]coreBatch, len(inputs))
				for i, in := range inputs {
					policy, err := json.Marshal(in.Config)
					if err != nil {
						b.Fatal(err)
					}
					batches[i] = coreBatch{ID: in.ID, Policy: policy, Points: make([]row, density)}
				}
				b.ReportAllocs()
				b.ResetTimer()
				start := atomicCPU()
				for n := 0; n < b.N; n++ {
					order := uint64(n + 1)
					advanceAtomicInputs(inputs, order, density)
					for i, in := range inputs {
						batch := &batches[i]
						batch.Order = order
						batch.Clock = in.Batches[0].Now
						for j, p := range in.Batches[0].Points {
							batch.Points[j] = row{ID: in.ID, Time: p.Time, Bits: p.Bits, Seq: p.Sequence}
						}
					}
					if err = e.submitBatches(0, batches, order, true); err != nil {
						b.Fatal(err)
					}
				}
				if err = e.close(); err != nil {
					b.Fatal(err)
				}
				cpu := atomicCPU() - start
				b.StopTimer()
				points := float64(b.N * 256 * density)
				b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
				b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
			})
		}
	}
}

// Fixed point count at each supplied original-apply density. One producer per
// eight owners fills and retains compact original batches, then submits a single
// seal barrier to its disjoint owner group. CPU includes generation, compression,
// expansion, routing, core seal, rollups, publication and close. Journal I/O is excluded.
// These are finite cost samples, never a settled-compaction qualification.
func BenchmarkCoreRollupProductionDensity(b *testing.B) {
	density, err := strconv.Atoi(os.Getenv("SEAL_BENCH_DENSITY"))
	if err != nil || (density != 24 && density != 118 && density != 513) {
		b.Skip("set SEAL_BENCH_DENSITY to 24, 118 or 513")
	}
	metrics := max(1, (1<<21)/density)
	if s := os.Getenv("SEAL_BENCH_METRICS"); s != "" {
		metrics, err = strconv.Atoi(s)
		if err != nil || metrics < 1 {
			b.Fatal("invalid SEAL_BENCH_METRICS")
		}
	}
	cores := runtime.GOMAXPROCS(0)
	warmup := 0
	if s := os.Getenv("SEAL_BENCH_WARMUP"); s != "" {
		warmup, err = strconv.Atoi(s)
		if err != nil || warmup < 0 || warmup > 10000 {
			b.Fatal("invalid SEAL_BENCH_WARMUP")
		}
	}
	fixedClock := os.Getenv("SEAL_BENCH_FIXED_CLOCK") == "1"
	shards, owners, producers := sealBenchmarkGeometry(b, cores)
	compact := os.Getenv("SEAL_BENCH_COMPACT") != "0"
	cfg := coreConfig{Shards: shards, Owners: owners, Producers: producers, MaxRows: max(density, 2*metrics*density/shards), PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "production-density-cost"}
	policyLimit, err := sealrollup.NewHeadPolicy(20_000_000, uint64(cfg.MaxRows)*uint64(shards)*96, 96, shards)
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	e, _, err := openCoreRollupsWithHeadPolicy(root, cfg, policyLimit)
	if err != nil {
		b.Fatal(err)
	}
	distribution := loadSealDistribution(b)
	inputs := atomicBenchInput(1, 1, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([][]uint64, producers)
	offset := sealBenchmarkIDOffset(b)
	for i := range metrics {
		id := offset + uint64(i)
		p := coreShard(id, shards) % producers
		if producers > shards {
			p = coreShard(id, producers)
		}
		ids[p] = append(ids[p], id)
	}
	jobs := make([]chan int, producers)
	errs := make(chan error, producers)
	headBytes := make([]int, producers)
	var workers sync.WaitGroup
	for p := range producers {
		jobs[p] = make(chan int)
		workers.Go(func() {
			h := compactRollupHead{batches: make([]compactRollupBatch, 0, len(ids[p]))}
			scratch := make([]row, density)
			rawRows := []row(nil)
			rawBatches := []coreBatch(nil)
			if !compact {
				rawRows = make([]row, len(ids[p])*density)
				rawBatches = make([]coreBatch, len(ids[p]))
				for i, id := range ids[p] {
					rawBatches[i] = coreBatch{ID: id, Policy: policy, Points: rawRows[i*density : (i+1)*density]}
				}
			}
			for n := range jobs[p] {
				order := uint64(n + 1)
				now := int64(1794700800 + (n+1)*density)
				if fixedClock {
					now = int64(1794700800 + density)
				}
				var failure error
				for i, id := range ids[p] {
					points := scratch
					if !compact {
						points = rawBatches[i].Points
					}
					fillSealRows(points, id, now, n, distribution)
					if compact {
						failure = h.append(coreBatch{ID: id, Order: order, Clock: now, Policy: policy, Points: points})
						if failure != nil {
							break
						}
					} else {
						rawBatches[i].Order, rawBatches[i].Clock = order, now
					}
				}
				batches := rawBatches
				if compact && failure == nil {
					headBytes[p] = h.residentBytes()
					failure = e.submitCompactRollupGroup(p, &h, order)
				} else if !compact && failure == nil {
					headBytes[p] = len(rawRows)*int(unsafe.Sizeof(row{})) + len(rawBatches)*int(unsafe.Sizeof(coreBatch{}))
					failure = e.submitRollupGroup(p, batches, order)
				}
				h.reset()
				errs <- failure
			}
		})
	}
	for n := range warmup {
		for _, job := range jobs {
			job <- n
		}
		for range producers {
			if err := <-errs; err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
	sealBenchmarkBarrier(b)
	stopProfile := sealBenchmarkProfile(b)
	gcStart := sealBenchmarkGCCPU()
	classes := []runtimemetrics.Sample{
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/dedicated:cpu-seconds"},
	}
	runtimemetrics.Read(classes)
	assistStart, dedicatedStart := classes[0].Value.Float64(), classes[1].Value.Float64()
	// Optional paired profiles exclude opening, warm-up and close. Subtract
	// alloc_objects/alloc_space, rather than attributing a whole-process profile
	// to steady seals. Profile flushing and forced GC are outside the timer.
	heapProfile := os.Getenv("SEAL_BENCH_WARM_HEAP")
	writeHeap := func(suffix string) {
		if heapProfile == "" {
			return
		}
		runtime.GC()
		runtime.GC()
		f, err := os.Create(heapProfile + suffix + ".pprof")
		if err != nil {
			b.Fatal(err)
		}
		if err = pprof.WriteHeapProfile(f); err != nil {
			b.Fatal(err)
		}
		if err = f.Close(); err != nil {
			b.Fatal(err)
		}
	}
	writeHeap("-before")
	b.ResetTimer()
	ioStart := sealProcessWritten()
	start := atomicCPU()
	for n := 0; n < b.N; n++ {
		for _, job := range jobs {
			job <- n + warmup
		}
		for range producers {
			if err := <-errs; err != nil {
				b.Fatal(err)
			}
		}
	}
	if heapProfile != "" {
		b.StopTimer()
		writeHeap("-after")
		b.StartTimer()
	}
	for _, job := range jobs {
		close(job)
	}
	workers.Wait()
	var compaction, stage uint64
	for _, o := range e.owners {
		stage += o.bytes.Load()
		m := o.db.Metrics()
		for _, l := range m.Levels {
			compaction += l.BytesCompacted
		}
	}
	closeStart, closeCPU := time.Now(), atomicCPU()
	if err = e.close(); err != nil {
		b.Fatal(err)
	}
	closeCPU = atomicCPU() - closeCPU
	closeWall := time.Since(closeStart)
	cpu := atomicCPU() - start
	b.StopTimer()
	gcCPU := sealBenchmarkGCCPU() - gcStart
	runtimemetrics.Read(classes)
	assistCPU := classes[0].Value.Float64() - assistStart
	dedicatedCPU := classes[1].Value.Float64() - dedicatedStart
	stopProfile()
	points := float64(b.N * metrics * density)
	written := sealProcessWritten() - ioStart
	var disk int64
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			st, e := d.Info()
			if e != nil {
				return e
			}
			disk += st.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(written)/points, "written-B/point")
	b.ReportMetric(float64(stage)/points, "staged-SST-B/point")
	b.ReportMetric(float64(compaction)/points, "compaction-B/point")
	b.ReportMetric(float64(disk)/points, "closed-disk-B/point")
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	runtime.KeepAlive(e)
	held := 0
	for _, n := range headBytes {
		held += n
	}
	b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
	b.ReportMetric(gcCPU*1e9/points, "gc-cpu-ns/point")
	b.ReportMetric(assistCPU*1e9/points, "gc-assist-ns/point")
	b.ReportMetric(dedicatedCPU*1e9/points, "gc-dedicated-ns/point")
	b.ReportMetric(float64(warmup), "warmup-seals")
	b.ReportMetric(float64(closeCPU)/points, "close-cpu-ns/point")
	b.ReportMetric(float64(closeWall.Nanoseconds())/points, "close-wall-ns/point")
	b.ReportMetric(float64(shards), "physical-DBs")
	b.ReportMetric(float64(owners), "execution-owners")
	b.ReportMetric(float64(held)/float64(metrics*density), "head-B/point")
	b.ReportMetric(float64(memory.HeapAlloc), "postseal-heap-bytes")
	b.ReportMetric(float64(memory.HeapAlloc)/float64(metrics*density), "engine-heap-B/point")
	b.ReportMetric(float64(density), "points/metric")
	b.ReportMetric(float64(producers), "producers")

}

// Fixed production-like sparse cardinality as cores change: 262144 metrics,
// one original point per metric per seal, and all four archives. Large owner
// files avoid shrinking the workload to a few metrics as owners increase.
// Three finite seals include close; this is not sustained compaction proof.
func BenchmarkCoreRollupSparse(b *testing.B) {
	const metrics = 1 << 18
	cores := runtime.GOMAXPROCS(0)
	cfg := coreConfig{Shards: cores, Producers: 1, MaxRows: 1 << 18, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "sparse-rollup-benchmark"}
	e, _, err := openCoreRollups(b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	inputs := atomicBenchInput(metrics, 1, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		b.Fatal(err)
	}
	batches := make([]coreBatch, metrics)
	for i := range batches {
		batches[i] = coreBatch{ID: uint64(i), Policy: policy, Points: make([]row, 1)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := atomicCPU()
	for n := 0; n < b.N; n++ {
		order := uint64(n + 1)
		now := int64(1794700800 + n)
		for i := range batches {
			batch := &batches[i]
			batch.Order = order
			batch.Clock = now
			batch.Points[0] = row{ID: uint64(i), Time: now, Bits: math.Float64bits(float64(n % 97)), Seq: order}
		}
		if err = e.submitBatches(0, batches, order, true); err != nil {
			b.Fatal(err)
		}
	}
	if err = e.close(); err != nil {
		b.Fatal(err)
	}
	cpu := atomicCPU() - start
	b.StopTimer()
	points := float64(b.N * metrics)
	b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
}

// Linux physical write accounting includes staging and compaction writes. It is
// separate from logical SST sizes and is unavailable on local non-Linux tests.
func sealProcessWritten() uint64 {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "write_bytes:") {
			n, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64)
			return n
		}
	}
	return 0
}

// Matched-density incumbent CPU reference: actual classic UpdateMany on sparse
// files, same four archives, clock and captured values. Each worker owns its
// file. Cardinality is one active file/core; report this scope with the result.
func BenchmarkClassicRollupProductionDensity(b *testing.B) {
	density, err := strconv.Atoi(os.Getenv("SEAL_BENCH_DENSITY"))
	if err != nil || (density != 24 && density != 118 && density != 513) {
		b.Skip("set matched density")
	}
	distribution := loadSealDistribution(b)
	cores := runtime.GOMAXPROCS(0)
	root := b.TempDir()
	writers := make([]*whisper.Whisper, cores)
	clocks := make([]int64, cores)
	for i := range writers {
		clocks[i] = 1794700800
		writers[i], err = whisper.CreateWithOptions(filepath.Join(root, fmt.Sprintf("classic-%d.wsp", i)), whisper.MustParseRetentionDefs("1s:1d,1m:30d,1h:1y,1d:10y"), whisper.Average, 0, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clocks[i], 0) }})
		if err != nil {
			b.Fatal(err)
		}
	}
	var ids atomic.Uint64
	b.ResetTimer()
	ioStart := sealProcessWritten()
	start := atomicCPU()
	b.RunParallel(func(pb *testing.PB) {
		id := int(ids.Add(1)) - 1
		rows := make([]row, density)
		values := make([]whisper.TimeSeriesPoint, density)
		points := make([]*whisper.TimeSeriesPoint, density)
		for j := range points {
			points[j] = &values[j]
		}
		n := 0
		for pb.Next() {
			n++
			now := int64(1794700800 + n*density)
			clocks[id] = now
			fillSealRows(rows, uint64(id), now, n-1, distribution)
			for j, p := range rows {
				values[j] = whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
			}
			if err := writers[id].UpdateMany(points); err != nil {
				b.Error(err)
				return
			}
		}
	})
	for _, w := range writers {
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	cpu := atomicCPU() - start
	b.StopTimer()
	written := sealProcessWritten() - ioStart
	points := float64(b.N * density)
	b.ReportMetric(float64(written)/points, "written-B/point")
	b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
}
