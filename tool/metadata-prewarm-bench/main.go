//go:build darwin || linux

// metadata-prewarm-bench measures one full write cycle of a private store.
// Build a fixture once, then run each mode against a fresh copy of it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"iter"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

var completed atomic.Int64

const base = int64(1_790_000_000)

func name(i int) string {
	return fmt.Sprintf("aggregations.secondly.service%04d.host%06d.by_region.eu.requests.count", i%1000, i)
}

func cpu() float64 {
	var ru syscall.Rusage
	check(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func emit(v any) { check(json.NewEncoder(os.Stdout).Encode(v)) }

func main() {
	pendingRestore := flag.Bool("pending-restore", false, "restore real dump/checkpoint history and admit live points while gated")
	savedPoints := flag.Int("restore-points", 92, "pending points per metric, including captured input")
	gatePoints := flag.Int("gate-input-points", 90, "new points per metric during the gate; positive multiple of3")
	backgroundCores := flag.Float64("background-cores", 0, "mandatory in-process CPU load during observation")
	warmTimeout := flag.Duration("warm-timeout", 0, "stop optional warming after this duration, including with -floor")
	stopAfter := flag.Duration("backlog-stop-after", 0, "end a starvation control without requiring full drain")
	lazyCheckpoint := flag.Bool("lazy-checkpoint", false, "validate checkpoint revisions at first use only")
	noPrewarm := flag.Bool("no-prewarm", false, "backlog mode: skip warming without priming metadata")
	deadlineOrder := flag.Bool("deadline-order", false, "prototype: warm in startup spread deadline order")
	observe := flag.Duration("observe", 0, "minimum observation window, including background work after drain")
	cpuCores := flag.Float64("cpu-cores", 0, "shared whole-process CPU allowance; 0 unpaced")
	checkpoint := flag.Bool("checkpoint", false, "save clean checkpoint and load it before scanning")
	extraNames := flag.Int("extra-names", 0, "expand mode: extra inactive catalog names")
	readRate := flag.Int("read-rate", 0, "concurrent snapshot reads per second")
	dir := flag.String("dir", "", "private store directory (required)")
	pendingCatalog := flag.Bool("pending-catalog", false, "select pending names through a catalog filter")
	priority := flag.Bool("priority", false, "offer pending names in write order before scanning the catalog")
	writers := flag.Int("writers", 8, "concurrent update workers")
	workers := flag.Int("workers", 0, "prewarm workers; 0 auto")
	window := flag.Duration("window", 0, "minimum write-cycle duration; 0 unpaced")
	gate := flag.Duration("gate", 0, "background persister wait deadline; 0 no wait")
	legacyCatchup := flag.Bool("legacy-catchup", false, "backlog mode: process-start spread and pending-recovery rate lift")
	adaptiveGate := flag.Bool("adaptive-gate", false, "backlog mode: project scan completion within -gate cap")
	backlogMinPoints := flag.Int("backlog-min-points", 2000, "backlog batching threshold")
	backlogSpread := flag.Float64("backlog-delay-spread", .25, "backlog ordinary delay spread")
	backlogStartupSpread := flag.Bool("backlog-startup-spread", false, "backlog explicit startup/restore spreading")
	updates := flag.Int("updates-per-second", 0, "backlog update limit; 0 uses metrics/window")
	pendingEvery := flag.Int("pending-every", 1, "select one pending name in this many for the first catalog pass")
	workingFloor := flag.Bool("working-set-floor", false, "prime only the metrics written, excluding inactive catalog names")
	floor := flag.Bool("floor", false, "prime metadata without writes before measuring the identical first cycle")
	mode := flag.String("mode", "cold", "build, age, cold, prewarm, metadata, background, backlog or steady")
	metrics := flag.Int("metrics", 1_000_000, "metrics in fixture")
	batch := flag.Int("batch", 0, "metrics per catalog read view; 0 uses store default")
	byteRate := flag.Int("byte-rate", 0, "logical key/value read bytes/s; 0 unpaced")
	rate := flag.Int("rate", 0, "prewarm metrics/s; 0 unpaced for isolated cost")
	cache := flag.Int64("metadata-bytes", 4<<30, "metadata cache budget")
	blocks := flag.Int64("block-bytes", 32<<20, "Pebble block cache budget")
	flag.Parse()
	if *dir == "" || *metrics <= 0 || *pendingEvery <= 0 || *writers <= 0 || *workers < 0 || *gate < 0 || *window < 0 || *rate < 0 || *byteRate < 0 || *cpuCores < 0 || *backlogMinPoints <= 0 || *updates < 0 || *backgroundCores < 0 || *warmTimeout < 0 || *stopAfter < 0 {
		panic("positive -metrics and private -dir required")
	}
	if *metrics%104729 == 0 || *metrics%999983 == 0 {
		panic("-metrics must be coprime to the creation and write strides (104729 and 999983)")
	}
	if (*pendingRestore || *mode == "pending-fixture") && (*metrics < 2 || *savedPoints < 2 || *gatePoints <= 0 || *gatePoints%3 != 0) {
		panic("pending restore needs metrics>=2, saved>=2 and positive gate points divisible by3")
	}
	if *mode == "pending-fixture" {
		buildPending(*dir, *metrics, *savedPoints)
		return
	}
	ctx := context.Background()
	now := time.Unix(base+120, 0)
	if *pendingRestore {
		now = time.Unix(base+360, 0)
	}
	metadata := *cache
	if *mode == "build" || *mode == "age" || *mode == "expand" {
		metadata = 0
	}
	if *mode == "build" {
		if _, err := os.Stat(*dir); !os.IsNotExist(err) {
			panic("build needs a new directory")
		}
		metadata = 0
		now = time.Unix(base, 0)
	}
	var codec store.ContinuationCodec = &whisperio.ContinuationCodec{}

	db, err := store.Open(*dir, store.Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true, MetadataCacheCheckpoint: *checkpoint, MetadataCacheCheckpointLazy: *lazyCheckpoint,
		Now: func() time.Time { return now }, SyncInterval: time.Second, CacheSize: *blocks, MemTableSize: 32 << 20,
		MaxUnflushedMemTableBytes: 128 << 20, MetadataCacheSize: metadata, BloomFilterBits: 10, CacheShards: 4})
	check(err)
	defer func() {
		at, used := time.Now(), cpu()
		check(db.Close())
		check(db.MetadataCheckpointError())
		if *checkpoint {
			info, err := os.Stat(*dir + "/metadata-cache-v1")
			check(err)
			emit(map[string]any{"mode": "checkpoint-save", "wall": time.Since(at).Seconds(), "cpu": cpu() - used, "bytes": info.Size()})
		}
	}()
	if *mode == "expand" {
		at, used := time.Now(), cpu()
		for i := range *extraNames {
			_, err := db.Create(ctx, store.MetricConfig{Name: fmt.Sprintf("expired.catalog.%09d", i), Retentions: []store.Retention{{Step: 60, Count: 10}}, AggregationMethod: store.Average})
			check(err)
		}
		check(db.Flush())
		emit(map[string]any{"mode": "expand", "extra_names": *extraNames, "wall": time.Since(at).Seconds(), "cpu": cpu() - used})
		return
	}
	budget := restartcpu.New(*cpuCores)
	if *mode == "build" {
		started, used := time.Now(), cpu()
		// Coprime stride separates creation/ID order from catalog order.
		for j := range *metrics {
			i := int(int64(j) * 104729 % int64(*metrics))
			_, err := db.Create(ctx, store.MetricConfig{Name: name(i), Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}}, AggregationMethod: store.Average, XFilesFactor: .5})
			check(err)
			points := make([]store.Point, 30)
			for k := range points {
				points[k] = store.Point{Timestamp: base - 30 + int64(k), Value: float64(i%100 + k)}
			}
			check(db.UpdateMany(ctx, name(i), points))
			if j%100000 == 99999 {
				fmt.Fprintf(os.Stderr, "seeded %d metrics in %s\n", j+1, time.Since(started))
			}
		}
		check(db.Compact())
		emit(map[string]any{"mode": *mode, "metrics": *metrics, "wall": time.Since(started).Seconds(), "cpu": cpu() - used, "disk_bytes": db.Stats().DiskBytes})
		return
	}
	if *mode == "age" {
		started, used := time.Now(), cpu()
		input := make([]store.Point, 30, 31)
		for k := range input {
			input[k] = store.Point{Timestamp: base + int64(k), Value: float64(k)}
		}
		for j := range *metrics {
			i := int(int64(j) * 999983 % int64(*metrics))
			points := input
			if i%8 == 0 {
				points = append(points, store.Point{Timestamp: base - 40, Value: .5})
			}
			check(db.UpdateMany(ctx, name(i), points))
		}
		check(db.Flush())
		emit(map[string]any{"mode": *mode, "wall": time.Since(started).Seconds(), "cpu": cpu() - used, "disk_bytes": db.Stats().DiskBytes})
		return
	}

	progress := new(store.PrewarmProgress)
	prewarm := func(ctx context.Context) store.PrewarmStats {
		started, used := time.Now(), cpu()
		var names iter.Seq[string]
		if *deadlineOrder {
			order := make([]uint64, *metrics)
			for i := range order {
				h := uint64(14695981039346656037)
				for _, c := range []byte(name(i)) {
					h ^= uint64(c)
					h *= 1099511628211
				}
				h ^= h >> 33
				h *= 0xff51afd7ed558ccd
				h ^= h >> 33
				h *= 0xc4ceb9fe1a85ec53
				h ^= h >> 33
				order[i] = uint64(uint32(h)>>16)<<48 | uint64(i)
			}
			slices.Sort(order)
			names = func(yield func(string) bool) {
				for _, entry := range order {
					if !yield(name(int(entry & ((1 << 48) - 1)))) {
						return
					}
				}
			}
		} else if *priority || *pendingCatalog {
			names = func(yield func(string) bool) {
				for j := 0; j < *metrics; j += *pendingEvery {
					if !yield(name(int(int64(j) * 999983 % int64(*metrics)))) {
						return
					}
				}
			}
		}
		options := store.PrewarmOptions{Wait: budget.Wait, Progress: progress, Workers: *workers, Rate: *rate, ByteRate: *byteRate, Batch: *batch}
		if *checkpoint && *mode != "checkpoint" {
			stats, err := db.LoadMetadataCheckpointAfter(ctx, options)
			check(err)
			emit(map[string]any{"mode": "checkpoint-load", "stats": stats, "cpu": cpu() - used, "wall": time.Since(started).Seconds()})
			if stats.Valid {
				return store.PrewarmStats{}
			}
		}
		if *mode == "checkpoint" {
			names = func(yield func(string) bool) {
				for i := range *metrics {
					if !yield(name(i)) {
						return
					}
				}
			}
		}
		var stats store.PrewarmStats
		var err error
		if *pendingCatalog {
			stats, err = db.PrewarmPendingMetadata(ctx, names, options)
		} else {
			stats, err = db.PrewarmMetadata(ctx, names, options)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			emit(map[string]any{"mode": "prewarm-stopped", "reason": err.Error(), "wall": time.Since(started).Seconds(), "stats": stats})
			return stats
		}
		check(err)
		if *pendingCatalog && stats.Filtered > 0 {
			remaining, err := db.PrewarmMetadata(ctx, nil, options)
			check(err)
			stats.Names += remaining.Names
			stats.Prewarmed += remaining.Prewarmed
			stats.Bytes += remaining.Bytes
			stats.ReadBytes += remaining.ReadBytes
			stats.Cached += remaining.Cached
			stats.Absent += remaining.Absent
			stats.Raced += remaining.Raced
			stats.Full += remaining.Full
			stats.Failed += remaining.Failed
			stats.Duration += remaining.Duration
			stats.BudgetFull = remaining.BudgetFull
		}
		emit(map[string]any{"mode": "prewarm", "stats": stats, "wall": time.Since(started).Seconds(), "cpu": cpu() - used})
		return stats
	}
	cycle := func(label string, stamp int64) {
		before := stagetime.Snapshot()
		var mem0, mem1 runtime.MemStats
		runtime.ReadMemStats(&mem0)
		started, used := time.Now(), cpu()
		input := make([]store.Point, 30)
		for k := range input {
			input[k] = store.Point{Timestamp: stamp - 30 + int64(k), Value: float64(k)}
		}
		reads := store.CountStoreReadsForTest()
		var wg sync.WaitGroup
		for worker := range *writers {
			wg.Go(func() {
				for j := worker; j < *metrics; j += *writers {
					if *window > 0 {
						due := started.Add(time.Duration(float64(j) / float64(*metrics) * float64(*window)))
						if delay := time.Until(due); delay > 0 {
							time.Sleep(delay)
						}
					}
					metric := name(int(int64(j) * 999983 % int64(*metrics)))
					start := time.Now()
					update, timer := stagetime.Start(ctx)
					timer.Enter(stagetime.Route)
					_, err := db.Metadata(update, metric)
					check(err)
					check(db.UpdateMany(update, metric, input))
					timer.Finish()
					stagetime.ObserveUpdate(time.Since(start))
					completed.Add(1)
				}
			})
		}
		wg.Wait()
		logical := reads()
		wall, used := time.Since(started).Seconds(), cpu()-used
		runtime.ReadMemStats(&mem1)
		after := stagetime.Snapshot()
		stages := map[string]float64{}
		for s := range after {
			stages[after[s].Stage.String()] = after[s].Seconds - before[s].Seconds
		}
		emit(map[string]any{"mode": label, "metrics": *metrics, "wall": wall, "cpu": used, "stages": stages,
			"logical_reads": logical, "allocated_bytes": mem1.TotalAlloc - mem0.TotalAlloc, "cache": db.Stats().StateCache})
	}
	if *mode == "checkpoint" || *mode == "restore" {
		prewarm(ctx)
		return
	}
	if *floor {
		var names iter.Seq[string]
		if *workingFloor {
			names = func(yield func(string) bool) {
				for i := range *metrics {
					if !yield(name(i)) {
						return
					}
				}
			}
		}
		stats, err := db.PrewarmMetadata(ctx, names, store.PrewarmOptions{Workers: *workers, Batch: *batch})
		check(err)
		emit(map[string]any{"mode": "floor-fill", "stats": stats})
		runtime.GC()
	}
	budget = restartcpu.New(*cpuCores)
	stopBackground := backgroundCPU(*backgroundCores)
	defer stopBackground()
	registry := prometheus.NewRegistry()
	if observed, ok := any(budget).(interface{ InitPrometheus(prometheus.Registerer) }); ok {
		observed.InitPrometheus(registry)
	}
	stopReads := make(chan struct{})
	readsDone := make(chan struct{})
	stopReaders := sync.OnceFunc(func() { close(stopReads); <-readsDone })
	defer stopReaders()
	go func() {
		defer close(readsDone)
		if *readRate <= 0 {
			return
		}
		ticker := time.NewTicker(time.Second / time.Duration(*readRate))
		defer ticker.Stop()
		var latency []float64
		i := 0
		for {
			select {
			case <-ticker.C:
				at := time.Now()
				_, err := db.Snapshot(ctx, name(i%*metrics))
				check(err)
				latency = append(latency, time.Since(at).Seconds())
				i++
			case <-stopReads:
				sort.Float64s(latency)
				if len(latency) > 0 {
					emit(map[string]any{"mode": "read-latency", "count": len(latency), "p50": latency[len(latency)/2], "p99": latency[min(len(latency)-1, len(latency)*99/100)]})
				}
				return
			}
		}
	}()
	stopSamples := make(chan struct{})
	samplesDone := make(chan struct{})
	stopSampling := sync.OnceFunc(func() { close(stopSamples); <-samplesDone })
	defer stopSampling()
	started, used := time.Now(), cpu()
	go func() {
		defer close(samplesDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last, lastCPU, lastDone := started, used, completed.Load()
		previous := stagetime.Snapshot()
		sample := func() {
			at, used := time.Now(), cpu()
			done := completed.Load()
			current := stagetime.Snapshot()
			stages := map[string]float64{}
			for i := range current {
				stages[current[i].Stage.String()] = current[i].Seconds - previous[i].Seconds
			}
			stats := db.Stats()
			controller := map[string]any{}
			families, err := registry.Gather()
			check(err)
			for _, family := range families {
				for _, metric := range family.Metric {
					key := family.GetName()
					for _, label := range metric.Label {
						key += "." + label.GetValue()
					}
					value := metric.GetGauge().GetValue()
					if metric.Counter != nil {
						value = metric.GetCounter().GetValue()
					}
					if math.IsInf(value, 1) {
						controller[key] = "unlimited"
					} else {
						controller[key] = value
					}
				}
			}
			emit(map[string]any{"controller": controller, "mode": "sample", "second": at.Sub(started).Seconds(), "interval": at.Sub(last).Seconds(), "cpu": used - lastCPU, "updates": done - lastDone, "stages": stages, "block_hits": stats.CacheHits, "block_misses": stats.CacheMisses, "cache_bytes": stats.StateCache.Bytes})
			last, lastCPU, lastDone, previous = at, used, done, current
		}
		for {
			select {
			case <-ticker.C:
				sample()
			case <-stopSamples:
				sample()
				return
			}
		}
	}()
	switch *mode {
	case "cold":
		cycle("cold", base+60)
	case "prewarm":
		prewarm(ctx)
		cycle("prewarmed", base+60)
	case "metadata":
		prewarm(ctx)
		// Isolate deferred validation from point encoding, compaction and
		// tail reads. The write-order permutation is identical to cycle.
		at, used := time.Now(), cpu()
		reads := store.CountStoreReadsForTest()
		var wg sync.WaitGroup
		for worker := range *writers {
			wg.Go(func() {
				for j := worker; j < *metrics; j += *writers {
					_, err := db.Metadata(ctx, name(int(int64(j)*999983%int64(*metrics))))
					check(err)
				}
			})
		}
		wg.Wait()
		emit(map[string]any{"mode": "metadata-first-use", "metrics": *metrics, "wall": time.Since(at).Seconds(), "cpu": cpu() - used, "logical_reads": reads()})
	case "steady":
		cycle("fill", base+60)
		cycle("steady", base+120)
	case "backlog":
		var warm func()
		if (!*floor || *warmTimeout > 0) && !*noPrewarm {
			warm = func() {
				warmCtx := ctx
				if *warmTimeout > 0 {
					var cancel context.CancelFunc
					warmCtx, cancel = context.WithTimeout(ctx, *warmTimeout)
					defer cancel()
				}
				prewarm(warmCtx)
			}
		}
		pendingDir := ""
		if *pendingRestore {
			pendingDir = *dir
		}
		runBacklog(db, *metrics, *writers, *window, *gate, *adaptiveGate, *legacyCatchup,
			backlogConfig{minPoints: *backlogMinPoints, updates: *updates, spread: *backlogSpread, startupSpread: *backlogStartupSpread, stopAfter: *stopAfter, pendingDir: pendingDir, savedPoints: *savedPoints, gatePoints: *gatePoints}, progress, budget, warm)
	case "background":
		done := make(chan struct{})
		go func() { defer close(done); prewarm(ctx) }()
		if *gate > 0 {
			timer := time.NewTimer(*gate)
			select {
			case <-done:
			case <-timer.C:
			}
			timer.Stop()
		}
		cycle("background", base+60)
		<-done
	default:
		panic("unknown mode")
	}
	if delay := *observe - time.Since(started); delay > 0 {
		time.Sleep(delay)
	}
	stopReaders()
	stopSampling()
	emit(map[string]any{"mode": *mode + "-combined", "wall": time.Since(started).Seconds(), "cpu": cpu() - used})
}
