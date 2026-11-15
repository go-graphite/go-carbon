package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

func TestSnapshotCoreFrozenHeadAndSealedPart(t *testing.T) {
	for _, appendParts := range []bool{false, true} {
		t.Run(strconv.FormatBool(appendParts), func(t *testing.T) { testSnapshotCoreFrozenHeadAndSealedPart(t, appendParts) })
	}
}
func testSnapshotCoreFrozenHeadAndSealedPart(t *testing.T, appendParts bool) {
	dir := t.TempDir()
	cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 64, PartitionSeconds: 86400, AppendParts: appendParts}
	var frozen, sealed bool
	e, err := openCoreWithHook(filepath.Join(dir, "db"), cfg, func(shard int, snap *pebble.Snapshot, head []row) error {
		defer snap.Close()
		if len(head) > 0 {
			frozen = true
			if head[0].Bits != math.Float64bits(42) {
				t.Error("head bits changed")
			}
		}
		src := snapshotCoreSource{snap: snap, partitions: []int64{corePartition(1800000000, 86400)}, appendParts: appendParts}
		if b, c, err := snap.Get(coreMetaKey); err == nil {
			var meta coreFrontier
			err = json.Unmarshal(b, &meta)
			_ = c.Close()
			if err != nil {
				t.Fatal(err)
			}
			src.nextPart = meta.NextPart
		}
		return src.Scan(context.Background(), 7, pr.Archive{Step: 1, Points: 86400}, 1799999999, 1800000001, func(p pr.Sample) error {
			if p.Bits != math.Float64bits(42) {
				t.Error("sealed bits changed")
			}
			sealed = true
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, []row{{7, 1800000000, math.Float64bits(42), 1}}, 1, false); err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	if !frozen || !sealed {
		t.Fatal("missing frozen or sealed visibility", frozen, sealed)
	}
}

// This opt-in test binary uses the same core ingest workload with and without
// render readers. Its explicit environment prevents accidental load in CI.
func TestSnapshotReadBenchmark(t *testing.T) {
	out := os.Getenv("PARTS_READ_BENCH_DIR")
	if out == "" {
		t.Skip("set PARTS_READ_BENCH_DIR to an empty private directory")
	}
	seconds := 20
	readers := 8
	if v := os.Getenv("PARTS_READ_BENCH_SECONDS"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 4 {
			t.Fatal("invalid seconds")
		}
		seconds = n
	}
	if v := os.Getenv("PARTS_READ_BENCH_READERS"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 0 {
			t.Fatal("invalid readers")
		}
		readers = n
	}
	cfg := coreConfig{Shards: 16, Producers: 4, MaxRows: 2097152, PartitionSeconds: 86400, AppendParts: true}
	for key, target := range map[string]*int{"PARTS_READ_BENCH_SHARDS": &cfg.Shards, "PARTS_READ_BENCH_PRODUCERS": &cfg.Producers} {
		if v := os.Getenv(key); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 1 || n > 192 {
				t.Fatal("invalid", key)
			}
			*target = n
		}
	}

	snapshotBenchmarkWithHook = snapshotProfiledBenchmarkWithHook
	snapshotReaderAffinity = pinSnapshotReader
	rate := uint64(30_000_000)
	if v := os.Getenv("PARTS_READ_BENCH_RATE"); v != "" {
		var e error
		rate, e = strconv.ParseUint(v, 10, 64)
		if e != nil {
			t.Fatal(e)
		}
	}
	if err := runSnapshotReadBenchmark(out, cfg, time.Duration(seconds)*time.Second, rate, 1<<24, 3*time.Second, readers); err != nil {
		t.Fatal(err)
	}
}

func snapshotProfiledBenchmarkWithHook(out string, cfg coreConfig, duration time.Duration, rate uint64, metrics uint64, settle time.Duration, hook func(int, *pebble.Snapshot, []row) error) error {
	if duration <= 0 || rate == 0 || metrics == 0 {
		return os.ErrInvalid
	}
	engine, err := openCoreWithHook(out, cfg, hook)
	if err != nil {
		return err
	}
	start := time.Now()
	baseline := procStats()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "core_start", "when_cet": start.In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "config": cfg, "rate": rate, "duration_seconds": duration.Seconds(), "metrics": metrics, "gomaxprocs": runtime.GOMAXPROCS(0), "proc": baseline, "workload": "synthetic_preidentified_rows"})
	done := make(chan error, cfg.Producers)
	var offered atomic.Uint64
	cursors := make([]atomic.Uint64, cfg.Producers)
	target := uint64(duration.Seconds() * float64(rate))
	for p := 0; p < cfg.Producers; p++ {
		go func(p int) {
			const batchSize = 32768
			var sent uint64
			for {
				global := sent*uint64(cfg.Producers) + uint64(p)
				if global >= target || time.Since(start) >= duration {
					done <- nil
					return
				}
				n := min(uint64(batchSize), (target-global+uint64(cfg.Producers)-1)/uint64(cfg.Producers))
				due := start.Add(time.Duration(float64(global) / float64(rate) * float64(time.Second)))
				if delay := time.Until(due); delay > 0 {
					time.Sleep(delay)
				}
				rows := make([]row, n)
				for i := range rows {
					seq := (sent+uint64(i))*uint64(cfg.Producers) + uint64(p)
					rows[i] = row{seq % metrics, 1800000000 + int64(seq/rate), math.Float64bits(float64(seq % 100003)), seq}
				}
				sent += n
				offered.Add(n)
				if e := engine.submit(p, rows, sent, false); e != nil {
					done <- e
					return
				}
				cursors[p].Store(sent)
			}
		}(p)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	left := cfg.Producers
	for left > 0 {
		select {
		case e := <-done:
			left--
			err = errors.Join(err, e)
		case <-tick.C:
			_ = json.NewEncoder(os.Stdout).Encode(snapshotBenchmarkSample(engine, "core_sample", start, offered.Load()))
		}
	}
	// Producers have stopped, so the caller may issue final barriers on their queues.
	for p := 0; p < cfg.Producers; p++ {
		cursor := cursors[p].Load()
		if cursor == 0 {
			continue
		}
		err = errors.Join(err, engine.submit(p, nil, cursor, true))
	}
	_ = json.NewEncoder(os.Stdout).Encode(snapshotBenchmarkSample(engine, "core_ingest_complete", start, offered.Load()))
	until := time.Now().Add(settle)
	for time.Now().Before(until) {
		time.Sleep(time.Second)
		_ = json.NewEncoder(os.Stdout).Encode(snapshotBenchmarkSample(engine, "core_settle", start, offered.Load()))
	}
	final := snapshotBenchmarkSample(engine, "core_complete", start, offered.Load())
	final["baseline_proc"] = baseline
	final["scheduled_points"] = target
	final["target_met"] = offered.Load() >= target
	if final["committed"].(uint64) != offered.Load() {
		err = errors.Join(err, errors.New("final seal did not commit all submitted points"))
	}
	if hook != nil {
		err = errors.Join(err, hook(-1, nil, nil))
	}
	err = errors.Join(err, engine.close())
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(final)
}

func snapshotBenchmarkSample(engine *coreEngine, stage string, start time.Time, offered uint64) map[string]any {
	out := coreSample(engine, stage, start, offered)
	var snapshots int
	var pinned, zombie, cacheSize, cacheHits, cacheMisses uint64
	for _, o := range engine.owners {
		m := o.db.Metrics()
		snapshots += m.Snapshots.Count
		pinned += m.Snapshots.PinnedSize
		zombie += m.MemTable.ZombieSize
		cacheSize += uint64(m.BlockCache.Size)
		cacheHits += uint64(m.BlockCache.Hits)
		cacheMisses += uint64(m.BlockCache.Misses)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	out["pebble_snapshots"] = snapshots
	out["pebble_pinned_bytes"] = pinned
	out["pebble_zombie_memtable_bytes"] = zombie
	out["pebble_cache_bytes"] = cacheSize
	out["pebble_cache_hits"] = cacheHits
	out["pebble_cache_misses"] = cacheMisses
	out["heap_alloc"] = mem.HeapAlloc
	out["total_alloc"] = mem.TotalAlloc
	out["gc_cpu_fraction"] = mem.GCCPUFraction
	out["num_gc"] = mem.NumGC
	return out
}
