package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type sweepFS struct {
	vfs.FS
	nanos atomic.Int64
	calls atomic.Int64
}

func (f *sweepFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	start := time.Now()
	u, err := f.FS.GetDiskUsage(path)
	f.nanos.Add(int64(time.Since(start)))
	f.calls.Add(1)
	return u, err
}

type sweepCodec struct {
	whisperio.ContinuationCodec
	nanos time.Duration
	calls int
}

func (c *sweepCodec) Repack(ctx context.Context, kv store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	start := time.Now()
	stats, next, done, err := c.ContinuationCodec.Repack(ctx, kv, r)
	c.nanos += time.Since(start)
	c.calls++
	return stats, next, done, err
}

// TestColdPackSyntheticSweep profiles the complete paced driver on fresh clones
// of a settled v7 store. Fifteen of every sixteen metrics have only 64 points;
// the others have 1800. All history is outside the write window. Set
// COLD_PACK_SWEEP_METRICS to opt in, and optionally COLD_PACK_SWEEP_RESULT (JSON)
// and COLD_PACK_SWEEP_PROFILE (prefix for per-trial CPU profiles and traces).
// COLD_PACK_SWEEP_TRIAL selects raised, default or unlimited; unset runs all.
// Fixture creation, settling and the full bit-exact snapshot oracle are untimed.
func TestColdPackSyntheticSweep(t *testing.T) {
	value := os.Getenv("COLD_PACK_SWEEP_METRICS")
	if value == "" {
		t.Skip("private synthetic sweep benchmark")
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 16 {
		t.Fatal("COLD_PACK_SWEEP_METRICS must be >= 16")
	}
	selected := os.Getenv("COLD_PACK_SWEEP_TRIAL")
	if selected != "" && selected != "raised" && selected != "default" && selected != "unlimited" {
		t.Fatal("COLD_PACK_SWEEP_TRIAL must be raised, default or unlimited")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 140*time.Second)
	defer cancel()
	const base = int64(1700000040)
	root := t.TempDir()
	opts := store.Options{ExactBlocks: true, TimeBuckets: true, CacheSize: 32 << 20, MemTableSize: 8 << 20, MetadataCacheSize: 1 << 20, SyncInterval: time.Second, Now: func() time.Time { return time.Unix(base+3*86400, 0) }}
	open := func(dir string, codec *sweepCodec, fs *sweepFS) *store.Store {
		t.Helper()
		opts.Continuation = codec
		db, e := store.Open(dir, opts.WithFS(fs))
		if e != nil {
			t.Fatal(e)
		}
		db.StopBackground()
		return db
	}
	name := func(i int) string { return fmt.Sprintf("synthetic.service.instance.%08d", i) }
	fixture := open(filepath.Join(root, "fixture"), &sweepCodec{}, &sweepFS{FS: vfs.Default})
	t.Cleanup(func() { fixture.Close() })
	points := make([]store.Point, 1800)
	for i := range points {
		points[i] = store.Point{Timestamp: base + int64(i*60), Value: 1000 + math.Sin(float64(i))*.0123456789012345}
	}
	for i := 0; i < count; i++ {
		cfg := store.MetricConfig{Name: name(i), Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
		if _, err := fixture.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		n := 64
		if i%16 == 0 {
			n = len(points)
		}
		if err := fixture.UpdateMany(ctx, cfg.Name, points[:n]); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.TransferFormat() != 7 {
		t.Fatal("fixture must start in v7 format")
	}
	if err := fixture.Compact(); err != nil {
		t.Fatal(err)
	}
	t.Logf("settled v7 fixture: %d metrics", count)
	var results []map[string]any
	for _, trial := range []struct {
		name string
		cpu  float64
		rate int64
	}{{"raised", .25, 8 << 20}, {"default", .02, 1 << 20}, {"unlimited", 1, 1 << 40}} {
		if selected != "" && selected != trial.name {
			continue
		}
		dir := filepath.Join(root, trial.name)
		if err := fixture.Checkpoint(dir); err != nil {
			t.Fatal(err)
		}
		codec, fs := &sweepCodec{}, &sweepFS{FS: vfs.Default}
		db := open(dir, codec, fs)
		t.Cleanup(func() {
			if db != nil {
				db.Close()
			}
		})
		if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", BytesPerSecond: trial.rate, CPUFraction: trial.cpu, MinFreeBytes: 1, MaxCompactionDebt: 1 << 40}); err != nil {
			t.Fatal(err)
		}
		fs.nanos.Store(0)
		fs.calls.Store(0)
		profile := os.Getenv("COLD_PACK_SWEEP_PROFILE")
		if profile != "" {
			cpuFile, err := os.Create(profile + "." + trial.name + ".cpu")
			if err != nil {
				t.Fatal(err)
			}
			defer cpuFile.Close()
			traceFile, err := os.Create(profile + "." + trial.name + ".trace")
			if err != nil {
				t.Fatal(err)
			}
			defer traceFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				t.Fatal(err)
			}
			if err := trace.Start(traceFile); err != nil {
				t.Fatal(err)
			}
		}
		cpu, started := benchCPU(), time.Now()
		err := db.RunColdPack(ctx)
		wall, cpu := time.Since(started).Seconds(), benchCPU()-cpu
		if profile != "" {
			trace.Stop()
			pprof.StopCPUProfile()
		}
		if err != nil {
			t.Fatal(err)
		}
		stats := db.ColdPackStats()
		if stats.Passes != 1 || stats.Points == 0 || stats.Errors+stats.Conflicts+stats.Skipped+stats.PressurePauses != 0 {
			t.Fatalf("incomplete sweep: %+v", stats)
		}
		result := map[string]any{"trial": trial.name, "metrics": count, "cpu_fraction": trial.cpu, "bytes_per_second": trial.rate,
			"wall_s": wall, "process_cpu_s": cpu, "cpu_cores": cpu / wall, "points_per_second": float64(stats.Points) / wall,
			"worker_s": float64(stats.WorkerNanos) / 1e9, "repack_s": codec.nanos.Seconds(), "repack_calls": codec.calls,
			"disk_usage_s": float64(fs.nanos.Load()) / 1e9, "disk_usage_calls": fs.calls.Load(), "stats": stats}
		encoded, _ := json.Marshal(result)
		t.Log(string(encoded))
		results = append(results, result)
		for i := 0; i < count; i++ {
			want, err := fixture.Snapshot(ctx, name(i))
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.Snapshot(ctx, name(i))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatalf("%s: %v", name(i), err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
	}
	if path := os.Getenv("COLD_PACK_SWEEP_RESULT"); path != "" {
		data, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
