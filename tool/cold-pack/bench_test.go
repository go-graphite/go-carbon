package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func benchCPU() float64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
}
func durableBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	var size int64
	for _, e := range entries {
		if !e.IsDir() && !strings.HasSuffix(e.Name(), ".log") && e.Name() != "LOCK" {
			i, err := e.Info()
			if err != nil {
				panic(err)
			}
			size += i.Size()
		}
	}
	return size
}
func quantiles(samples []float64) map[string]any {
	sort.Float64s(samples)
	return map[string]any{"samples": len(samples), "p50_ms": samples[len(samples)/2], "p99_ms": samples[min(len(samples)-1, len(samples)*99/100)]}
}

// TestColdPackCorpus is opt-in and consumes private clones of settled v7 stores.
// It measures the real maintenance path, not a stand-alone codec. Every run has a
// four-minute context and uses a fixed low-rate, twenty-series native read mix.
func TestColdPackCorpus(t *testing.T) {
	dir := os.Getenv("COLD_PACK_BENCH_DIR")
	if dir == "" {
		t.Skip("private corpus benchmark")
	}
	resultPath := os.Getenv("COLD_PACK_BENCH_RESULT")
	mode := os.Getenv("COLD_PACK_BENCH_MODE")
	if mode == "" {
		mode = "exact"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	marker, err := os.ReadFile(filepath.Join(dir, "INCREMENTAL-FILE-MIGRATION"))
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Split(string(marker), "\n")[1]
	var clock int64
	if _, err := fmt.Sscan(os.Getenv("COLD_PACK_BENCH_CLOCK"), &clock); err != nil {
		t.Fatal(err)
	}
	opts := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: source, CacheSize: 64 << 20, MemTableSize: 8 << 20, MaxUnflushedMemTableBytes: 32 << 20, SyncInterval: time.Second, Now: func() time.Time { return time.Unix(clock, 0) }}
	open := func(path string) *store.Store {
		s, e := store.Open(path, opts)
		if e != nil {
			t.Fatal(e)
		}
		s.StopBackground()
		return s
	}
	db := open(dir)
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	metrics, err := db.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// Immutable reference and isolated-CPU trial are independent checkpoints.
	refDir, isoDir := filepath.Join(filepath.Dir(dir), "reference"), filepath.Join(filepath.Dir(dir), "isolated")
	if err := db.Checkpoint(refDir); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(isoDir); err != nil {
		t.Fatal(err)
	}
	before := durableBytes(dir)
	query := func(reader *store.Store, n int) (float64, error) {
		n %= 500 // Repeat exactly the same request mix in every measured phase.
		start := time.Now()
		for j := 0; j < 20; j++ {
			m := metrics[(n*7919+j*104729)%len(metrics)]
			if _, err := reader.Fetch(ctx, m.Name, int(clock)-3600, int(clock)); err != nil {
				return 0, err
			}
		}
		return float64(time.Since(start)) / float64(time.Millisecond), nil
	}
	measure := func(reader *store.Store, count int) map[string]any {
		samples := make([]float64, count)
		for n := range samples {
			v, e := query(reader, n)
			if e != nil {
				t.Fatal(e)
			}
			samples[n] = v
			timer := time.NewTimer(5 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			case <-timer.C:
			}
		}
		return quantiles(samples)
	}
	result := map[string]any{"mode": mode, "series": len(metrics), "before_durable_bytes": before, "idle": measure(db, 500)}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: time.Second, BytesPerSecond: 1 << 20, CPUFraction: .02, MinFreeBytes: 30 << 30}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- db.RunColdPack(ctx) }()
	var samples []float64
	for n := 0; ; n++ {
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
			goto finished
		default:
		}
		v, e := query(db, n)
		if e != nil {
			t.Fatal(e)
		}
		samples = append(samples, v)
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
finished:
	result["maintenance_wall_s"] = time.Since(started).Seconds()
	result["during"] = quantiles(samples)
	result["paced_stats"] = db.ColdPackStats()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	result["after"] = measure(db, 500)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	result["after_durable_bytes"] = durableBytes(dir)
	// Open a fresh, deliberately tiny block cache for cache-miss reads. This does
	// not evict the operating system page cache and is labelled accordingly.
	opts.CacheSize = 64 << 10
	reader := open(dir)
	result["block_cache_cold"] = measure(reader, 500)
	reader.Close()
	opts.CacheSize = 64 << 20
	// CPU trial has no foreground reader and does not include settling compaction.
	isolated := open(isoDir)
	if err := isolated.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: time.Second, BytesPerSecond: 1 << 40, CPUFraction: 1}); err != nil {
		t.Fatal(err)
	}
	cpuStart := benchCPU()
	start := time.Now()
	for n := 0; n < 1000000; n++ {
		done, e := isolated.ColdPackStep(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if done {
			break
		}
		if n == 999999 {
			t.Fatal("sweep failed to finish")
		}
	}
	seconds := benchCPU() - cpuStart
	result["isolated_cpu_s"] = seconds
	result["isolated_wall_s"] = time.Since(start).Seconds()
	result["cpu_seconds_per_durable_GB"] = seconds / (float64(before) / 1e9)
	result["isolated_stats"] = isolated.ColdPackStats()
	isolated.Close()
	// Full archive oracle, including native rollups, against the ordinary source.
	candidate, reference := open(dir), open(refDir)
	defer candidate.Close()
	defer reference.Close()
	var checked, changed uint64
	maxError := 0.
	for _, m := range metrics {
		a, e := reference.Snapshot(ctx, m.Name)
		if e != nil {
			t.Fatal(e)
		}
		b, e := candidate.Snapshot(ctx, m.Name)
		if e != nil {
			t.Fatal(e)
		}
		if len(a.Archives) != len(b.Archives) {
			t.Fatal("archive count")
		}
		for n, w := range a.Archives {
			g := b.Archives[n]
			if w.Retention != g.Retention || len(w.Points) != len(g.Points) {
				t.Fatal("archive shape")
			}
			for i, p := range w.Points {
				q := g.Points[i]
				checked++
				if p.Timestamp != q.Timestamp {
					t.Fatal("timestamp changed")
				}
				if math.Float64bits(p.Value) != math.Float64bits(q.Value) {
					changed++
					rel := math.Abs((q.Value - p.Value) / p.Value)
					if mode == "exact" || math.IsNaN(rel) || rel > 5.1e-12 {
						t.Fatal("numeric oracle")
					}
					maxError = max(maxError, rel)
				}
			}
		}
	}
	result["oracle"] = map[string]any{"points": checked, "changed": changed, "max_relative_error": maxError}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
