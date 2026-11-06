package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Opt-in physical Linux comparison. Synthetic random mantissas prevent a tiny
// compressed source from fitting the old cache. This diagnoses cache scaling;
// it is deliberately not a production workload or a NIC-speed qualification.
func TestNativeExportCacheScaling(t *testing.T) {
	output := os.Getenv("GRAPHITE_NATIVE_CACHE_EVAL")
	if output == "" {
		t.Skip("set GRAPHITE_NATIVE_CACHE_EVAL to a private evidence directory")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("physical cache comparison requires Linux")
	}
	ctx := context.Background()
	root := t.TempDir()
	clock := func() time.Time { return time.Unix(1_793_960_000, 0) }
	options := func(size int64, shards int) store.Options {
		return store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: clock,
			CacheSize: size, CacheShards: shards, MemTableSize: 16 << 20, SyncInterval: time.Second}
	}
	directory := filepath.Join(root, "donor")
	source, err := store.Open(directory, options(32<<20, 0))
	if err != nil {
		t.Fatal(err)
	}
	const count, pointsPerName = 4096, 2048
	names := make([]string, count)
	var random uint64 = 0x9e3779b97f4a7c15
	for i := range names {
		name := fmt.Sprintf("cache.scaling.metric%06d", i)
		names[i] = name
		if _, err := source.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 4096}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
		values := make([]store.Point, pointsPerName)
		for j := range values {
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			values[j] = store.Point{Timestamp: clock().Unix() - pointsPerName + int64(j), Value: math.Float64frombits(0x3ff0000000000000 | random&0xfffffffffffff)}
		}
		if err := source.UpdateMany(ctx, name, values); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	for _, config := range []struct {
		name   string
		bytes  int64
		shards int
	}{{"before-32MiB-auto", 32 << 20, 0}, {"after-1GiB-128", 1 << 30, 128}} {
		t.Run(config.name, func(t *testing.T) {
			source, err := store.Open(directory, options(config.bytes, config.shards))
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			for pass := range 3 {
				target, err := store.Open(filepath.Join(root, fmt.Sprintf("%s-%d", config.name, pass)), options(32<<20, 0))
				if err != nil {
					t.Fatal(err)
				}
				prefix := filepath.Join(output, fmt.Sprintf("%s-%d", config.name, pass))
				cpu, err := os.Create(prefix + ".cpu")
				if err != nil {
					t.Fatal(err)
				}
				if err := pprof.StartCPUProfile(cpu); err != nil {
					t.Fatal(err)
				}
				runtime.SetMutexProfileFraction(1)
				runtime.SetBlockProfileRate(1)
				countersBefore := nativeEvalCounters()
				result, deferred, err := measureNative(ctx, source, target, names, 128, 32)
				countersAfter := nativeEvalCounters()
				pprof.StopCPUProfile()
				cpu.Close()
				runtime.SetMutexProfileFraction(0)
				runtime.SetBlockProfileRate(0)
				if err != nil || len(deferred) != 0 || result.Metrics != count {
					t.Fatal("copy failed", result, len(deferred), err)
				}
				for _, kind := range []string{"mutex", "block"} {
					file, err := os.Create(prefix + "." + kind)
					if err != nil {
						t.Fatal(err)
					}
					if err := pprof.Lookup(kind).WriteTo(file, 0); err != nil {
						t.Fatal(err)
					}
					file.Close()
				}
				row := map[string]any{"configuration": config.name, "pass": pass, "result": result, "native_stats": target.NativeStats(), "source_stats": source.Stats(), "synthetic_names": count, "points_per_name": pointsPerName, "cpus": runtime.GOMAXPROCS(0), "shared_disk": true, "counters_before": countersBefore, "counters_after": countersAfter}
				data, _ := json.Marshal(row)
				if err := os.WriteFile(prefix+".json", append(data, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				t.Logf("%s pass%d %.3f MB/s, %d bytes, %.6fs", config.name, pass, result.MBPerSecond, result.WireBytes, result.Seconds)
				if pass == 2 {
					for _, name := range names {
						want, err := source.Snapshot(ctx, name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := target.Snapshot(ctx, name)
						if err != nil {
							t.Fatal(err)
						}
						if err := store.EqualSnapshots(want, got); err != nil {
							t.Fatal(name, err)
						}
						if len(got.Archives[0].Points) != pointsPerName {
							t.Fatal("lost points", name, len(got.Archives[0].Points))
						}
					}
				}
				if err := target.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(filepath.Join(root, fmt.Sprintf("%s-%d", config.name, pass))); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func nativeEvalCounters() map[string]string {
	result := make(map[string]string)
	for _, path := range []string{"/proc/self/stat", "/proc/stat", "/proc/diskstats", "/proc/net/dev"} {
		data, err := os.ReadFile(path)
		if err == nil {
			result[path] = string(data)
		}
	}
	return result
}
