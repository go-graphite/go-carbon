package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/points"
)

type sealSampleMetric struct {
	Name   string
	Points []sealrollup.Sample
}

func loadSealDistribution(t testing.TB) []sealSampleMetric {
	t.Helper()
	path := os.Getenv("SEAL_SAMPLE_PATH")
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metrics []sealSampleMetric
	if err = json.Unmarshal(data, &metrics); err != nil {
		t.Fatal(err)
	}
	if len(metrics) == 0 {
		t.Fatal("empty captured distribution")
	}
	for _, m := range metrics {
		if m.Name == "" || len(m.Points) < 2 {
			t.Fatal("invalid sampled metric")
		}
		for i := 1; i < len(m.Points); i++ {
			if m.Points[i].Time <= m.Points[i-1].Time {
				t.Fatal("sample timestamps not increasing")
			}
		}
	}
	t.Logf("sampled_distribution metrics=%d source=%s", len(metrics), path)
	return metrics
}

func fillSealRows(dst []row, id uint64, now int64, iteration int, metrics []sealSampleMetric) {
	if len(metrics) == 0 {
		for j := range dst {
			dst[j] = row{ID: id, Time: now - int64(len(dst)) + int64(j), Bits: math.Float64bits(float64((iteration + j) % 97)), Seq: uint64(j + 1)}
		}
		return
	}
	ps := metrics[id%uint64(len(metrics))].Points
	step := ps[len(ps)-1].Time - ps[len(ps)-2].Time
	span := ps[len(ps)-1].Time - ps[0].Time + step
	last := (int64((len(dst)-1)/len(ps)) * span) + ps[(len(dst)-1)%len(ps)].Time - ps[0].Time
	for j := range dst {
		p := ps[j%len(ps)]
		offset := int64(j/len(ps))*span + p.Time - ps[0].Time
		dst[j] = row{ID: id, Time: now - step - last + offset, Bits: p.Bits, Seq: uint64(j + 1)}
	}
}

// Run alone in a native process. The baseline precedes opening the actual
// engine: Pebble, routing, held arenas, metadata and history all count. RSS from
// /usr/bin/time is a separate process high-water mark, never called live heap.
func TestCompactWholeEngineMemory(t *testing.T) {
	s := os.Getenv("SEAL_ENGINE_MEMORY_POINTS")
	if s == "" {
		t.Skip("native whole-engine memory receipt")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		t.Fatal("invalid memory points")
	}
	density := rollupMemoryDensity(t)
	distribution := loadSealDistribution(t)
	if len(distribution) < 1024 {
		t.Fatal("whole-engine qualification requires >=1024 rendered metrics")
	}
	cores := runtime.GOMAXPROCS(0)
	shards, owners, producers := sealBenchmarkGeometry(t, cores)
	offset := sealBenchmarkIDOffset(t)
	metrics := (n + density - 1) / density
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	// Count the numeric fixture's identity catalogue as well as the classic
	// cache's names. This avoids granting compact admission free identities.
	names := make([]string, metrics)
	catalogue := make(map[string]uint64, metrics)
	for i := range names {
		id := offset + uint64(i)
		names[i] = fmt.Sprintf("%s.fixture.%d", distribution[id%uint64(len(distribution))].Name, id)
		catalogue[names[i]] = id
	}
	measure := func(stage string) {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		delta := int64(m.HeapAlloc) - int64(baseline.HeapAlloc)
		fmt.Printf("ENGINE_MEMORY stage=%s points=%d density=%d cores=%d heap_delta=%d whole_heap=%d heap_inuse=%d system_reserved=%d delta_B_pt=%.6f whole_heap_B_pt=%.6f\n", stage, n, density, cores, delta, m.HeapAlloc, m.HeapInuse, m.Sys, float64(delta)/float64(n), float64(m.HeapAlloc)/float64(n))
	}
	if os.Getenv("SEAL_MEMORY_ENGINE") == "classic" {
		c := cache.New()
		c.SetMaxSize(uint64(n + 100000))
		scratch := make([]row, density)
		batch := make([]points.Point, density)
		for added := 0; added < n; {
			count := min(density, n-added)
			id := added / density
			fillSealRows(scratch[:count], offset+uint64(id), 1794700800, 0, distribution)
			for i, p := range scratch[:count] {
				batch[i] = points.Point{Timestamp: p.Time, Value: math.Float64frombits(p.Bits)}
			}
			c.AddPoints([]byte(names[id]), batch[:count])
			added += count
		}
		measure("classic-admission")
		runtime.KeepAlive(c)
		runtime.KeepAlive(catalogue)
		runtime.KeepAlive(names)
		return
	}
	cfg := coreConfig{Shards: shards, Owners: owners, Producers: producers, MaxRows: max(density, 2*n/shards), PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "compact-whole-memory"}
	e, _, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	heads := make([]compactRollupHead, producers)
	scratch := make([]row, density)
	policy, _ := json.Marshal(atomicBenchInput(1, 1, 4)[0].Config)
	for added := 0; added < n; {
		count := min(density, n-added)
		id := offset + uint64(added/density)
		fillSealRows(scratch[:count], id, 1794700800, 0, distribution)
		p := coreShard(id, shards) % producers
		if err = heads[p].append(coreBatch{ID: id, Order: 1, Clock: 1794700800, Policy: policy, Points: scratch[:count]}); err != nil {
			t.Fatal(err)
		}
		added += count
	}
	measure("admission-frozen")
	var workers sync.WaitGroup
	errs := make(chan error, producers)
	for p := range heads {
		workers.Go(func() { errs <- e.submitCompactRollupGroup(p, &heads[p], 1) })
	}
	workers.Wait()
	for range heads {
		if err = <-errs; err != nil {
			t.Fatal(err)
		}
	}
	measure("postseal-with-frozen-arenas")
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	measure("closed-with-history")
	runtime.KeepAlive(e)
	runtime.KeepAlive(heads)
	runtime.KeepAlive(catalogue)
	runtime.KeepAlive(names)
}
