package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

// footprintStateKeys mirrors the whisperio codec's state keys: file names,
// manifests ('f' + id + 'h'), header pages ('f' + id + 'H' + page) and the
// file sequence.
type footprintStateKeys struct{}

func (footprintStateKeys) StateKey(suffix []byte) bool {
	switch {
	case bytes.HasPrefix(suffix, []byte("n/")), string(suffix) == "sequence":
		return true
	case len(suffix) == 10 && suffix[0] == 'f' && suffix[9] == 'h':
		return true
	case len(suffix) == 12 && suffix[0] == 'f' && suffix[9] == 'H':
		return true
	}
	return false
}

func (footprintStateKeys) StatePrefix(prefix []byte) bool {
	return string(prefix) == "n/" || len(prefix) == 10 && prefix[0] == 'f' && prefix[9] == 'H'
}

func footprintFileKey(id uint64, kind byte) []byte {
	k := make([]byte, 10)
	k[0] = 'f'
	binary.BigEndian.PutUint64(k[1:9], id)
	k[9] = kind
	return k
}

func footprintPageKey(id uint64, page uint16) []byte {
	return binary.BigEndian.AppendUint16(footprintFileKey(id, 'H'), page)
}

func footprintBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

// footprintState builds one metric's state shaped like the whisperio codec's
// (after key dumps of production stores): dense metrics have two archives,
// one file and 3-4 header pages; sparse metrics three archives, nine header
// pages, and half of them an out-of-order sidecar file. Every key and value is
// its own allocation, as states read from Pebble are.
func footprintState(shape string, rng *rand.Rand, i int) *metricState {
	name := fmt.Sprintf("%s.service-%03d.namespace.%07d.host-%04d.some_metric_name.%s", shape, i%997, i, i%7919, strings.Repeat("x", rng.Intn(24)))
	meta := Metadata{MetricConfig: MetricConfig{Name: name, AggregationMethod: Average, XFilesFactor: .5},
		ID: uint64(i) + 1, Generation: 1, Revision: uint64(rng.Intn(1000)), LastUpdate: time.Unix(0, 1_790_000_000_000_000_000+int64(i)), Continuation: true}
	type file struct {
		id       uint64
		name     string
		pages    int
		last     int
		manifest int
	}
	var files []file
	switch shape {
	case "dense":
		meta.Retentions = []Retention{{Step: 60, Count: 43200}, {Step: 600, Count: 52560}}
		files = []file{{id: 1, name: "main.wsp", pages: 3 + rng.Intn(2), last: 47, manifest: 29}}
	case "sparse":
		meta.Retentions = []Retention{{Step: 60, Count: 10080}, {Step: 300, Count: 8640}, {Step: 3600, Count: 8760}}
		files = []file{{id: 1, name: "main.wsp", pages: 9, last: 71, manifest: 37}}
		if rng.Intn(2) == 0 {
			files = append(files, file{id: 2, name: "main.wsp.ooo", pages: 1, last: 52, manifest: 34})
		}
	default:
		panic(shape)
	}
	st := newMetricState(meta)
	names := make(map[string][]byte)
	for _, f := range files {
		names["n/"+f.name] = binary.BigEndian.AppendUint64(nil, f.id)
		pages := make(map[string][]byte)
		for p := range f.pages {
			size := 128
			if p == f.pages-1 {
				size = f.last
			}
			pages[string(footprintPageKey(f.id, uint16(p)))] = footprintBytes(rng, size)
		}
		st.replacePrefix(footprintFileKey(f.id, 'H'), pages)
		st.set(footprintFileKey(f.id, 'h'), footprintBytes(rng, f.manifest))
	}
	st.replacePrefix([]byte("n/"), names)
	if len(files) > 1 {
		st.set([]byte("sequence"), binary.BigEndian.AppendUint64(nil, uint64(len(files))))
	}
	return st.seal()
}

func footprintStripe(name string) uint64 { return (helper.HashString(name) >> 32) % metricStripes }

type gcSample struct {
	cpu, wall  float64
	pauseNs    uint64
	cycles     uint32
	scanHeap   uint64
	heapAlloc  uint64
	heapObject uint64
}

func readGCSample() gcSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/scan/heap:bytes"}}
	metrics.Read(samples)
	return gcSample{cpu: samples[0].Value.Float64(), pauseNs: ms.PauseTotalNs, cycles: ms.NumGC, scanHeap: samples[1].Value.Uint64(),
		heapAlloc: ms.HeapAlloc, heapObject: ms.HeapObjects}
}

// processCPU returns the process's user and system CPU seconds.
func processCPU() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		panic(err)
	}
	return float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
}

// forcedGC runs n forced collections and reports per collection the GC CPU
// seconds the runtime accounts (P time, inflated when the host is
// oversubscribed), the process CPU seconds (the program is otherwise idle),
// and wall seconds.
func forcedGC(n int) (cpu, wall float64, pause uint64, rusage float64) {
	before := readGCSample()
	cpuBefore := processCPU()
	start := time.Now()
	for range n {
		runtime.GC()
	}
	wall = time.Since(start).Seconds() / float64(n)
	rusage = (processCPU() - cpuBefore) / float64(n)
	after := readGCSample()
	return (after.cpu - before.cpu) / float64(n), wall, (after.pauseNs - before.pauseNs) / uint64(n), rusage
}

// footprintFetch reads a state the way a continuation fetch does: the file
// names, then each file's manifest and header pages, through a state view.
func footprintFetch(st *metricState) int {
	m := st.meta
	m.MetricConfig = cloneConfig(m.MetricConfig)
	io := &continuationIO{ctx: context.Background(), state: &stateView{keys: footprintStateKeys{}, base: st, meta: m}}
	n := 0
	var ids []uint64
	if err := io.Visit([]byte("n/"), func(_, v []byte) error {
		ids = append(ids, binary.BigEndian.Uint64(v))
		return nil
	}); err != nil {
		panic(err)
	}
	for _, id := range ids {
		v, err := io.Get(footprintFileKey(id, 'h'))
		if err != nil {
			panic(err)
		}
		n += len(v)
		if err := io.Visit(footprintFileKey(id, 'H'), func(_, v []byte) error {
			n += len(v)
			return nil
		}); err != nil {
			panic(err)
		}
	}
	return n
}

// footprintUpdate runs an update's cache interactions: begin, the locked
// read, the reads above, a rewritten last header page, final and install.
func footprintUpdate(c *stateCache, stripe uint64, name string, rng *rand.Rand) {
	c.beginMutation(stripe, name)
	st := c.locked(stripe, name)
	if st == nil {
		c.endMutation(stripe, name)
		return
	}
	m := st.meta
	m.MetricConfig = cloneConfig(m.MetricConfig)
	v := &stateView{keys: footprintStateKeys{}, base: st, meta: m}
	footprintFetch(st)
	page := footprintPageKey(1, 2)
	v.mutable().set(page, footprintBytes(rng, 47+rng.Intn(80)))
	m.Revision++
	c.installMutation(stripe, name, v.final(m))
	c.endMutation(stripe, name)
}

// TestStateCacheFootprint fills a metadata cache with production-shaped
// states and reports its heap objects, bytes per entry, the GC CPU of a
// forced collection over it, and hit, fetch and update latencies. It runs
// only when GO_CARBON_STATE_CACHE_FOOTPRINT lists shape:count pairs, e.g.
// "dense:5000000,sparse:2000000".
func TestStateCacheFootprint(t *testing.T) {
	spec := os.Getenv("GO_CARBON_STATE_CACHE_FOOTPRINT")
	if spec == "" {
		t.Skip("set GO_CARBON_STATE_CACHE_FOOTPRINT=shape:count[,shape:count]")
	}
	for _, part := range strings.Split(spec, ",") {
		shape, count, _ := strings.Cut(part, ":")
		n, err := strconv.Atoi(count)
		if err != nil {
			t.Fatal(err)
		}
		footprintRun(t, shape, n)
	}
}

func footprintRun(t *testing.T, shape string, n int) {
	runtime.GC()
	runtime.GC()
	emptyCPU, emptyWall, _, emptyRusage := forcedGC(3)
	base := readGCSample()

	c := newStateCache(int64(n)*8192, footprintStateKeys{})
	names := make([]string, n)
	rng := rand.New(rand.NewSource(1))
	var build, fill time.Duration
	var sumSize int64
	for i := range n {
		start := time.Now()
		st := footprintState(shape, rng, i)
		mid := time.Now()
		name := st.meta.Name
		stripe := footprintStripe(name)
		c.fill(stripe, name, c.epoch(stripe), st)
		fill += time.Since(mid)
		build += mid.Sub(start)
		names[i] = name
		sumSize += st.size
	}
	runtime.GC()
	runtime.GC()
	full := readGCSample()
	gcCPU, gcWall, gcPause, gcRusage := forcedGC(5)
	stats := c.stats()
	reported := float64(stats.Bytes) / float64(n)
	if stats.Entries != int64(n) {
		t.Fatalf("%d entries cached, want %d", stats.Entries, n)
	}
	namesBytes := 0
	for _, name := range names {
		namesBytes += 16 + len(name)
	}
	heapPerEntry := (float64(full.heapAlloc) - float64(base.heapAlloc) - float64(namesBytes)) / float64(n)
	objPerEntry := (float64(full.heapObject) - float64(base.heapObject) - float64(n) - 1) / float64(n)
	scanPerEntry := (float64(full.scanHeap) - float64(base.scanHeap) - float64(namesBytes)) / float64(n)

	// Hits: get alone, and get plus a fetch's state reads.
	const ops = 1_000_000
	order := make([]int32, ops)
	for i := range order {
		order[i] = int32(rng.Intn(n))
	}
	measure := func(f func(i int)) (nsPerOp, allocsPerOp float64) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		for i := range ops {
			f(i)
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		return float64(elapsed.Nanoseconds()) / ops, float64(after.Mallocs-before.Mallocs) / ops
	}
	var sink atomic.Int64
	hitNs, hitAllocs := measure(func(i int) {
		name := names[order[i]]
		stripe := footprintStripe(name)
		if st := c.get(stripe, name, c.epoch(stripe)); st == nil {
			panic("miss")
		} else {
			sink.Add(int64(st.meta.ID))
		}
	})
	fetchNs, fetchAllocs := measure(func(i int) {
		name := names[order[i]]
		stripe := footprintStripe(name)
		st := c.get(stripe, name, c.epoch(stripe))
		sink.Add(int64(footprintFetch(st)))
	})
	// Parallel hits with a fetch's reads, one goroutine per P.
	procs := runtime.GOMAXPROCS(0)
	start := time.Now()
	var wg sync.WaitGroup
	for p := range procs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := p; i < ops; i += procs {
				name := names[order[i]]
				stripe := footprintStripe(name)
				st := c.get(stripe, name, c.epoch(stripe))
				sink.Add(int64(footprintFetch(st)))
			}
		}()
	}
	wg.Wait()
	parallelFetchNs := float64(time.Since(start).Nanoseconds()) / ops

	updRng := rand.New(rand.NewSource(2))
	updateNs, updateAllocs := measure(func(i int) {
		name := names[order[i]]
		footprintUpdate(c, footprintStripe(name), name, updRng)
	})
	// Churn: every entry rewritten once more, then the full-cache GC again.
	churnStart := time.Now()
	for i := range n {
		footprintUpdate(c, footprintStripe(names[i]), names[i], updRng)
	}
	churnNs := float64(time.Since(churnStart).Nanoseconds()) / float64(n)
	runtime.GC()
	runtime.GC()
	churned := readGCSample()
	churnCPU, churnWall, _, churnRusage := forcedGC(5)
	stats = c.stats()
	fmt.Printf("FOOTPRINT shape=%s n=%d build_ns=%.0f fill_ns=%.0f reported_bytes_per_entry=%.0f size_per_entry=%.0f "+
		"heap_bytes_per_entry=%.0f heap_objects_per_entry=%.2f scan_bytes_per_entry=%.0f heap_alloc_gb=%.2f "+
		"gc_cpu_s_empty=%.4f gc_wall_s_empty=%.4f gc_cpu_s_full=%.4f gc_wall_s_full=%.4f gc_pause_us_full=%.0f "+
		"hit_ns=%.0f hit_allocs=%.2f fetch_ns=%.0f fetch_allocs=%.2f parallel_fetch_ns=%.0f update_ns=%.0f update_allocs=%.2f "+
		"churn_update_ns=%.0f churn_heap_bytes_per_entry=%.0f churn_heap_objects_per_entry=%.2f gc_cpu_s_churned=%.4f gc_wall_s_churned=%.4f "+
		"reported_bytes_per_entry_churned=%.0f gc_rusage_s_empty=%.4f gc_rusage_s_full=%.4f gc_rusage_s_churned=%.4f%s sink=%d\n",
		shape, n, float64(build.Nanoseconds())/float64(n), float64(fill.Nanoseconds())/float64(n),
		reported, float64(sumSize)/float64(n),
		heapPerEntry, objPerEntry, scanPerEntry, float64(full.heapAlloc)/(1<<30),
		emptyCPU, emptyWall, gcCPU, gcWall, float64(gcPause)/1e3,
		hitNs, hitAllocs, fetchNs, fetchAllocs, parallelFetchNs, updateNs, updateAllocs,
		churnNs, (float64(churned.heapAlloc)-float64(base.heapAlloc)-float64(namesBytes))/float64(n),
		(float64(churned.heapObject)-float64(base.heapObject)-float64(n)-1)/float64(n), churnCPU, churnWall,
		float64(stats.Bytes)/float64(n), emptyRusage, gcRusage, churnRusage, footprintExtra(c), sink.Load())
	runtime.KeepAlive(c)
	runtime.KeepAlive(names)
}

// footprintExtra reports the arena's memory and compaction per entry.
func footprintExtra(c *stateCache) string {
	s := c.stats()
	return fmt.Sprintf(" arena_bytes_per_entry=%.0f compacted_bytes_per_entry=%.0f", float64(s.ArenaBytes)/float64(s.Entries), float64(s.CompactedBytes)/float64(s.Entries))
}
