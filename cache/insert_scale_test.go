//go:build linux || darwin

package cache

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/zeebo/xxh3"
)

// TestCacheInsertScale is opt-in: private captured names are tiled to a cold
// production-sized resident set. Only aggregate statistics leave this process.
func TestCacheInsertScale(t *testing.T) {
	path := os.Getenv("CACHE_INSERT_NAMES")
	if path == "" {
		t.Skip("set CACHE_INSERT_NAMES to a private name fixture")
	}
	env := func(key string, fallback int) int {
		if s := os.Getenv(key); s != "" {
			n, e := strconv.Atoi(s)
			if e != nil {
				t.Fatal(e)
			}
			return n
		}
		return fallback
	}
	n, connections, trials, updates := env("CACHE_INSERT_METRICS", 20000000), env("CACHE_INSERT_CONNECTIONS", 16), env("CACHE_INSERT_TRIALS", 8), env("CACHE_INSERT_UPDATES", 8000000)
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	scanner := bufio.NewScanner(f)
	var names []string
	for scanner.Scan() {
		names = append(names, scanner.Text())
	}
	e = scanner.Err()
	f.Close()
	if e != nil || len(names) == 0 {
		t.Fatal("empty or invalid fixture", e)
	}
	nameFor := func(dst []byte, i int) []byte {
		dst = append(dst, names[i%len(names)]...)
		dst = append(dst, '.')
		for shift := 28; shift >= 0; shift -= 4 {
			dst = append(dst, "0123456789abcdef"[(i>>shift)&15])
		}
		return dst
	}
	c := New()
	c.SetMaxSize(0)
	c.SetBloomSize(uint64(n))
	c.SetNewMetricsChan(make(chan string, 1))
	if e = c.SetWriteoutBatching(960, 960*time.Second); e != nil {
		t.Fatal(e)
	}
	// Recreate the resident point-buffer distribution without billions of timed
	// setup calls. Every item owns its buffer; partial words stay inline as usual.
	var seed compactPoints
	for i := range 237 {
		seed.append(float64(i%13), 1790000000+int64(i)*4)
	}
	var buf []byte
	for i := range n {
		buf = nameFor(buf[:0], i)
		name := string(buf)
		it := newItem(name, 0)
		it.compact = seed
		it.compact.buf = bytes.Clone(seed.buf)
		it.firstArrival = time.Now()
		it.indexGeneration = c.filterGeneration.Load()
		h := xxh3.HashString(name)
		sh := c.data[h&(shardCount-1)]
		sh.items.setHashed(h, it)
		if i%1000000 == 0 {
			if rss := insertPeakRSS(); rss > 24e9 {
				t.Fatalf("RSS bound exceeded: %d", rss)
			}
			t.Logf("seeded %d/%d RSS=%d", i, n, insertPeakRSS())
		}
	}
	// Size/stat and stable summary setup uses the same inputs for both binaries.
	for _, sh := range c.data {
		c.addSizeLocked(sh, int64(sh.items.n*seed.n), 0)
	}
	c.makeQueueAt(time.Now())
	if os.Getenv("CACHE_INSERT_LOG") != "0" {
		c.EnableInputLog()
	}
	c.SetThrottleWithHash(func(_ *points.Points, exists bool, _ uint64, state *any) bool {
		if !exists {
			panic("resident fixture missed")
		}
		if *state == nil {
			*state = uint64(1)
		}
		return false
	})
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("resident=%d item=%d heap=%d estimated=%d", n, unsafe.Sizeof(item{}), mem.HeapAlloc, c.EstimatedBytes())
	var spare [shardCount]inputlog.Batch
	for trial := -3; trial < trials; trial++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sample := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
		metrics.Read(sample)
		waitBefore := sample[0].Value.Float64()
		var usageBefore, usageAfter syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		cpu := compactCPUTime()
		batchLookup := os.Getenv("CACHE_INSERT_BATCH") == "1"
		var wg sync.WaitGroup
		for conn := range connections {
			wg.Go(func() {
				// Bounded relay-like plaintext writes and a separately reused input buffer.
				block := make([]byte, 0, 65536)
				var key []byte
				for at := conn; at < updates; {
					block = block[:0]
					for len(block) < 60000 && at < updates {
						index := int((int64(at+(trial+3)*updates) * 15485863) % int64(n))
						key = nameFor(key[:0], index)
						block = append(block, key...)
						block = append(block, " 17.25 1790001000\n"...)
						at += connections
					}
					var batchNames [lookupBatchSize][]byte
					var batchData [lookupBatchSize]points.Point
					count := 0
					for rest := block; len(rest) > 0; {
						line, tail, _ := bytes.Cut(rest, []byte{'\n'})
						rest = tail
						name, v, ts, err := parse.PlainLine(line)
						if err != nil {
							panic(err)
						}
						if batchLookup {
							batchNames[count] = name
							batchData[count] = points.Point{Value: v, Timestamp: ts}
							count++
							if count == lookupBatchSize {
								c.AddPointBatch(batchNames[:count], batchData[:count])
								count = 0
							}
						} else {
							c.AddPoint(name, v, ts)
						}
					}
					if count > 0 {
						c.AddPointBatch(batchNames[:count], batchData[:count])
					}
				}
			})
		}
		wg.Wait()
		cpu = compactCPUTime() - cpu
		wall := time.Since(start)
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter); err != nil {
			t.Fatal(err)
		}
		metrics.Read(sample)
		runtime.ReadMemStats(&after)
		result := map[string]any{"cpu_user_ns_point": float64(int64(usageAfter.Utime.Sec-usageBefore.Utime.Sec)*1e9+int64(usageAfter.Utime.Usec-usageBefore.Utime.Usec)*1000) / float64(updates), "cpu_system_ns_point": float64(int64(usageAfter.Stime.Sec-usageBefore.Stime.Sec)*1e9+int64(usageAfter.Stime.Usec-usageBefore.Stime.Usec)*1000) / float64(updates), "major_faults_point": float64(usageAfter.Majflt-usageBefore.Majflt) / float64(updates), "minor_faults_point": float64(usageAfter.Minflt-usageBefore.Minflt) / float64(updates), "gc_cycles": after.NumGC - before.NumGC, "trial": trial, "metrics": n, "connections": connections, "points": updates, "wall_ns_point": float64(wall.Nanoseconds()) / float64(updates), "cpu_ns_point": float64(cpu) / float64(updates), "allocs_point": float64(after.Mallocs-before.Mallocs) / float64(updates), "bytes_point": float64(after.TotalAlloc-before.TotalAlloc) / float64(updates), "mutex_wait_ns_point": (sample[0].Value.Float64() - waitBefore) * 1e9 / float64(updates), "heap_bytes": after.HeapAlloc, "item_bytes": unsafe.Sizeof(item{}), "peak_rss_bytes": insertPeakRSS()}
		raw, _ := json.Marshal(result)
		fmt.Printf("CACHE_INSERT %s\n", raw)
		if insertPeakRSS() > 24e9 {
			t.Fatal("RSS bound exceeded")
		}
		// Drain between trials; no log drops are permitted in the measured window.
		if c.LogDropped() != 0 {
			t.Fatal("input log overflowed")
		}
		for sh := range shardCount {
			spare[sh] = c.TakeLogBatch(sh, uint64(trial+4), spare[sh])
		}
	}
	if c.Size() != int64(n*seed.n+(trials+3)*updates) {
		t.Fatal("scale replay lost points")
	}
	runtime.KeepAlive(c)
}

func insertPeakRSS() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	if runtime.GOOS == "darwin" {
		return int64(u.Maxrss)
	}
	return int64(u.Maxrss) * 1024
}
