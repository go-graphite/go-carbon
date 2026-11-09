//go:build linux || darwin

package tcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

// TestCacheInsertReceiverScale drives the actual plaintext connection handler.
// Its 20M resident metrics start with one point (the cache-package scale test
// separately reproduces 237-point resident buffers). Names stay private.
func TestCacheInsertReceiverScale(t *testing.T) {
	path := os.Getenv("CACHE_INSERT_NAMES")
	if path == "" {
		t.Skip("set CACHE_INSERT_NAMES to a private fixture")
	}
	env := func(k string, d int) int {
		if s := os.Getenv(k); s != "" {
			v, e := strconv.Atoi(s)
			if e != nil {
				t.Fatal(e)
			}
			return v
		}
		return d
	}
	n, connections, trials, updates := env("CACHE_INSERT_METRICS", 20000000), env("CACHE_INSERT_CONNECTIONS", 16), env("CACHE_INSERT_TRIALS", 4), env("CACHE_INSERT_UPDATES", 8000000)
	runLength := env("CACHE_INSERT_RUN_LENGTH", 1)
	if runLength < 1 {
		t.Fatal("invalid run length")
	}
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
	c := cache.New()
	c.SetMaxSize(0)
	c.SetBloomSize(uint64(n))
	c.SetNewMetricsChan(make(chan string, 1))
	c.SetMetricExists(func(string) bool { return true })
	if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
		t.Fatal(err)
	}
	c.SetThrottleWithHash(func(_ *points.Points, exists bool, _ uint64, state *any) bool {
		if *state == nil {
			*state = uint64(1)
		}
		return false
	})
	var key []byte
	for i := range n {
		key = nameFor(key[:0], i)
		c.AddPoint(key, 1, 1790000000)
		if i%1000000 == 0 {
			t.Logf("seeded %d/%d", i, n)
		}
	}
	c.EnableInputLog()
	runtime.GC()
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
	r.SetPointOut(c.AddPoint)
	r.SetPointsOut(c.AddPoints)
	r.cacheLookupBatch = os.Getenv("CACHE_INSERT_BATCH") == "1"
	r.SetPointBatchOut(c.AddPointBatch)
	r.Start()
	defer r.Stop()
	var spare [1024]inputlog.Batch
	for trial := -3; trial < trials; trial++ {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		sample := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
		metrics.Read(sample)
		wait := sample[0].Value.Float64()
		cpu := receiverScaleCPU()
		start := time.Now()
		var wg sync.WaitGroup
		for conn := range connections {
			wg.Go(func() {
				input := &cacheInsertConn{makeBlock: func(dst []byte, at int) ([]byte, int) {
					for len(dst) < 60000 && at < updates {
						group := conn + ((at-conn)/connections/runLength)*connections + (trial+3)*updates/runLength
						index := int(int64(group) * 15485863 % int64(n))
						dst = nameFor(dst, index)
						dst = append(dst, " 17.25 1790001000\n"...)
						at += connections
					}
					return dst, at
				}, at: conn, limit: updates}
				r.HandleConnection(input)
			})
		}
		wg.Wait()
		elapsed := time.Since(start)
		cpu = receiverScaleCPU() - cpu
		metrics.Read(sample)
		runtime.ReadMemStats(&after)
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		rss := int64(usage.Maxrss)
		if runtime.GOOS != "darwin" {
			rss *= 1024
		}
		result := map[string]any{"gc_cycles": after.NumGC - before.NumGC, "receiver": true, "trial": trial, "metrics": n, "connections": connections, "points": updates, "cpu_ns_point": float64(cpu) / float64(updates), "wall_ns_point": float64(elapsed.Nanoseconds()) / float64(updates), "allocs_point": float64(after.Mallocs-before.Mallocs) / float64(updates), "bytes_point": float64(after.TotalAlloc-before.TotalAlloc) / float64(updates), "mutex_wait_ns_point": (sample[0].Value.Float64() - wait) * 1e9 / float64(updates), "peak_rss_bytes": rss}
		raw, _ := json.Marshal(result)
		fmt.Printf("CACHE_INSERT %s\n", raw)
		if rss > 24e9 {
			t.Fatal("RSS bound exceeded")
		}
		if c.LogDropped() != 0 {
			t.Fatal("input log overflowed")
		}
		for sh := range 1024 {
			spare[sh] = c.TakeLogBatch(sh, uint64(trial+4), spare[sh])
		}
	}
	if c.Size() != int64(n+(trials+3)*updates) {
		t.Fatal("receiver lost points")
	}
}

func receiverScaleCPU() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + int64(u.Utime.Usec+u.Stime.Usec)*1000
}

type cacheInsertConn struct {
	reader    bytes.Reader
	block     []byte
	makeBlock func([]byte, int) ([]byte, int)
	at, limit int
}

func (c *cacheInsertConn) Read(p []byte) (int, error) {
	if c.reader.Len() == 0 {
		if c.at >= c.limit {
			return 0, io.EOF
		}
		c.block, c.at = c.makeBlock(c.block[:0], c.at)
		c.reader.Reset(c.block)
	}
	return c.reader.Read(p)
}
func (*cacheInsertConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (*cacheInsertConn) Close() error                     { return nil }
func (*cacheInsertConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*cacheInsertConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*cacheInsertConn) SetDeadline(time.Time) error      { return nil }
func (*cacheInsertConn) SetReadDeadline(time.Time) error  { return nil }
func (*cacheInsertConn) SetWriteDeadline(time.Time) error { return nil }
