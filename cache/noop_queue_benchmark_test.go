package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// This optional component benchmark uses real production names. Point counts
// are controlled; it is not a captured ingestion or full-service replay.
func BenchmarkProductionNoopDrain(b *testing.B) {
	path := os.Getenv("GO_CARBON_NOOP_NAMES")
	if path == "" {
		b.Skip("production metric-name fixture not supplied")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		b.Fatal(err)
	}
	if len(names) < 100000 {
		b.Fatal("at least 100000 production names required")
	}
	b.Logf("names=%d sha256=%x workers=96 GOMAXPROCS=%d; controlled36-point batches, cache component only", len(names), sha256.Sum256(data), runtime.GOMAXPROCS(0))
	b.ResetTimer()
	b.StopTimer()
	var cpuSeconds float64
	for iteration := 0; iteration < b.N; iteration++ {
		c := New()
		c.SetMaxSize(400000000)
		for _, name := range names {
			p := &points.Points{Metric: name, Data: make([]points.Point, 36)}
			for j := range p.Data {
				p.Data[j] = points.Point{Timestamp: int64(100000 + (j*17)%36), Value: float64(j) + 0.125}
			}
			c.Add(p)
		}
		runtime.GC()
		var before, after syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		q, _ := c.makeQueueAt(time.Now())
		close(q)
		var drained, bad atomic.Int64
		var wg sync.WaitGroup
		wg.Add(96)
		for worker := 0; worker < 96; worker++ {
			go func() {
				defer wg.Done()
				for name := range q {
					p, ok := c.PopNotConfirmed(name)
					if !ok || len(p.Data) != 36 {
						bad.Add(1)
						continue
					}
					for j, point := range p.Data {
						if point.Timestamp != int64(100000+(j*17)%36) || point.Value != float64(j)+0.125 {
							bad.Add(1)
						}
					}
					c.Confirm(p)
					drained.Add(1)
				}
			}()
		}
		wg.Wait()
		b.StopTimer()
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
			b.Fatal(err)
		}
		seconds := func(r syscall.Rusage) float64 {
			return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
		}
		cpuSeconds += seconds(after) - seconds(before)
		if bad.Load() != 0 || drained.Load() != int64(len(names)) || c.Size() != 0 || c.Len() != 0 || c.NotConfirmedLength() != 0 {
			b.Fatal(fmt.Sprintf("incorrect drain: bad=%d drained=%d size=%d metrics=%d pending=%d", bad.Load(), drained.Load(), c.Size(), c.Len(), c.NotConfirmedLength()))
		}
	}
	b.ReportMetric(cpuSeconds*1e9/float64(b.N*len(names)), "cpu-ns/metric")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(names)), "wall-ns/metric")
}
