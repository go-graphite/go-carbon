//go:build unix

package cache

import (
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/inputlog/loadgen"
)

// TestInputLogCycleCost measures the log's own work per cycle at one point
// per metric per cycle, as one second of input at that rate: the summed
// worker time of each stage, so collect+encode+compress is the cores the log
// needs at that rate, and the bytes it writes per point. Run it explicitly:
//
//	INPUTLOG_COST_METRICS=4500000 go test -run TestInputLogCycleCost -v ./cache/
func TestInputLogCycleCost(t *testing.T) {
	metrics, _ := strconv.Atoi(os.Getenv("INPUTLOG_COST_METRICS"))
	if metrics <= 0 {
		t.Skip("set INPUTLOG_COST_METRICS to run")
	}
	cycles := 8
	mix := loadgen.New(metrics, 1)
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{SegmentSize: 1 << 30, Workers: 8})
	defer l.Kill()
	producers := runtime.GOMAXPROCS(0)
	rs := make([]*rand.Rand, producers)
	for p := range rs {
		rs[p] = rand.New(rand.NewPCG(uint64(p), 3))
	}
	var steady inputlog.Metrics
	for cycle := range cycles {
		var wg sync.WaitGroup
		for p := range producers {
			wg.Go(func() {
				ts := int64(1_790_000_000 + 10*cycle)
				for i := p; i < metrics; i += producers {
					c.AddPoint(mix.Names[i], mix.Next(i, rs[p]), ts)
				}
			})
		}
		wg.Wait()
		before := l.Metrics()
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		after := l.Metrics()
		d := func(s int) time.Duration { return after.Stages[s] - before.Stages[s] }
		points := after.PointsWritten - before.PointsWritten
		cpu := d(inputlog.StageCollect) + d(inputlog.StageEncode) + d(inputlog.StageCompress)
		t.Logf("cycle %d: %d points, %.2f B/pt, collect %v encode %v compress %v write %v sync %v: %.2f cores at %d points/s",
			cycle, points, float64(after.BytesWritten-before.BytesWritten)/float64(points),
			d(inputlog.StageCollect).Round(time.Millisecond), d(inputlog.StageEncode).Round(time.Millisecond),
			d(inputlog.StageCompress).Round(time.Millisecond), d(inputlog.StageWrite).Round(time.Millisecond),
			d(inputlog.StageSync).Round(time.Millisecond), cpu.Seconds(), points)
		if cycle == 1 {
			steady = before
		}
	}
	end := l.Metrics()
	n := float64(end.PointsWritten - steady.PointsWritten)
	var cpu time.Duration
	for _, s := range []int{inputlog.StageCollect, inputlog.StageEncode, inputlog.StageCompress} {
		cpu += end.Stages[s] - steady.Stages[s]
	}
	steadyCycles := float64(cycles - 1)
	t.Logf("steady (cycles 1-%d): %.2f B/pt, %.2f cores at %.0f points/s (%.0f ns/pt)", cycles-1,
		float64(end.BytesWritten-steady.BytesWritten)/n, cpu.Seconds()/steadyCycles, n/steadyCycles, float64(cpu.Nanoseconds())/n)
}
