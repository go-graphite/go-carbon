//go:build unix

package tcp

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in fixed-offer restart experiment. The sender has five seconds of bounded
// buffering per connection; its clock never slows down with the consumer.
func TestLiveIngestLoad(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("LIVE_METRICS"))
	if n == 0 {
		t.Skip("set LIVE_METRICS for the restart load experiment")
	}
	rate, _ := strconv.Atoi(os.Getenv("LIVE_RATE"))
	if rate == 0 {
		rate = 500000
	}
	seconds, _ := strconv.Atoi(os.Getenv("LIVE_SECONDS"))
	if seconds == 0 {
		seconds = 10
	}
	mode := os.Getenv("LIVE_BACKGROUND")
	compressed := os.Getenv("LIVE_ZSTD") == "1"
	const connections, rows = 16, 128
	c := cache.New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteoutSpread(.2); err != nil {
		t.Fatal(err)
	}
	c.SetWriteoutPacing(true)
	c.PauseStartupWriteout()
	c.SetRestartOrder(func() bool { return true })
	c.SetRestartWarmFirst(func(string) bool { return false }, nil, nil)
	// The production restored cache represents about 680 seconds of live input.
	perMetric := max(1, rate*680/n)
	data := make([]points.Point, perMetric)
	for i := range data {
		data[i] = points.Point{Value: float64(i % 29), Timestamp: 1790000000 + int64(i*60)}
	}
	name := func(i int) string {
		return fmt.Sprintf("services.namespace.%04d.host.%08d.component.requests.region.zone.application.long.metric.measurement.value", i%1000, i)
	}
	for i := range n {
		c.Add(&points.Points{Metric: name(i), Data: data})
	}
	c.ResumeStartupWriteout(time.Now().Add(-480 * time.Second))
	runtime.GC()
	var accepted atomic.Int64
	r, err := newTCP("tcp", &Options{Listen: "127.0.0.1:0", Enabled: true}, func(p *points.Points) { c.Add(p); accepted.Add(int64(len(p.Data))) })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	r.SetPointOut(func(name []byte, v float64, ts int64) { c.AddPoint(name, v, ts); accepted.Add(1) })
	r.SetPointsOut(func(name []byte, data []points.Point) { c.AddPoints(name, data); accepted.Add(int64(len(data))) })
	var conns [connections]net.Conn
	var wire [connections][][]byte
	for w := range connections {
		conns[w], err = net.Dial("tcp", r.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conns[w].Close()
		for batch := range 128 {
			var plain []byte
			for j := range rows {
				plain = fmt.Appendf(plain, "%s 1 1791000000\n", name(int((uint64(w*16384+batch*rows+j)*6364136223846793005>>32)%uint64(n))))
			}
			if compressed {
				plain = encodeFrames(t, plain, 1<<20)
			}
			wire[w] = append(wire[w], plain)
		}
	}
	stop := make(chan struct{})
	var background sync.WaitGroup
	var popped, restored atomic.Int64
	if mode != "none" {
		background.Add(1)
		go func() {
			defer background.Done()
			abort := make(chan bool)
			finished := make(chan struct{})
			defer close(finished)
			go func() {
				select {
				case <-stop:
					close(abort)
				case <-finished:
				}
			}()
			// Use the actual queue producer and eligibility checks. A fixed write rate
			// prevents a memory-only sink from draining the fixture before measurement.
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					for range max(1, rate*3/2/perMetric/200) {
						key := c.WriteoutQueue().Get(abort)
						if key == "" {
							return
						}
						if p, ok := c.PopNotConfirmed(key); ok {
							popped.Add(int64(len(p.Data)))
							c.Confirm(p)
						}
					}
				}
			}
		}()
	}
	if mode == "restore" {
		background.Add(1)
		go func() {
			defer background.Done()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			i := 0
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					for range max(1, rate/4/perMetric/200) {
						c.AddRestored(&points.Points{Metric: name(i % n), Data: data})
						restored.Add(int64(len(data)))
						i++
					}
				}
			}
		}()
	}
	profile := os.Getenv("LIVE_PROFILE")
	if profile != "" {
		f, e := os.Create(profile + "-cpu.pprof")
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if e = pprof.StartCPUProfile(f); e != nil {
			t.Fatal(e)
		}
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(100000)
	}
	cpu := streamBenchCPU(t)
	started := time.Now()
	var senders, transports sync.WaitGroup
	var offered, dropped atomic.Int64
	for w := range connections {
		senders.Add(1)
		go func() {
			defer senders.Done()
			q := make(chan []byte, max(1, rate*5/connections/rows))
			transports.Add(1)
			go func() {
				defer transports.Done()
				for p := range q {
					if _, e := conns[w].Write(p); e != nil {
						return
					}
				}
			}()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			batches := 0
			for elapsed := range ticker.C {
				target := min(seconds*rate/connections/rows, int(elapsed.Sub(started).Seconds()*float64(rate/connections)/rows))
				for batches < target {
					offered.Add(rows)
					select {
					case q <- wire[w][batches%len(wire[w])]:
					default:
						dropped.Add(rows)
					}
					batches++
				}
				if elapsed.Sub(started) >= time.Duration(seconds)*time.Second {
					close(q)
					return
				}
			}
		}()
	}
	senders.Wait()
	elapsed := time.Since(started)
	got := accepted.Load()
	cpu = streamBenchCPU(t) - cpu
	t.Logf("metrics=%d restored=%d ratio_seconds=%.1f rate=%d zstd=%t background=%s offered=%d accepted=%d accepted/offered=%.6f accepted/s=%.0f relay_dropped=%d popped=%d restored_during=%d cpu_cores=%.3f", n, n*perMetric, float64(n*perMetric)/float64(rate), rate, compressed, mode, offered.Load(), got, float64(got)/float64(offered.Load()), float64(got)/elapsed.Seconds(), dropped.Load(), popped.Load(), restored.Load(), float64(cpu)/float64(elapsed))
	close(stop)
	background.Wait()
	// Measurement ends above. Flush the bounded sender tail without background
	// work before closing sockets, and prove every nondropped offer arrived.
	flushed := make(chan struct{})
	go func() { transports.Wait(); close(flushed) }()
	select {
	case <-flushed:
	case <-time.After(30 * time.Second):
		for _, conn := range conns {
			_ = conn.Close()
		}
		t.Fatal("sender tail did not flush")
	}
	for _, conn := range conns {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}
	wantLive := offered.Load() - dropped.Load()
	deadline := time.Now().Add(10 * time.Second)
	for accepted.Load() < wantLive && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	r.Stop()
	if accepted.Load() != wantLive {
		t.Fatalf("wire conservation: received=%d nondropped offered=%d", accepted.Load(), wantLive)
	}
	t.Logf("final queued=%d persisted=%d live=%d restored_during=%d cache_change=%d", c.Size(), popped.Load(), accepted.Load(), restored.Load(), c.Size()-int64(n*perMetric))
	if want := int64(n*perMetric) + accepted.Load() + restored.Load(); c.Size()+popped.Load() != want {
		t.Fatalf("point conservation: queued=%d persisted=%d want=%d", c.Size(), popped.Load(), want)
	}
	if profile != "" {
		pprof.StopCPUProfile()
		for _, kind := range []string{"mutex", "block"} {
			f, e := os.Create(profile + "-" + kind + ".pprof")
			if e != nil {
				t.Fatal(e)
			}
			_ = pprof.Lookup(kind).WriteTo(f, 0)
			_ = f.Close()
		}
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	}
}
