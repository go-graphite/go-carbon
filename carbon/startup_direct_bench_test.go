//go:build unix

package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/tcp"
)

// Opt-in: use the replay-fix 25M x 100-byte-name / 800M-history-point cache
// shape, then drive 190 real TCP connections at 4M/s before/after transition.
// Profiles exclude fixture construction and use identical prebuilt input.
func TestStartupDirectProductionShape(t *testing.T) {
	if os.Getenv("CAPTURE_LIVE_SCALE") != "1" {
		t.Skip("set CAPTURE_LIVE_SCALE=1")
	}
	keys, rate, seconds := 25000000, 4000000, 7
	for key, target := range map[string]*int{"CAPTURE_LIVE_KEYS": &keys, "CAPTURE_LIVE_RATE": &rate, "CAPTURE_LIVE_SECONDS": &seconds} {
		if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
			*target = v
		}
	}
	const clients, batch = 190, 512
	if keys < clients*batch {
		t.Fatal("fixture requires at least one full frame per connection")
	}
	core := cache.New()
	core.SetMaxSize(0)
	core.SetRecoveryMemory(func() (uint64, bool) { return 24 << 30, true })
	name := func(i int) string { return fmt.Sprintf("production.%08d.%s", i, strings.Repeat("x", 80)) }
	var wg sync.WaitGroup
	started := time.Now()
	for worker := range min(16, runtime.GOMAXPROCS(0)) {
		wg.Go(func() {
			data := make([]points.Point, 32)
			for j := range data {
				data[j] = points.Point{Value: float64(j), Timestamp: int64(1700000000 + j)}
			}
			for i := worker; i < keys; i += min(16, runtime.GOMAXPROCS(0)) {
				core.AddRecovered(&points.Points{Metric: name(i), Data: data})
			}
		})
	}
	wg.Wait()
	t.Logf("seed names=%d name_bytes=%d history_points=%d estimated_bytes=%d duration=%v", keys, len(name(0)), core.Size(), core.EstimatedBytes(), time.Since(started))
	raw := make([][][]byte, clients)
	for worker := range clients {
		wg.Go(func() {
			for base := 0; base+batch <= keys/clients; base += batch {
				frame := make([]byte, 0, batch*120)
				for j := range batch {
					frame = fmt.Appendf(frame, "%s %d %d\n", name((base+j)*clients+worker), (base+j)%101, 1700000100+(base+j)/101)
				}
				raw[worker] = append(raw[worker], frame)
			}
		})
	}
	wg.Wait()
	t.Logf("prepared_names=%d raw_frames=%d", len(raw[0])*batch*clients, len(raw[0])*clients)
	s, address := captureFixture(t)
	app := New("")
	app.Config = NewConfig()
	app.Config.InputLog.Enabled = true
	app.Config.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	app.Config.InputLog.SegmentSize = 64 << 20
	// The seed models independent checkpoint-owned history, not journal input.
	core.EnableInputLogFromNow()
	if err := app.openInputLog(core); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.closeInputLog)
	app.startupInput = s
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return allCaptureLive(s) })
	s.reclaim()
	cpuSeconds := func() float64 {
		var r syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
			t.Fatal(err)
		}
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	total := int64(keys) * 32
	phase := func(label, addr string) (float64, float64) {
		conns := make([]net.Conn, clients)
		for i := range conns {
			var err error
			conns[i], err = net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
		}
		var profile *os.File
		if dir := os.Getenv("CAPTURE_LIVE_PROFILE_DIR"); dir != "" {
			var err error
			profile, err = os.Create(filepath.Join(dir, "capture-live-"+label+".cpu.pprof"))
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(profile); err != nil {
				t.Fatal(err)
			}
		}
		loops := max(1, rate*seconds/(clients*batch))
		count := int64(loops * clients * batch)
		beforeCPU, beforeBytes := cpuSeconds(), s.size.Load()
		beforeLog := app.inputLog.Load().Metrics()
		start := time.Now().Add(100 * time.Millisecond)
		var maxLag time.Duration
		var lagMu sync.Mutex
		for worker := range clients {
			wg.Go(func() {
				defer conns[worker].Close()
				for seq := range loops {
					target := start.Add(time.Duration(float64(seq*clients*batch+worker*batch) / float64(rate) * float64(time.Second)))
					if d := time.Until(target); d > 0 {
						time.Sleep(d)
					}
					if _, err := conns[worker].Write(raw[worker][seq%len(raw[worker])]); err != nil {
						t.Error(err)
						return
					}
					lag := time.Since(target)
					lagMu.Lock()
					maxLag = max(maxLag, lag)
					lagMu.Unlock()
				}
			})
		}
		wg.Wait()
		senderTime := time.Since(start)
		total += count
		awaitCapture(t, func() bool { return core.Size() == total })
		if err := app.syncInputLog(); err != nil {
			t.Fatal(err)
		}
		cpu := cpuSeconds() - beforeCPU
		if profile != nil {
			pprof.StopCPUProfile()
			if err := profile.Close(); err != nil {
				t.Fatal(err)
			}
		}
		log := app.inputLog.Load().Metrics()
		bpp := float64(log.BytesWritten-beforeLog.BytesWritten) / float64(count)
		t.Logf("phase=%s points=%d offered=%d/s achieved=%.0f/s producer=%v max_sender_lag=%v cpu_seconds=%.6f cpu_ns_per_point=%.2f spool_growth=%d log_points=%d log_bytes_per_point=%.6f", label, count, rate, float64(count)/senderTime.Seconds(), senderTime, maxLag, cpu, cpu*1e9/float64(count), s.size.Load()-beforeBytes, log.PointsWritten-beforeLog.PointsWritten, bpp)
		if senderTime > time.Duration(float64(count)/float64(rate)*float64(time.Second)/.99) {
			t.Errorf("%s intake below 99%% of offered rate", label)
		}
		if label != "spool" && (s.size.Load() != beforeBytes || log.PointsWritten-beforeLog.PointsWritten != count) {
			t.Error("direct growth/journal accounting gate failed")
		}
		return cpu * 1e9 / float64(count), bpp
	}
	spoolCPU, _ := phase("spool", address)
	goDirect(t, s)
	directCPU, directBPP := phase("direct", address)
	// An ordinary receiver on the same cache provides a steady-state control.
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	ro, err := receiver.WithProtocol(opts, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	out, err := receiver.New("tcp", ro, core.Add)
	if err != nil {
		t.Fatal(err)
	}
	normal := out.(*tcp.TCP)
	defer normal.Stop()
	normal.SetPointOut(core.AddPoint)
	normal.SetPointsOut(core.AddPoints)
	normal.SetPointBatchOut(core.AddPointBatch)
	normalCPU, normalBPP := phase("ordinary", normal.Addr().String())
	t.Logf("direct_vs_spool_cpu=%.4f direct_vs_ordinary_cpu=%.4f direct_Bpp=%.6f ordinary_warm_dictionary_Bpp=%.6f final_points=%d", directCPU/spoolCPU, directCPU/normalCPU, directBPP, normalBPP, core.Size())
}
