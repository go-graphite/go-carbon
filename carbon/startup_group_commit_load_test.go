package carbon

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

// Explicitly isolated, capture-only qualification. Ordinary startup replay and
// archive correctness are exercised separately; this never restarts a daemon.
func TestStartupGroupCommitCaptureGate(t *testing.T) {
	if os.Getenv("GROUP_COMMIT_CAPTURE_GATE") != "1" {
		t.Skip("explicit capture gate")
	}
	if runtime.GOOS != "linux" || os.Getenv("STARTUP_LOAD_ISOLATED") != "1" {
		t.Fatal("isolated Linux required")
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	seconds := 600
	if s := os.Getenv("GROUP_COMMIT_SECONDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatal(s)
		}
		seconds = n
	}
	rate := int64(26_000_000)
	const connections = 280
	output := os.Getenv("GROUP_COMMIT_OUTPUT")
	f, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	evidence := json.NewEncoder(f)
	app := journalStartupFixture(t, t.TempDir(), func(c *Config) {
		c.Tags.Enabled = false
		c.InputLog.SegmentSize = 256 << 20
		// Each stream has 256 names. Reserve a bounded dictionary that also
		// permits all 280 connections inside the ordinary global budget.
		c.Tcp.StreamDictionaryBytes = 1 << 20
	})
	defer closeJournalFixture(app)
	s := app.startupInput
	stamp := time.Now().Unix() - 30
	wires := make([][2][8][]byte, connections)
	for id := range connections {
		for extra := range 2 {
			n := int(rate/int64(connections)/10) + extra
			for phase := range 8 {
				e, err := codec.NewEncoder(codec.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err := e.Reset(); err != nil {
					t.Fatal(err)
				}
				wire, err := codec.AppendPreamble(nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				for i := range n {
					bits := []uint64{0, 1 << 63, 1, 0x3ff0000000000001, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 0x4000000000000001}
					name := fmt.Sprintf("load.c%03d.m%04d;a=first;z=last", id, i%256)
					if err := e.Add([]byte(name), math.Float64frombits(bits[(i+phase)%len(bits)]), stamp+int64(i/256%8)); err != nil {
						t.Fatal(err)
					}
				}
				wire, err = e.Flush(wire)
				e.Close()
				if err != nil {
					t.Fatal(err)
				}
				wires[id][extra][phase] = wire
			}
		}
	}

	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for range connections {
		c, err := net.DialTimeout("tcp", s.receiver.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	var usage syscall.Rusage
	cpu := func() float64 {
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	}
	var sent atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	failures := make(chan error, connections)
	start := time.Now().Add(100 * time.Millisecond)
	cpuStart := cpu()
	for id, c := range conns {
		wg.Go(func() {
			for tick := range seconds * 10 {
				at := start.Add(time.Duration(tick) * 100 * time.Millisecond)
				timer := time.NewTimer(max(0, time.Until(at)))
				select {
				case <-stop:
					timer.Stop()
					return
				case <-timer.C:
				}
				n := rate / 10 / connections
				if int64(id) < (rate/10)%connections {
					n++
				}
				extra := int(n - rate/connections/10)
				wire := wires[id][extra][tick%8]
				if tick > 0 {
					wire = wire[codec.PreambleSize:]
				}
				_ = c.SetWriteDeadline(at.Add(time.Second))
				written, err := c.Write(wire)
				if err != nil || written != len(wire) {
					failures <- fmt.Errorf("sender %d: %d/%d %v", id, written, len(wire), err)
					return
				}
				sent.Add(n)
			}
		})
	}
	defer func() {
		close(stop)
		for _, c := range conns {
			c.Close()
		}
		wg.Wait()
	}()
	var memory runtime.MemStats
	var peakHeap uint64
	previous := int64(0)
	minRate := float64(rate)
	previousAt := start
	for second := 1; second <= seconds; second++ {
		time.Sleep(max(0, time.Until(start.Add(time.Duration(second)*time.Second))))
		at := time.Now()
		intake := s.points.Load()
		actual := float64(intake-previous) / at.Sub(previousAt).Seconds()
		if second > 1 {
			minRate = min(minRate, actual)
		}
		previous, previousAt = intake, at
		m := app.inputLog.Load().Metrics()
		total, maximum, calls := s.receiver.CaptureAdmission()
		runtime.ReadMemStats(&memory)
		peakHeap = max(peakHeap, memory.HeapAlloc)
		_ = evidence.Encode(map[string]any{"second": second, "sent": sent.Load(), "intake": intake, "actual_rate": actual, "syncs": m.Syncs, "durable_points": m.AppendDurablePoints, "admission_total_ns": total, "admission_max_ns": maximum, "admission_calls": calls, "staged_peak_bytes": s.journal.budget.peak.Load(), "heap_bytes": memory.HeapAlloc, "cpu_seconds": cpu() - cpuStart, "max_sync_exposure_ns": m.AppendMaxSyncExposure.Nanoseconds()})
		select {
		case err := <-failures:
			t.Fatal(err)
		default:
		}
	}
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for s.points.Load() < sent.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start).Seconds()
	m := app.inputLog.Load().Metrics()
	total, maximum, calls := s.receiver.CaptureAdmission()
	verdict := "CAPTURE_ONLY_NOT_STARTUP_REPLAY_QUALIFIED"
	_ = evidence.Encode(map[string]any{"verdict": verdict, "seconds": seconds, "elapsed": elapsed, "offered_rate": rate, "intake": s.points.Load(), "sent": sent.Load(), "durable_points": m.AppendDurablePoints, "syncs_per_second": float64(m.Syncs) / elapsed, "actual_intake_per_second": float64(s.points.Load()) / elapsed, "minimum_sample_rate": minRate, "cpu_ns_per_point": (cpu() - cpuStart) * 1e9 / float64(s.points.Load()), "staged_peak_bytes": s.journal.budget.peak.Load(), "peak_heap_bytes": peakHeap, "admission_total_ns": total, "admission_max_ns": maximum, "admission_calls": calls, "max_sync_exposure_ns": m.AppendMaxSyncExposure.Nanoseconds()})
	_ = f.Sync()
	if s.points.Load() != sent.Load() || m.AppendDurablePoints != sent.Load() || sent.Load() != rate*int64(seconds) {
		t.Fatal("capture lost points")
	}
	if float64(s.points.Load())/elapsed < 25_000_000 {
		t.Fatal("actual capture below 25M/s")
	}
}
