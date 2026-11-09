package carbon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

// Opt-in real shared-store restart with a fixed cache and TCP offered load.
// HANDOFF_RESTART=metrics,points_per_metric,points_per_second; run each mode in
// a separate process (HANDOFF_RESTART_CAPTURE=0/1) for comparable heap pressure.
func TestMeasureHandoffRestart(t *testing.T) {
	spec := strings.Split(os.Getenv("HANDOFF_RESTART"), ",")
	if len(spec) != 3 {
		t.Skip("set HANDOFF_RESTART=metrics,points_per_metric,points_per_second")
	}
	metrics, _ := strconv.Atoi(spec[0])
	history, _ := strconv.Atoi(spec[1])
	rate, _ := strconv.Atoi(spec[2])
	if metrics <= 0 || history <= 0 || rate <= 0 {
		t.Fatal("invalid HANDOFF_RESTART")
	}
	capture := os.Getenv("HANDOFF_RESTART_CAPTURE") == "1"
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "blocks"
	// A short path is needed for the actual Unix descriptor transfer.
	short, err := os.MkdirTemp("/tmp", "hr-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	cfg.Dump.Path = short
	cfg.Dump.StoreInputHandoff = true
	cfg.Cache.MaxSize = 0
	// Cache-only synthetic names would otherwise benchmark thousands of new
	// Whisper-file creations rather than a populated survivor's input handoff.
	// Bound that unrelated work identically in the baseline and candidate.
	cfg.Whisper.Workers, cfg.Whisper.MaxUpdatesPerSecond = 8, 100
	cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 8<<20, 8<<20
	cfg.Dump.RestorePerSecond = 0
	cfg.Common.MaxCPU = runtime.GOMAXPROCS(0)
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	old.Persister.Stop()
	old.Persister = nil
	base := time.Now().Unix() - int64(history) - 120
	name := func(i int) string {
		return fmt.Sprintf("servers.host%06d.application.requests.by_region.region%02d.by_status.success.total.%s", i, i%3, strings.Repeat("segment.", 15)+"segment")
	}
	seedStart := time.Now()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			p := &points.Points{Data: make([]points.Point, history)}
			for i := worker; i < metrics; i += 8 {
				p.Metric = name(i)
				for j := range p.Data {
					p.Data[j] = points.Point{Timestamp: base + int64(j), Value: float64((uint64(i+1)*6364136223846793005+uint64(j)*1442695040888963407)%1000000) / 1000}
				}
				old.Cache.Add(p)
			}
		})
	}
	wg.Wait()
	t.Logf("seed points=%d metrics=%d estimated_bytes=%d time=%v", old.Cache.Size(), metrics, old.Cache.EstimatedBytes(), time.Since(seedStart))
	address := old.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	nextCfg := *old.Config
	nextCfg.Tcp = tcp.NewOptions()
	*nextCfg.Tcp = *cfg.Tcp
	nextCfg.Tcp.Listen = address
	nextCfg.Dump.StoreInputHandoff = capture
	next := New(path)
	next.Config = &nextCfg
	defer next.Stop()
	started := make(chan error, 1)
	old.SetReadHandoff(storeHandoffSuccessor(t, true), func() {
		go func() { next.RegisterReadHandoff(); started <- next.Start() }()
	})
	saved := handoff.PeerExecutable
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return storeHandoffSuccessor(t, true), nil }
	defer func() { handoff.PeerExecutable = saved }()
	stop := make(chan struct{})
	stopFeed := sync.OnceFunc(func() { close(stop) })
	defer stopFeed()
	var limited atomic.Bool
	feedLimit := time.AfterFunc(30*time.Second, func() { limited.Store(true); stopFeed() })
	defer feedLimit.Stop()
	debug := time.AfterFunc(10*time.Second, func() {
		fmt.Println(zapwriter.TestString())
		if path := os.Getenv("HANDOFF_STACK_PATH"); path != "" {
			if f, err := os.Create(path); err == nil {
				_ = pprof.Lookup("goroutine").WriteTo(f, 2)
				_ = f.Close()
			}
		}
	})
	defer debug.Stop()
	var sent atomic.Int64
	var bufferRetries atomic.Int64
	var producers sync.WaitGroup
	for worker := range 16 {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		producers.Go(func() {
			defer conn.Close()
			_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Minute))
			pending := make([]byte, 0, 80<<10)
			batch := max(1, rate/(16*1000))
			now := time.Now().Unix() - 1
			for i := range batch {
				pending = fmt.Appendf(pending, "%s %d %d\n", name((worker*batch+i)%min(metrics, 8192)), i, now)
			}
			begin := time.Now()
			var count int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				for rest := pending; len(rest) > 0; {
					n, err := conn.Write(rest)
					rest = rest[n:]
					if errors.Is(err, syscall.ENOBUFS) {
						bufferRetries.Add(1)
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil {
						t.Error(err)
						return
					}
				}
				count += int64(batch)
				sent.Add(int64(batch))
				due := begin.Add(time.Duration(float64(count) * float64(time.Second) * 16 / float64(rate)))
				if delay := time.Until(due); delay > 0 {
					time.Sleep(delay)
				}
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	stopBegan := time.Now()
	if err := old.DumpStop(); err != nil {
		stopFeed()
		producers.Wait()
		t.Fatal(err)
	}
	// A real predecessor exits at commit. Release its cache and index rather
	// than pinning a second full daemon's heap throughout successor recovery.
	old.RecordStopExit()
	old.Stop()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err, zapwriter.TestString())
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("start timed out", zapwriter.TestString())
	}
	stopFeed()
	producers.Wait()
	old.restart.mu.Lock()
	closed := old.restart.out.InputClosed
	old.restart.mu.Unlock()
	next.restart.mu.Lock()
	opened := next.restart.inputOpened
	next.restart.mu.Unlock()
	var pause time.Duration
	if m := regexp.MustCompile(`startup input capture switched to live input .*"input_pause": ?([0-9.e+-]+)`).FindStringSubmatch(zapwriter.TestString()); m != nil {
		seconds, _ := strconv.ParseFloat(m[1], 64)
		pause = time.Duration(seconds * float64(time.Second))
	}
	t.Logf("capture=%v offered_rate=%d sent=%d stop_and_start=%v input_unavailable=%v initial_input_gap=%v capture_switch=%v feeder_limit_reached=%v sender_enobufs_retries=%d", capture, rate, sent.Load(), time.Since(stopBegan), opened.Sub(closed)+pause, opened.Sub(closed), pause, limited.Load(), bufferRetries.Load())
	if capture && limited.Load() {
		t.Error("capture did not catch up under ongoing input before the fixture's disk safety limit")
	}
	for _, line := range strings.Split(zapwriter.TestString(), "\n") {
		for _, key := range []string{"cache dump finished", "input handed over", "input capture active", "startup input capture sealed", "checkpoint", "input tail loaded", "start phases"} {
			if strings.Contains(line, key) {
				t.Log(line)
				break
			}
		}
	}
}
