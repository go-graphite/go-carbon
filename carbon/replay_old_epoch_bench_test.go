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
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in actual old-epoch reader plus plaintext TCP intake. Unlike the smaller
// promotion trace, history remains ahead of the current epoch during intake.
func TestReplayFixOldEpochTCP(t *testing.T) {
	if os.Getenv("SB5_OLD_TCP") != "1" {
		t.Skip("set SB5_OLD_TCP=1")
	}
	const keys, rate, clients, batch = 25000000, 4000000, 190, 4096
	core := cache.New()
	core.SetMaxSize(0)
	core.SetRecoveryMemory(func() (uint64, bool) { return 24 << 30, true })
	name := func(i int) string { return fmt.Sprintf("production.%08d.%s", i, strings.Repeat("x", 80)) }
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Go(func() {
			data := make([]points.Point, 32)
			for j := range data {
				data[j] = points.Point{Value: float64(j), Timestamp: int64(1700000000 + j)}
			}
			for i := worker; i < keys; i += 16 {
				core.AddRecovered(&points.Points{Metric: name(i), Data: data})
			}
		})
	}
	wg.Wait()
	oldPath := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	old, err := openStartupInputWithPolicy(oldPath, chooseStartupPolicy(runtime.GOMAXPROCS(0), 24<<30, 1<<40))
	if err != nil {
		t.Fatal(err)
	}
	// Prebuild only the live frames offered in this run. Older history sweeps all
	// 25M identities once, with one point per name/encoded frame.
	frames := (keys + batch - 1) / batch
	perClient := (2*rate + clients*batch - 1) / (clients * batch)
	plaintext := make([][]byte, clients*perClient)
	for worker := range len(old.shards) {
		wg.Go(func() {
			raw := make([]byte, 0, batch*120)
			for f := worker; f < frames; f += len(old.shards) {
				raw = raw[:0]
				var text []byte
				if f < len(plaintext) {
					text = make([]byte, 0, batch*125)
				}
				count := min(batch, keys-f*batch)
				for j := range count {
					i := f*batch + j
					metric := name(i)
					raw = points.OnePoint(metric, float64(i), 1700000100).AppendBinary(raw)
					if text != nil {
						text = append(text, metric...)
						text = append(text, ' ')
						text = strconv.AppendInt(text, int64(i), 10)
						text = append(text, " 1700000100\n"...)
					}
				}
				if text != nil {
					plaintext[f] = text
				}
				if e := old.shards[worker].Write(raw, count); e != nil {
					t.Error(e)
					return
				}
			}
		})
	}
	wg.Wait()
	if err = old.close(); err != nil {
		t.Fatal(err)
	}
	s, address := captureFixture(t)
	paths, err := filepath.Glob(oldPath + ".capture-*.tail.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		target := s.path + strings.TrimPrefix(p, oldPath)
		if err = os.Rename(p, target); err != nil {
			t.Fatal(err)
		}
	}
	conns := make([]net.Conn, clients)
	for i := range conns {
		conns[i], err = net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		defer conns[i].Close()
	}
	if prefix := os.Getenv("SB5_PROFILE_PREFIX"); prefix != "" {
		f, e := os.Create(prefix + "-cpu.pprof")
		if e != nil {
			t.Fatal(e)
		}
		if e = pprof.StartCPUProfile(f); e != nil {
			t.Fatal(e)
		}
		fraction := runtime.SetMutexProfileFraction(1)
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			m, e := os.Create(prefix + "-mutex.pprof")
			if e != nil {
				t.Error(e)
				return
			}
			if e = pprof.Lookup("mutex").WriteTo(m, 0); e != nil {
				t.Error(e)
			}
			_ = m.Close()
			runtime.SetMutexProfileFraction(fraction)
		}()
	}
	app := New("")
	app.startupInput = s
	started := time.Now()
	app.finishStartupInput(core)
	start := time.Now().Add(50 * time.Millisecond)
	var maxLag [clients]time.Duration
	for c, conn := range conns {
		wg.Go(func() {
			_ = conn.SetWriteDeadline(start.Add(20 * time.Second))
			for n := range perClient {
				target := start.Add(time.Duration(float64(n*batch*clients+c*batch) / rate * float64(time.Second)))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
				raw := plaintext[n*clients+c]
				for len(raw) > 0 {
					k, e := conn.Write(raw)
					if e != nil {
						t.Error(e)
						return
					}
					raw = raw[k:]
				}
				maxLag[c] = max(maxLag[c], time.Since(target))
			}
			_ = conn.Close()
		})
	}
	offered := int64(clients * perClient * batch)
	historyDone := make(chan time.Duration, 1)
	go func() {
		for !s.historyReady.Load() {
			select {
			case <-s.replayStop:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
		historyDone <- time.Since(started)
	}()
	wg.Wait()
	awaitCapture(t, func() bool { return s.points.Load() == offered })
	captureDuration := time.Since(start)
	var oldDuration time.Duration
	select {
	case oldDuration = <-historyDone:
	case <-time.After(30 * time.Second):
		t.Fatal("old replay did not finish")
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == offered })
	var lag time.Duration
	for _, d := range maxLag {
		lag = max(lag, d)
	}
	oldRate := float64(keys) / oldDuration.Seconds()
	intake := float64(offered) / captureDuration.Seconds()
	t.Logf("old_names=%d name_bytes=100 history_points=%d offered=%d target=%d/s capture_duration=%v intake=%.0f/s old_replay_duration=%v old_replay=%.0f/s ratio=%.3f max_sender_lag=%v current_backlog=%d", keys, keys*32, offered, rate, captureDuration, intake, oldDuration, oldRate, oldRate/rate, lag, s.points.Load()-s.replayedPoints.Load())
	if got, want := core.Size(), int64(keys)*33+offered; got != want {
		t.Fatal("missing/duplicate points", got, want)
	}
	if oldRate < float64(rate)*2.5 || intake < float64(rate)*.99 {
		t.Fatal("old replay/intake performance gate failed")
	}
}
