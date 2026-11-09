package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/tcp"
)

// Opt-in, bounded measurement of the actual point callback used by TCP during
// handoff. Include parse/socket cost in the separate end-to-end restart run.
func TestMeasureHandoffTailRate(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("HANDOFF_TAIL_SECONDS"))
	if seconds <= 0 {
		t.Skip("set HANDOFF_TAIL_SECONDS")
	}
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	w, err := recovery.NewWriter(path, 1, recovery.InputFileBuffer, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := cache.New()
	s := &switchWriter{write: w.WritePoints}
	c.DivertToPointWriter(s.Write)
	names := make([][]byte, 8192)
	for i := range names {
		names[i] = []byte(fmt.Sprintf("servers.host%05d.application.component.requests.by_region.region%02d.by_status.success.total.%s", i, i%3, strings.Repeat("segment.", 15)+"segment"))
	}
	var wg sync.WaitGroup
	var counts [16]int64
	start := time.Now()
	until := start.Add(time.Duration(seconds) * time.Second)
	for worker := range counts {
		wg.Go(func() {
			for time.Now().Before(until) {
				for j := range 1024 {
					i := (int(counts[worker]) + j*16 + worker) % len(names)
					c.AddPoint(names[i], float64(j), 1700000000+counts[worker]/8192)
				}
				counts[worker] += 1024
			}
		})
	}
	wg.Wait()
	writeTime := time.Since(start)
	f, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, n := range counts {
		count += n
	}
	if w.Points() != count {
		t.Fatal("count", w.Points(), count)
	}
	t.Logf("points=%d rate=%.0f/s write=%v seal=%v bytes=%d", count, float64(count)/writeTime.Seconds(), writeTime, time.Since(start)-writeTime, f.Size)
}

// Full TCP, parsing, allocation, framing and disk measurement. Each connection
// sends 238-byte production-shaped names; writers exert real TCP backpressure.
func TestMeasureHandoffTCPRate(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("HANDOFF_TCP_SECONDS"))
	if seconds <= 0 {
		t.Skip("set HANDOFF_TCP_SECONDS")
	}
	tcp.Register()
	for _, mode := range []string{"before", "capture"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
			c := cache.New()
			w, err := recovery.NewWriter(path, 1, recovery.InputFileBuffer, nil)
			if err != nil {
				t.Fatal(err)
			}
			divert := &switchWriter{write: w.WritePoints}
			c.DivertToPointWriter(divert.Write)
			opts := tcp.NewOptions()
			opts.Listen = "127.0.0.1:0"
			ro, err := receiver.WithProtocol(opts, "tcp")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := receiver.New("tcp", ro, c.Add)
			if err != nil {
				t.Fatal(err)
			}
			r := raw.(*tcp.TCP)
			address := r.Addr().String()
			r.SetPointOut(c.AddPoint)
			var spool *startupInput
			if mode == "capture" {
				if _, err := w.Close(); err != nil {
					t.Fatal(err)
				}
				spool, err = openStartupInput(path)
				if err != nil {
					t.Fatal(err)
				}
				h, err := r.Detach()
				if err != nil {
					t.Fatal(err)
				}
				opts.Listen = address
				r, err = tcp.NewInheritedCapture("tcp", opts, h, spool)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer r.Stop()
			var wg sync.WaitGroup
			var sent [16]int64
			start := time.Now()
			until := start.Add(time.Duration(seconds) * time.Second)
			for worker := range sent {
				conn, err := net.Dial("tcp", address)
				if err != nil {
					t.Fatal(err)
				}
				wg.Go(func() {
					defer conn.Close()
					_ = conn.SetWriteDeadline(until.Add(30 * time.Second))
					var data []byte
					for j := range 256 {
						data = fmt.Appendf(data, "servers.host%05d.application.component.requests.by_region.region%02d.by_status.success.total.%s %d 1700000000\n", worker*256+j, j%3, strings.Repeat("segment.", 15)+"segment", j)
					}
					for time.Now().Before(until) {
						if _, err := conn.Write(data); err != nil {
							t.Error(err)
							return
						}
						sent[worker] += 256
					}
				})
			}
			wg.Wait()
			var count int64
			for _, n := range sent {
				count += n
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				var got int64
				if spool != nil {
					got = spool.points.Load()
				} else {
					got = w.Points()
				}
				if got == count {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("received %d of %d", got, count)
				}
				time.Sleep(time.Millisecond)
			}
			duration := time.Since(start)
			r.Stop()
			if spool != nil {
				err = spool.close()
			} else {
				_, err = w.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			info, _ := os.Stat(path)
			t.Logf("points=%d rate=%.0f/s drained=%v durable=%v bytes=%d", count, float64(count)/duration.Seconds(), duration, time.Since(start), info.Size())
			// Verify the persisted record count, independent of the receiver's counter.
			var records int64
			if err := points.ReadFromFile(path, func(p *points.Points) { records += int64(len(p.Data)) }); err != nil || records != count {
				t.Fatal("disk records", records, count, err)
			}
		})
	}
}
