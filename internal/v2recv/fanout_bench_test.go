package v2recv

import (
	"context"
	"fmt"
	"hash/maphash"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// BenchmarkReceiverFanoutPaced offers 3M points/s over 64 connections to 96
// owners for 5 s, the store bench load, and reports process CPU per point
// (writers included). Owners idle between batches, so hand-off wakeups count.
// V2_FANOUT_BUFFER sets BufferBytes (default 1 MiB). Run with -benchtime=1x.
func BenchmarkReceiverFanoutPaced(b *testing.B) {
	const owners, conns, names, rate, seconds = 96, 64, 20000, 3000000, 5
	buffer, _ := strconv.Atoi(os.Getenv("V2_FANOUT_BUFFER"))
	lines := make([][]byte, names)
	for i := range lines {
		lines[i] = fmt.Appendf(nil, "fanout.%s.%06d 1.25 1700000000\n", strings.Repeat("n", 240), i)
	}
	list := make([]Owner, owners)
	for i := range list {
		list[i] = &bytesBenchOwner{seed: maphash.MakeSeed()}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	s, err := Start(l, list, Config{BufferBytes: buffer, SyncInterval: time.Second, MaxConnections: conns})
	if err != nil {
		b.Fatal(err)
	}
	var r0 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r0)
	b.ResetTimer()
	var wg sync.WaitGroup
	perTick := rate / conns / 100 // lines per connection per 10 ms
	cut := os.Getenv("V2_FANOUT_CUT") == "1"
	for c := range conns {
		conn, e := net.Dial("tcp", l.Addr().String())
		if e != nil {
			b.Fatal(e)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			next, chunk := c*313, []byte(nil)
			for tick := range seconds * 100 {
				for range perTick {
					chunk = append(chunk, lines[next%names]...)
					next++
				}
				// V2_FANOUT_CUT=1 ends writes mid-line, like a continuous relay stream.
				n := len(chunk)
				if cut && tick < seconds*100-1 {
					n -= len(lines[0]) / 2
				}
				if _, e := conn.Write(chunk[:n]); e != nil {
					b.Error(e)
					return
				}
				chunk = append(chunk[:0], chunk[n:]...)
				<-ticker.C
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err = s.Wait(ctx); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	var r1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r1)
	total := float64(conns * perTick * seconds * 100)
	if s.Stats().Accepted != uint64(total) {
		b.Fatal(s.Stats())
	}
	cpu := time.Duration(r1.Utime.Nano() + r1.Stime.Nano() - r0.Utime.Nano() - r0.Stime.Nano())
	b.ReportMetric(float64(cpu.Nanoseconds())/total, "cpu-ns/point")
}
