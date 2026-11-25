package carbonserver

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/receiver/tcp"
)

// BenchmarkCompressedStreamAdmission runs the real TCP handler with shared
// dictionary names, repeated ids, production quota routes and cache bookkeeping.
// Initial definitions and warmup are excluded; an operation replays every name.
func BenchmarkCompressedStreamAdmission(b *testing.B) {
	for _, resident := range []int{4096, 65536, 262144} {
		b.Run(fmt.Sprint(resident), func(b *testing.B) {
			f := newIngestTCPFixture(b, resident)
			c := f.cache
			e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd, MaxNames: resident, MaxRows: 4096, DictionaryBytes: 128 << 20, PeerDictionaryBytes: 128 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()
			warm, err := codec.AppendPreamble(nil, codec.FlagZstd)
			if err != nil {
				b.Fatal(err)
			}
			encode := func(dst []byte) []byte {
				for i := range resident {
					if err := e.Add([]byte(ingestName(i)), 1, 1790000000); err != nil {
						b.Fatal(err)
					}
				}
				out, err := e.Flush(dst)
				if err != nil {
					b.Fatal(err)
				}
				return out
			}
			warm = encode(warm)
			steady := encode(nil)
			ingestTCPRegister.Do(tcp.Register)
			opts := tcp.NewOptions()
			opts.Listen = "127.0.0.1:0"
			opts.StreamDictionaryBytes = 128 << 20
			options, err := receiver.WithProtocol(opts, "tcp")
			if err != nil {
				b.Fatal(err)
			}
			r, err := receiver.New("stream-admission-bench", options, c.Add)
			if err != nil {
				b.Fatal(err)
			}
			rcv := r.(*tcp.TCP)
			defer rcv.Stop()
			rcv.SetStreamNameResolver(c.ResolveStreamName)
			rcv.SetStreamPointOut(c.AddStreamPoint)
			rcv.SetStreamPointsOut(c.AddStreamPoints)
			var cpu float64
			var sizeBefore int64
			var before, after runtime.MemStats
			conn := &quotaStreamConn{reader: *bytes.NewReader(warm), steady: steady, remaining: b.N, start: func() {
				runtime.GC()
				sizeBefore = c.Size()
				runtime.ReadMemStats(&before)
				b.ResetTimer()
				cpu = ingestCPU()
			}}
			b.ReportAllocs()
			rcv.HandleConnection(conn)
			cpu = ingestCPU() - cpu
			b.StopTimer()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N*resident), "B/point")
			b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(b.N*resident), "allocs/point")
			if got := c.Size() - sizeBefore; got != int64(b.N*resident) {
				b.Fatalf("accepted %d points, want %d", got, b.N*resident)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*resident), "ns/point")
			b.ReportMetric(cpu*1e9/float64(b.N*resident), "cpu-ns/point")
			if conn.remaining != 0 {
				b.Fatalf("unread repeats: %d", conn.remaining)
			}
			runtime.KeepAlive(c)
		})
	}
}

type quotaStreamConn struct {
	reader    bytes.Reader
	steady    []byte
	remaining int
	start     func()
}

func (c *quotaStreamConn) Read(p []byte) (int, error) {
	if c.reader.Len() == 0 {
		if c.start != nil {
			c.start()
			c.start = nil
		}
		if c.remaining == 0 {
			return 0, io.EOF
		}
		c.remaining--
		c.reader.Reset(c.steady)
	}
	return c.reader.Read(p)
}
func (*quotaStreamConn) Write([]byte) (int, error)        { panic("unused") }
func (*quotaStreamConn) Close() error                     { return nil }
func (*quotaStreamConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*quotaStreamConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*quotaStreamConn) SetDeadline(time.Time) error      { return nil }
func (*quotaStreamConn) SetReadDeadline(time.Time) error  { return nil }
func (*quotaStreamConn) SetWriteDeadline(time.Time) error { return nil }
