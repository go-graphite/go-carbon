package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func TestStreamBatchReceiverDifferential(t *testing.T) {
	for _, flags := range []byte{0, codec.FlagZstd} {
		e, err := codec.NewEncoder(codec.Options{Flags: flags, MaxNames: 2, MaxRows: 257})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := codec.AppendPreamble(nil, flags)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 900 {
			name := []byte("first")
			if i/137%3 == 1 {
				name = []byte("second")
			} else if i/137%3 == 2 {
				name = []byte("third")
			}
			v := math.Float64frombits(uint64(i) * 0x7123456789abcdef)
			if err = e.Add(name, v, int64(i%7)-3); err != nil {
				t.Fatal(err)
			}
		}
		wire, err = e.Flush(wire)
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, truncate := range []int{0, 1, 17} {
			input := wire[:len(wire)-truncate]
			var transcript [2][]byte
			var runs []int
			for k := range 2 {
				emit := func(name []byte, v float64, ts int64, slot *any) {
					if *slot == nil {
						*slot = string(name)
					} else if (*slot).(string) != string(name) {
						t.Fatal("dictionary slot crossed reset")
					}
					transcript[k] = binary.AppendUvarint(transcript[k], uint64(len(name)))
					transcript[k] = append(transcript[k], name...)
					transcript[k] = binary.LittleEndian.AppendUint64(transcript[k], math.Float64bits(v))
					transcript[k] = binary.LittleEndian.AppendUint64(transcript[k], uint64(ts))
				}
				_, _, _ = runStream(t, input, func(r *TCP) {
					r.SetStreamPointOut(emit)
					if k == 1 {
						r.SetStreamPointsOut(func(name []byte, data []points.Point, slot *any) {
							if len(data) < 2 || len(data) > 64 {
								t.Fatal("unbounded run")
							}
							runs = append(runs, len(data))
							for _, p := range data {
								emit(name, p.Value, p.Timestamp, slot)
							}
						})
					}
				})
			}
			if !bytes.Equal(transcript[0], transcript[1]) {
				t.Fatalf("flags=%d truncate=%d receiver transcript differs", flags, truncate)
			}
			if len(runs) == 0 {
				t.Fatal("batch callback unused")
			}
		}
	}
}

func BenchmarkStreamBatchConnection(b *testing.B) {
	const rows = 4096
	for _, run := range []int{1, 2, 8, 64} {
		e, err := codec.NewEncoder(codec.Options{MaxRows: rows})
		if err != nil {
			b.Fatal(err)
		}
		wire, err := codec.AppendPreamble(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		for i := range rows {
			name := fmt.Appendf(nil, "benchmark.series.%d", (i/run)%64)
			if err = e.Add(name, float64(i%17), int64(i)); err != nil {
				b.Fatal(err)
			}
		}
		wire, err = e.Flush(wire)
		e.Close()
		if err != nil {
			b.Fatal(err)
		}
		for _, batch := range []bool{false, true} {
			b.Run(fmt.Sprintf("run%d/batch%v", run, batch), func(b *testing.B) {
				c := cache.New()
				c.SetMaxSize(0)
				r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
				r.Start()
				defer r.Stop()
				r.SetStreamPointOut(c.AddStreamPoint)
				r.SetStreamNameResolver(c.ResolveStreamName)
				if batch {
					r.SetStreamPointsOut(c.AddStreamPoints)
				}
				conn := &benchmarkReadConn{}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					conn.Reader.Reset(wire)
					r.HandleConnection(conn)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/point")
			})
		}
	}
}

func TestStreamBatchCaptureFence(t *testing.T) {
	var captured atomic.Int64
	got := make(chan points.Point, 400)
	emit := func(p *points.Points) {
		for _, point := range p.Data {
			got <- point
		}
	}
	writer := captureFunc(func(data []byte, n int) error {
		captured.Add(int64(n))
		return points.ReadBinary(bytes.NewReader(data), emit)
	})
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor(""), capture: writer, out: emit}
	r.Start()
	defer r.Stop()
	r.SetStreamPointOut(func(name []byte, v float64, ts int64, _ *any) { emit(points.OnePoint(string(name), v, ts)) })
	r.SetStreamPointsOut(func(name []byte, data []points.Point, _ *any) { emit(&points.Points{Metric: string(name), Data: data}) })
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() { r.HandleConnection(server); close(done) }()
	e, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	wire, err := codec.AppendPreamble(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	write := func(first, n int) {
		t.Helper()
		for i := first; i < first+n; i++ {
			if err = e.Add([]byte("same.series"), math.Float64frombits(uint64(i)|1<<63), int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		wire, err = e.Flush(wire)
		if err != nil {
			t.Fatal(err)
		}
		_ = client.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err = client.Write(wire); err != nil {
			t.Fatal(err)
		}
		wire = wire[:0]
	}
	check := func(i int) {
		t.Helper()
		select {
		case p := <-got:
			if p.Timestamp != int64(i) || math.Float64bits(p.Value) != uint64(i)|1<<63 {
				t.Fatal("fence order/bits", i, p)
			}
		case <-time.After(time.Second):
			t.Fatal("fence lost point", i)
		}
	}
	write(0, 1)
	check(0)
	if captured.Load() != 1 {
		t.Fatal("batch bypassed capture")
	}
	if err = r.SwitchCapture(emit, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	write(1, 300)
	_ = client.Close()
	for i := 1; i <= 300; i++ {
		check(i)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("EOF failed to flush run")
	}
	if captured.Load() != 1 {
		t.Fatal("direct suffix re-entered capture")
	}
}
