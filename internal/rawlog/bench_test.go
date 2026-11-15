package rawlog_test

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/rawlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func benchWire(b testing.TB) ([][]byte, int) {
	b.Helper()
	const n = 8929
	e, err := codec.NewEncoder(codec.Options{})
	must(b, err)
	defer e.Close()
	must(b, e.Reset())
	bits := []uint64{0, 1 << 63, 1, 0x3ff0000000000001, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 0x4000000000000001}
	for i := range n {
		must(b, e.Add([]byte(fmt.Sprintf("load.c000.m%04d;a=first;z=last", i%256)), math.Float64frombits(bits[i%8]), 1_800_000_000+int64(i/256%8)))
	}
	wire, err := e.Flush(nil)
	must(b, err)
	r, err := codec.NewWireReader(codec.Options{})
	must(b, err)
	rd := bytes.NewReader(wire)
	var frames [][]byte
	for rd.Len() > 0 {
		f, _, err := r.Read(rd)
		must(b, err)
		frames = append(frames, bytes.Clone(f))
	}
	return frames, n
}

// These are isolated capture CPU components, not fsync or end-to-end gates.
func BenchmarkCaptureComponents(b *testing.B) {
	frames, n := benchWire(b)
	b.Run("raw-envelope", func(b *testing.B) {
		r, err := codec.NewWireReader(codec.Options{})
		must(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, f := range frames {
				_, err = r.Validate(f)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
	})
	b.Run("current-decode-legacy-frame", func(b *testing.B) {
		d, err := codec.NewDecoder(codec.Options{})
		must(b, err)
		defer d.Close()
		var legacy []byte
		var fd points.FrameDecoder
		var count int
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			legacy = legacy[:0]
			for _, f := range frames {
				_, err = d.Decode(f, func(_ uint32, name []byte, value float64, ts int64, _ *any) {
					p := points.Points{Metric: string(name), Data: []points.Point{{Value: value, Timestamp: ts}}}
					legacy = p.AppendBinary(legacy)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			must(b, fd.Read(legacy, func(_ []byte, data []points.Point) { count += len(data) }))
		}
		if count != b.N*n {
			b.Fatal(count)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
	})
}

// Append consumes the identical decoded frame at today's durable journal API.
// Its fsync-per-Append is explicitly different from raw group commit.
func BenchmarkDurableCapture(b *testing.B) {
	frames, n := benchWire(b)
	b.Run("raw-group32", func(b *testing.B) {
		w, err := rawlog.Create(b.TempDir())
		must(b, err)
		defer w.Close()
		pre, _ := codec.AppendPreamble(nil, 0)
		_, err = w.Begin(0, pre)
		must(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			for _, f := range frames {
				_, err = w.Stage(0, f, 1)
				must(b, err)
			}
			if i%32 == 31 {
				_, err = w.Sync()
				must(b, err)
			}
		}
		_, err = w.Sync()
		must(b, err)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
	})
	b.Run("current-journal", func(b *testing.B) {
		f, err := os.CreateTemp(b.TempDir(), "journal")
		must(b, err)
		defer f.Close()
		w, err := inputlog.NewSegmentWriter(f, 1)
		must(b, err)
		defer w.Close()
		d, err := codec.NewDecoder(codec.Options{})
		must(b, err)
		defer d.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var legacy []byte
			for _, frame := range frames {
				_, err = d.Decode(frame, func(_ uint32, name []byte, v float64, ts int64, _ *any) {
					p := points.Points{Metric: string(name), Data: []points.Point{{Value: v, Timestamp: ts}}}
					legacy = p.AppendBinary(legacy)
				})
				must(b, err)
			}
			var fd points.FrameDecoder
			var records []*points.Points
			must(b, fd.Read(legacy, func(name []byte, data []points.Point) {
				records = append(records, &points.Points{Metric: string(name), Data: append([]points.Point(nil), data...)})
			}))
			_, err = w.Append(records, nil)
			must(b, err)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
		info, err := f.Stat()
		must(b, err)
		b.ReportMetric(float64(info.Size())/float64(b.N*n), "disk-B/point")
	})
}
