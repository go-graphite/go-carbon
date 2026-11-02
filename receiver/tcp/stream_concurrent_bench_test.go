//go:build unix

package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

func streamBenchCPU(b testing.TB) int64 {
	b.Helper()
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		b.Fatal(err)
	}
	return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + (int64(u.Utime.Usec)+int64(u.Stime.Usec))*1000
}

// BenchmarkStream40Connections keeps wire ids private while four independent
// connections per partition feed the same cache. Warmup, fixtures and GC are
// excluded. The immutable steady frame is identical on old/new codec builds.
func BenchmarkStream40Connections(b *testing.B) {
	const count = 4096
	var definitions [10][]byte
	c := cache.New()
	c.SetMaxSize(0)
	for partition := range 10 {
		e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd, MaxRows: count})
		if err != nil {
			b.Fatal(err)
		}
		for i := range count {
			name := fmt.Appendf(nil, "example.application.%d.%snode.%06d.requests", partition, strings.Repeat("component.", 17+i%5), i)
			if err := e.Add(name, float64(i%29), 1700000000); err != nil {
				b.Fatal(err)
			}
			c.AddPoint(name, 0, 0)
		}
		definitions[partition], err = e.Flush(nil)
		e.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
	raw := binary.AppendUvarint(nil, count)
	raw = binary.AppendUvarint(raw, 0)
	raw = binary.AppendUvarint(raw, count)
	raw = binary.AppendUvarint(raw, count)
	raw = append(raw, 0)
	for range count - 1 {
		raw = append(raw, 1)
	}
	for range count {
		raw = binary.AppendVarint(raw, 60)
	}
	raw = append(raw, make([]byte, count)...)
	z, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	if err != nil {
		b.Fatal(err)
	}
	wire := make([]byte, codec.HeaderSize)
	wire = z.EncodeAll(raw, wire)
	_ = z.Close()
	binary.BigEndian.PutUint32(wire[4:8], uint32(len(wire)-codec.HeaderSize))
	binary.BigEndian.PutUint32(wire[8:12], uint32(len(raw)))
	binary.BigEndian.PutUint32(wire[12:16], count)
	binary.BigEndian.PutUint32(wire[16:20], crc32.Checksum(wire[codec.HeaderSize:], crc32.MakeTable(crc32.Castagnoli)))
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	cacheBefore := c.EstimatedBytes()
	var decoders [40]*codec.Decoder
	visit := func(_ uint32, name []byte, value float64, ts int64, slot *any) {
		c.AddStreamPoint(name, value, ts, slot)
	}
	shared := os.Getenv("STREAM_RECEIVER_SHARED") != "0"
	for i := range decoders {
		d, err := codec.NewDecoder(codec.Options{Flags: codec.FlagZstd})
		if err != nil {
			b.Fatal(err)
		}
		decoders[i] = d
		if shared {
			if target, ok := any(d).(interface{ SetNameResolver(func([]byte) any) }); ok {
				if names, ok := any(c).(interface{ ResolveStreamName([]byte) any }); ok {
					target.SetNameResolver(names.ResolveStreamName)
				}
			}
		}
		r := bytes.NewReader(definitions[i%10])
		for r.Len() > 0 {
			if _, err := d.ReadFrame(r, visit); err != nil {
				b.Fatal(err)
			}
		}
	}
	runtime.GC()
	var warm runtime.MemStats
	runtime.ReadMemStats(&warm)
	retained := int64(warm.HeapAlloc) - int64(before.HeapAlloc) - (c.EstimatedBytes() - cacheBefore)
	b.ReportAllocs()
	b.ResetTimer()
	cpu := streamBenchCPU(b)
	var wg sync.WaitGroup
	for worker := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := decoders[worker]
			r := bytes.NewReader(wire)
			for n := worker; n < b.N; n += 40 {
				r.Reset(wire)
				if _, err := d.ReadFrame(r, visit); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	cpu = streamBenchCPU(b) - cpu
	b.StopTimer()
	b.ReportMetric(float64(cpu)/float64(b.N*count), "cpu-ns/point")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/point")
	b.ReportMetric(float64(retained)/float64(10*count), "retained-B/name")
	b.ReportMetric(float64(retained)/float64(40*count), "retained-B/wire-id")
	for _, d := range decoders {
		d.Close()
	}
	runtime.KeepAlive(c)
	runtime.KeepAlive(definitions)
}
