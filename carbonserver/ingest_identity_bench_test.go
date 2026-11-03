//go:build unix

package carbonserver

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/zeebo/xxh3"
)

// BenchmarkIdentityStreamIngest exercises the production decoder -> bound
// StreamSeries -> cache -> real quota route. Forty connections share ten
// partitions, with long names and 100 buffered points/series (the supplied
// snapshots have 20-209). Definitions, binding and warmup are outside timing.
// Each op is a validated 4096-point frame. No sender, socket or disk I/O is
// included; cpu-ns/point uses process CPU to tolerate a shared benchmark host.
func BenchmarkIdentityStreamIngest(b *testing.B) {
	const rows, partitions, connections = 4096, 10, 40
	ti := newTrie(".wsp", 0, nil)
	c := cache.New()
	c.SetMaxSize(0)
	c.SetWriteoutBatching(960, 960*time.Second)
	c.SetWriteoutSpread(.25)
	var definitions [partitions][]byte
	var wire []byte
	for partition := range partitions {
		e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd, MaxRows: rows})
		if err != nil {
			b.Fatal(err)
		}
		names := make([]string, rows)
		for i := range names {
			name := fmt.Sprintf("services.namespace-%02d.%snode-%06d.requests", partition, strings.Repeat("component.", 14+i%9), i)
			names[i] = name
			if _, err := ti.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
				b.Fatal(err)
			}
			warm := make([]points.Point, 100)
			for j := range warm {
				warm[j] = points.Point{Value: float64(i%29) + float64(j%7)/10, Timestamp: 1700000000 + int64(j)*60}
			}
			c.Add(&points.Points{Metric: name, Data: warm})
			if err := e.Add([]byte(name), float64(i%29), 1700006000); err != nil {
				b.Fatal(err)
			}
		}
		definitions[partition], err = e.Flush(nil)
		if err != nil {
			b.Fatal(err)
		}
		for i, name := range names {
			value := float64(i%29) + 1
			switch i % 3 {
			case 0:
				value = float64((uint64(i)*48271)%1000003) / 1000
			case 1:
				value += .1
			}
			if err := e.Add([]byte(name), value, 1700006060); err != nil {
				b.Fatal(err)
			}
		}
		frame, err := e.Flush(nil)
		e.Close()
		if err != nil {
			b.Fatal(err)
		}
		if partition == 0 {
			wire = frame
		} else if !bytes.Equal(wire, frame) {
			b.Fatal("steady frames differ across partitions")
		}
	}
	if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "/", Throughput: 159_000_000}, &Quota{Pattern: "services.*", Throughput: 50_000_000}); err != nil {
		b.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	l.UpdateFileIndex(&fileIndex{trieIdx: ti})
	c.SetThrottleWithHash(l.ShouldThrottleMetricWithHash)
	var decoders [connections]*codec.Decoder
	visit := func(_ uint32, name []byte, value float64, ts int64, slot *any) {
		c.AddStreamPoint(name, value, ts, slot)
	}
	for i := range decoders {
		d, err := codec.NewDecoder(codec.Options{Flags: codec.FlagZstd})
		if err != nil {
			b.Fatal(err)
		}
		decoders[i] = d
		d.SetNameResolver(c.ResolveStreamName)
		if _, err := d.ReadFrame(bytes.NewReader(definitions[i%partitions]), visit); err != nil {
			b.Fatal(err)
		}
	}
	runtime.GC()
	latencies := make([]int64, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	cpu := ingestCPU()
	var wg sync.WaitGroup
	for worker := range connections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := bytes.NewReader(wire)
			for i := worker; i < b.N; i += connections {
				r.Reset(wire)
				start := time.Now()
				if _, err := decoders[worker].ReadFrame(r, visit); err != nil {
					b.Error(err)
					return
				}
				latencies[i] = time.Since(start).Nanoseconds()
			}
		}()
	}
	wg.Wait()
	cpu = ingestCPU() - cpu
	b.StopTimer()
	if got := ti.throughputs.load("/").dpRecorder().total(); got != int64((b.N+connections)*rows) {
		b.Fatalf("quota accounting: got %d points", got)
	}
	if got := c.Size(); got != int64(partitions*rows*100+(b.N+connections)*rows) {
		b.Fatalf("cache admission: got %d points", got)
	}
	slices.Sort(latencies)
	b.ReportMetric(cpu*1e9/float64(b.N*rows), "cpu-ns/point")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/point")
	b.ReportMetric(float64(latencies[len(latencies)/2])/1e6, "frame-p50-ms")
	b.ReportMetric(float64(latencies[min(len(latencies)-1, len(latencies)*99/100)])/1e6, "frame-p99-ms")
	b.ReportMetric(float64(len(wire))/rows, "wire-B/point")
	for _, d := range decoders {
		d.Close()
	}
}

// Isolate the accounting handoff from scheduling and compact-buffer growth.
func BenchmarkIdentityQuotaHash(b *testing.B) {
	for _, resolved := range []bool{false, true} {
		b.Run(fmt.Sprintf("resolved=%t", resolved), func(b *testing.B) {
			ti := newTrie(".wsp", 0, nil)
			if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "/", Throughput: math.MaxInt64}); err != nil {
				b.Fatal(err)
			}
			l := NewCarbonserverListener(nil)
			l.UpdateFileIndex(&fileIndex{trieIdx: ti})
			const n = 16384
			names, hashes, states := make([]string, n), make([]uint64, n), make([]any, n)
			ps := points.OnePoint("", 1, 1)
			for i := range names {
				names[i] = fmt.Sprintf("services.namespace.%snode-%06d.requests", strings.Repeat("component.", 14+i%9), i)
				hashes[i] = xxh3.HashString(names[i])
				ps.Metric = names[i]
				l.ShouldThrottleMetricWithHash(ps, true, hashes[i], &states[i])
			}
			b.ReportAllocs()
			b.ResetTimer()
			cpu := ingestCPU()
			for i := range b.N {
				j := i % n
				ps.Metric = names[j]
				if resolved {
					l.ShouldThrottleMetricWithHash(ps, true, hashes[j], &states[j])
				} else {
					l.ShouldThrottleMetricWithState(ps, true, &states[j])
				}
			}
			cpu = ingestCPU() - cpu
			b.StopTimer()
			b.ReportMetric(cpu*1e9/float64(b.N), "cpu-ns/point")
		})
	}
}
