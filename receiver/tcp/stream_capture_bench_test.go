package tcp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
)

// Runs the real stream receiver after a 64-shard startup capture fence, against
// the same cache sink as ordinary input. Copy this benchmark to the baseline
// revision to measure allocations and CPU without modelling the old hot path.
func BenchmarkStreamAfterCaptureFence(b *testing.B) {
	const rows, names = 4096, 128
	encoder, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer encoder.Close()
	wire, _ := codec.AppendPreamble(nil, 0)
	for i := range rows {
		name := fmt.Sprintf("stream.capture.%03d.%s", i%names, strings.Repeat("x", 80))
		if err := encoder.Add([]byte(name), float64(i), int64(1700000000+i)); err != nil {
			b.Fatal(err)
		}
	}
	wire, err = encoder.Flush(wire)
	if err != nil {
		b.Fatal(err)
	}
	core := cache.New()
	core.SetMaxSize(0)
	shards := make(streamCaptureShards, 64)
	for i := range shards {
		shards[i] = captureFunc(func([]byte, int) error { b.Fatal("sealed capture used"); return nil })
	}
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor(""), capture: shards, out: core.Add}
	r.SetStreamPointOut(core.AddStreamPoint)
	r.SetStreamNameResolver(core.ResolveStreamName)
	r.Start()
	defer r.Stop()
	if err := r.SwitchCapture(core.Add, func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	conn := &benchmarkReadConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn.Reader.Reset(wire)
		r.HandleConnection(conn)
	}
	b.StopTimer()
	if core.Size() != int64(b.N*rows) {
		b.Fatalf("received %d points, want %d", core.Size(), b.N*rows)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/point")
}
