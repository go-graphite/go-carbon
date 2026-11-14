package carbon

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// Compare the old serial reader and ordered pipeline at the persistence boundary,
// including corrections repeated across separate historical epochs and live input.
func TestStartupHistoryPipelineStoredDifferential(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	const name = "pipeline.corrections"
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042}
	var epochs [3][]byte
	var want []points.Point
	for i := range 180 {
		p := points.OnePoint(name, math.Float64frombits(bits[i%len(bits)]), int64(1700000000+i%23))
		p.Add(math.Float64frombits(bits[(i+1)%len(bits)]), p.Data[0].Timestamp)
		want = append(want, p.Data...)
		epoch := i / 60
		epochs[epoch] = append(epochs[epoch], recovery.EncodeCaptureFrame(enc, p.AppendBinary(nil), nil, len(p.Data))...)
	}
	replay := func(pipeline bool) *cache.Cache {
		core := cache.New()
		core.SetMaxSize(0)
		sh := &startupShard{}
		sink := &startupSink{admission: core, add: core.AddRecoveredFrom}
		var stream cache.RecoveryStream
		for _, epoch := range epochs {
			source := stream.Next()
			apply := func(raw []byte, n int, end, cost int64) error {
				if !core.HasRecoveryCapacityBatch(n, cost) {
					t.Fatal("unexpected admission block")
				}
				return sh.apply(raw, source, sink)
			}
			var e error
			if pipeline {
				_, e = recovery.ReadCaptureFramesPipeline(bytes.NewReader(epoch), 4096, func(raw []byte, n int, stop <-chan struct{}) (int64, error) { return captureMetricBytes(raw, core) }, apply)
			} else {
				_, e = recovery.ReadCaptureFramesBuffer(bytes.NewReader(epoch), 4096, func(raw []byte, n int) error {
					cost, e := captureMetricBytes(raw, core)
					if e != nil {
						return e
					}
					return apply(raw, n, 0, cost)
				})
			}
			if e != nil {
				t.Fatal(e)
			}
		}
		return core
	}
	assertDirectStoreDifferential(t, replay(true), replay(false), name, want)
}

// Opt-in resident-cardinality workload with identical decode, admission pricing,
// and cache application on both paths. Fixture construction is not timed.
func BenchmarkStartupHistoryPipeline(b *testing.B) {
	const namesCount = 200000
	names := make([]string, namesCount)
	for i := range names {
		names[i] = fmt.Sprintf("service.%08d.%s", i, strings.Repeat("namespace.", 24))
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	rng := rand.New(rand.NewPCG(8144, 1308))
	var encoded []byte
	const frames = 32
	const perFrame = 4096
	for range frames {
		var raw []byte
		for range perFrame {
			p := points.OnePoint(names[rng.IntN(namesCount)], rng.Float64(), 1700000000)
			raw = p.AppendBinary(raw)
		}
		encoded = append(encoded, recovery.EncodeCaptureFrame(enc, raw, nil, perFrame)...)
	}
	for _, pipeline := range []bool{false, true} {
		b.Run(fmt.Sprint(pipeline), func(b *testing.B) {
			core := cache.New()
			core.SetMaxSize(0)
			var stream cache.RecoveryStream
			source := stream.Next()
			for _, name := range names {
				core.AddRecoveredFrom(points.OnePoint(name, 1, 1700000000), source)
			}
			sh := &startupShard{}
			sink := &startupSink{admission: core, add: core.AddRecoveredFrom}
			apply := func(raw []byte, n int, end, cost int64) error { return sh.apply(raw, source, sink) }
			prepare := func(raw []byte, n int, stop <-chan struct{}) (int64, error) { return captureMetricBytes(raw, core) }
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ResetTimer()
			for range b.N {
				var err error
				if pipeline {
					_, err = recovery.ReadCaptureFramesPipeline(bytes.NewReader(encoded), 256<<10, prepare, apply)
				} else {
					_, err = recovery.ReadCaptureFramesBuffer(bytes.NewReader(encoded), 256<<10, func(raw []byte, n int) error {
						cost, e := prepare(raw, n, nil)
						if e != nil {
							return e
						}
						return apply(raw, n, 0, cost)
					})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*frames*perFrame)/b.Elapsed().Seconds(), "points/s")
		})
	}
}
