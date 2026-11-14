//go:build unix

package carbon

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
)

// Opt-in: the fixture remains private. Both paths decode the identical captured
// frames and apply every original point, with 200k other resident metric names.
func BenchmarkCaptureBatchReplay(b *testing.B) {
	path := os.Getenv("CAPTURE_BATCH_FIXTURE")
	if path == "" {
		b.Skip("set CAPTURE_BATCH_FIXTURE to an existing capture frame")
	}
	frame, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	defer dec.Close()
	raw, n, err := recovery.DecodeCaptureFrame(dec, frame, nil)
	if err != nil {
		b.Fatal(err)
	}
	original := make(map[string][]points.Point)
	if err := points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) {
		original[string(name)] = append(original[string(name)], data...)
	}); err != nil {
		b.Fatal(err)
	}
	const frames = 256
	encoded := bytes.Repeat(frame, frames)
	var writeBatch captureBatch
	if err := writeBatch.prepare(bytes.Repeat(raw, 32), &startupSink{admission: &captureWriteNames{}}); err != nil {
		b.Fatal(err)
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
	defer enc.Close()
	groupedFrame := recovery.EncodeCaptureFrame(enc, writeBatch.appendBinary(nil), nil, n*32)
	groupedEncoded := bytes.Repeat(groupedFrame, frames/32)
	cpu := func() float64 {
		var r syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	for _, mode := range []string{"original", "grouped-replay", "grouped-capture"} {
		b.Run(mode, func(b *testing.B) {
			core := cache.New()
			core.SetMaxSize(0)
			core.SetRecoveryMemory(func() (uint64, bool) { return 128 << 30, true })
			for i := range 200000 {
				core.AddRecovered(points.OnePoint(fmt.Sprintf("resident.namespace.%09d.service.requests", i), 1, 1700000000))
			}
			sink := &startupSink{admission: core, add: core.AddRecoveredFrom, groupedAdd: core.AddRecoveryMetric}
			s := batchReplayOwner()
			sh := &startupShard{}
			var stream cache.RecoveryStream
			b.ReportAllocs()
			b.ResetTimer()
			started := cpu()
			for range b.N {
				source := stream.Next()
				var err error
				if mode != "original" {
					wire := encoded
					if mode == "grouped-capture" {
						wire = groupedEncoded
					}
					_, err = s.replayCaptureBatches(bytes.NewReader(wire), source, sink, func(int64, int) {})
				} else {
					_, err = recovery.ReadCaptureFramesPipeline(bytes.NewReader(encoded), 4<<20, func(raw []byte, _ int, _ <-chan struct{}) (int64, error) { return captureMetricBytes(raw, core) }, func(raw []byte, n int, _, cost int64) error {
						if !core.HasRecoveryCapacityBatch(n, cost) {
							b.Fatal("admission")
						}
						return sh.apply(raw, source, sink)
					})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			used := cpu() - started
			b.StopTimer()
			total := float64(b.N * n * frames)
			b.ReportMetric(used*1e6/total, "cpu-s/Mpt")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/pt")
			b.ReportMetric(float64(n*frames), "points/op")
			if core.Size() != int64(200000+b.N*n*frames) {
				b.Fatal("replay point count", core.Size())
			}
			for name, want := range original {
				got := core.Get(name)
				if len(got) != len(want)*frames*b.N {
					b.Fatal("real fixture missing/duplicate points")
				}
				for i, p := range got {
					w := want[i%len(want)]
					if p.Timestamp != w.Timestamp || math.Float64bits(p.Value) != math.Float64bits(w.Value) {
						b.Fatal("real fixture point order/bits changed")
					}
				}
			}
		})
	}
}
