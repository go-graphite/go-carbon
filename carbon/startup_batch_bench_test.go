//go:build unix

package carbon

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery/capturetest"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
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
	groupedFrame := capturetest.Encode(enc, writeBatch.appendBinary(nil), nil, n*32)
	groupedEncoded := bytes.Repeat(groupedFrame, frames/32)
	cpu := func() float64 {
		var r syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	framesRaw := make([][]byte, frames/32)
	for i := range framesRaw {
		framesRaw[i] = bytes.Repeat(raw, 32)
	}
	inputWire := inputlogCaptureFixture(b, framesRaw...)
	for _, mode := range []string{"original", "grouped-replay", "grouped-capture", "inputlog"} {
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
			file, e := os.CreateTemp(b.TempDir(), "capture")
			if e != nil {
				b.Fatal(e)
			}
			defer file.Close()
			if _, e = file.Write(inputWire); e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			started := cpu()
			for range b.N {
				source := stream.Next()
				var err error
				switch {
				case mode == "inputlog":
					seg := &startupSegment{file: file, source: source}
					seg.size.Store(int64(len(inputWire)))
					_, err = s.replayInputSegment(seg, sink, 0, func(int64, int) {})
				case mode != "original":
					wire := encoded
					if mode == "grouped-capture" {
						wire = groupedEncoded
					}
					_, err = s.replayCaptureBatches(bytes.NewReader(wire), source, sink, func(int64, int) {})
				default:
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
			size := len(encoded)
			if mode == "grouped-capture" {
				size = len(groupedEncoded)
			}
			if mode == "inputlog" {
				size = len(inputWire)
			}
			b.ReportMetric(float64(size)/float64(n*frames), "encoded-B/pt")
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

func BenchmarkCaptureInputlogWrite(b *testing.B) {
	path := os.Getenv("CAPTURE_BATCH_FIXTURE")
	if path == "" {
		b.Skip("set CAPTURE_BATCH_FIXTURE")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	defer dec.Close()
	raw, n, err := recovery.DecodeCaptureFrame(dec, wire, nil)
	if err != nil {
		b.Fatal(err)
	}
	cpu := func() float64 {
		var r syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	for _, mode := range []string{"legacy-write", "legacy-sync", "inputlog"} {
		b.Run(mode, func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "capture")
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			writer, err := inputlog.NewSegmentWriter(f, 1)
			if err != nil {
				b.Fatal(err)
			}
			defer writer.Close()
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
			defer enc.Close()
			var batch captureBatch
			var records []*points.Points
			var frame []byte
			sink := &startupSink{admission: &captureWriteNames{}}
			var size int64
			b.ReportAllocs()
			b.ResetTimer()
			started := cpu()
			for range b.N {
				if err := batch.prepare(raw, sink); err != nil {
					b.Fatal(err)
				}
				if mode == "inputlog" {
					records = records[:0]
					for i := range batch.used {
						g := &batch.groups[i]
						g.point.Metric, g.point.Data = g.name, g.data
						records = append(records, &g.point)
					}
					count, err := writer.Append(records, nil)
					if err != nil {
						b.Fatal(err)
					}
					size += count
				} else {
					frame = capturetest.Encode(enc, raw, frame, n)
					count, err := f.Write(frame)
					if err != nil {
						b.Fatal(err)
					}
					size += int64(count)
					if mode == "legacy-sync" {
						if err := f.Sync(); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			used := cpu() - started
			b.StopTimer()
			total := float64(b.N * n)
			b.ReportMetric(used*1e6/total, "cpu-s/Mpt")
			b.ReportMetric(float64(size)/total, "encoded-B/pt")
		})
	}
}
