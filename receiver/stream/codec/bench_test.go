package codec

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// Default: one million distinct 150–300-byte dotted names, sampled once per
// second. STREAM_BENCH_NAMES=5000000 exercises the upper dictionary size. The
// first epoch defines names; measurements below use the warmed connection.
func syntheticNames(n int) [][]byte {
	names := make([][]byte, n)
	for i := range names {
		base := fmt.Sprintf("example.application.region.zone.service.worker.%06d.instance.%08d.measurement.", i/1024, i)
		size := 150 + i%151
		name := make([]byte, 0, size)
		for len(name)+len(base) < size {
			name = append(name, base...)
		}
		name = append(name, base[:size-len(name)]...)
		// Keep uniqueness even where the tail is truncated.
		names[i] = name
	}
	return names
}
func syntheticValue(id, epoch int) float64 {
	switch id % 4 {
	case 0:
		return float64(id + epoch*100)
	case 1:
		return float64((id+epoch)%10000) / 100
	case 2:
		return float64(id % 100) // stable gauges
	default:
		// Deterministic full-mantissa random floats; no PRNG work in the benchmark.
		x := uint64(id) + uint64(epoch)*0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		x ^= x >> 31
		return math.Float64frombits(x&((1<<52)-1) | 0x3ff0000000000000)
	}
}

func BenchmarkStream(b *testing.B) {
	n := 1_000_000
	if s := os.Getenv("STREAM_BENCH_NAMES"); s != "" {
		var err error
		n, err = strconv.Atoi(s)
		if err != nil || n < 1 {
			b.Fatal("invalid STREAM_BENCH_NAMES")
		}
	}
	names := syntheticNames(n)
	for _, flags := range []byte{0, FlagZstd} {
		label := "raw"
		if flags != 0 {
			label = "zstd"
		}
		b.Run(label, func(b *testing.B) {
			for _, mode := range []string{"encode", "decode"} {
				b.Run(mode, func(b *testing.B) {
					o := Options{Flags: flags, MaxNames: max(n, DefaultMaxNames)}
					e, d := encoder(b, o), decoder(b, o)
					var dst []byte
					var coldBytes int64
					// Warm all encoder and decoder names without retaining a plaintext epoch.
					for i, name := range names {
						if err := e.Add(name, syntheticValue(i, 0), 1700000000); err != nil {
							b.Fatal(err)
						}
						if e.Ready() {
							dst = flush(b, e, dst[:0])
							coldBytes += int64(len(dst))
							decodeAll(b, d, dst, nil)
						}
					}
					dst = flush(b, e, dst[:0])
					coldBytes += int64(len(dst))
					decodeAll(b, d, dst, nil)
					var steady []byte
					for i, name := range names {
						if err := e.Add(name, syntheticValue(i, 1), 1700000001); err != nil {
							b.Fatal(err)
						}
						if e.Ready() {
							steady = flush(b, e, steady)
						}
					}
					steady = flush(b, e, steady)
					decodeAll(b, d, steady, nil)
					frames := splitFrames(b, steady)
					var wireBytes int64
					b.ReportAllocs()
					b.ResetTimer()
					for epoch := range b.N {
						if mode == "encode" {
							for i, name := range names {
								if err := e.Add(name, syntheticValue(i, epoch+2), 1700000002+int64(epoch)); err != nil {
									b.Fatal(err)
								}
								if e.Ready() {
									dst = flush(b, e, dst[:0])
									wireBytes += int64(len(dst))
								}
							}
							dst = flush(b, e, dst[:0])
							wireBytes += int64(len(dst))
						} else {
							for _, frame := range frames {
								if _, err := d.Decode(frame, benchmarkVisit); err != nil {
									b.Fatal(err)
								}
							}
							wireBytes += int64(len(steady))
						}
					}
					b.StopTimer()
					count := float64(b.N) * float64(n)
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/count, "ns/point")
					b.ReportMetric(float64(wireBytes)/count, "wire-B/point")
					b.ReportMetric(float64(coldBytes)/float64(n), "cold-B/point")
				})
			}
		})
	}
}
func benchmarkVisit(_ uint32, _ []byte, _ float64, _ int64, _ *any) {}
func splitFrames(b *testing.B, wire []byte) [][]byte {
	b.Helper()
	var frames [][]byte
	d := decoder(b, Options{})
	r := bytes.NewReader(wire)
	for r.Len() > 0 {
		off := len(wire) - r.Len()
		h := make([]byte, HeaderSize)
		if _, err := io.ReadFull(r, h); err != nil {
			b.Fatal(err)
		}
		info, err := d.header(h)
		// The header length is independent of compression mode.
		if err != nil && info.PayloadBytes == 0 {
			b.Fatal(err)
		}
		size := HeaderSize + int(info.PayloadBytes)
		if size > len(wire)-off {
			b.Fatal("truncated benchmark frame")
		}
		frames = append(frames, wire[off:off+size])
		_, _ = r.Seek(int64(info.PayloadBytes), io.SeekCurrent)
	}
	return frames
}

// BenchmarkPlaintextZstd compares independent whole-epoch zstd compression of
// the exact same synthetic plaintext (including names on every point).
func BenchmarkPlaintextZstd(b *testing.B) {
	n := 1_000_000
	if s := os.Getenv("STREAM_BENCH_NAMES"); s != "" {
		var err error
		n, err = strconv.Atoi(s)
		if err != nil || n < 1 {
			b.Fatal("invalid STREAM_BENCH_NAMES")
		}
	}
	names := syntheticNames(n)
	var plain []byte
	for i, name := range names {
		plain = append(plain, name...)
		plain = append(plain, ' ')
		plain = strconv.AppendFloat(plain, syntheticValue(i, 1), 'g', -1, 64)
		plain = append(plain, " 1700000001\n"...)
	}
	for _, level := range []int{1, 3} {
		b.Run(fmt.Sprintf("level%d", level), func(b *testing.B) {
			z, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = z.Close() }()
			dst := z.EncodeAll(plain, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				dst = z.EncodeAll(plain, dst[:0])
			}
			b.StopTimer()
			b.ReportMetric(float64(len(plain))/float64(n), "plain-B/point")
			b.ReportMetric(float64(len(dst))/float64(n), "wire-B/point")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/point")
		})
	}
}
