package chunkstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func sealedReuseFixture(n int, kind string) []Point {
	rng := rand.New(rand.NewSource(73))
	points := make([]Point, n)
	for i := range points {
		v := float64(i*123 + i/97)
		switch kind {
		case "decimal":
			v = float64(rng.Intn(100000)) / 100
		case "random":
			v = math.Float64frombits(0x3ff0000000000000 | rng.Uint64()>>12)
		case "special":
			v = math.Float64frombits([]uint64{0x8000000000000000, 0x7ff0000000000000, 0x7ff8000000000123, 1}[i%4])
		}
		points[i] = Point{Timestamp: 1700000040 + int64(i*60), Value: v}
		if kind == "gaps" {
			points[i].Timestamp += int64(i / 3 * 60)
		}
	}
	return points
}

func BenchmarkSealedDecodeOwned(b *testing.B) {
	for _, n := range []int{128, 4096} {
		for _, kind := range []string{"counter", "decimal", "random"} {
			b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
				data, err := EncodeSealedPoints(sealedReuseFixture(n, kind), 60)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				cpu := sealedCPU()
				b.ResetTimer()
				for range b.N {
					if _, err := DecodeSealedPoints(data); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(sealedCPU()-cpu)/float64(b.N), "cpu-ns/op")
				b.ReportMetric(float64(len(data)), "encoded-B/block")
			})
		}
	}
}

func TestSealedPointBufferReuse(t *testing.T) {
	var scratch SealedPointBuffer
	for _, n := range []int{1, 127, 4096, MaxSealedPoints, 32} {
		for _, kind := range []string{"counter", "decimal", "random", "special", "gaps"} {
			want := sealedReuseFixture(n, kind)
			data, err := EncodeSealedPoints(want, 60)
			if err != nil {
				t.Fatal(err)
			}
			got, err := scratch.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range want {
				if got[i].Timestamp != p.Timestamp || math.Float64bits(got[i].Value) != math.Float64bits(p.Value) {
					t.Fatalf("%s/%d point %d differs", kind, n, i)
				}
			}
			if cap(scratch.points) > MaxSealedPoints || cap(scratch.body) > maxSealedBody {
				t.Fatal("scratch exceeds bounds")
			}
			// Every truncation and sampled corruptions must fail closed after reuse.
			for i := 0; i < len(data); i += max(1, len(data)/24) {
				if got, err := scratch.Decode(data[:i]); err == nil || got != nil {
					t.Fatalf("truncation %d accepted", i)
				}
				bad := bytes.Clone(data)
				bad[i] ^= 0x80
				if got, err := scratch.Decode(bad); err == nil {
					// Some header changes can be semantically identical (e.g. step of one point).
					if len(got) != len(want) {
						t.Fatal("corrupt count accepted")
					}
					for j, p := range want {
						if got[j].Timestamp != p.Timestamp || math.Float64bits(got[j].Value) != math.Float64bits(p.Value) {
							t.Fatalf("corruption %d accepted", i)
						}
					}
				} else if got != nil {
					t.Fatal("error returned points")
				}
			}
		}
	}
	// Ownership of the compatibility API must survive unrelated later decodes.
	want := sealedReuseFixture(4096, "special")
	data, _ := EncodeSealedPoints(want, 60)
	owned, err := DecodeSealedPoints(data)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := EncodeSealedPoints(sealedReuseFixture(4096, "counter"), 60)
	if _, err := scratch.Decode(other); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSealedPoints(other); err != nil {
		t.Fatal(err)
	}
	for i, p := range want {
		if math.Float64bits(owned[i].Value) != math.Float64bits(p.Value) {
			t.Fatal("owned points overwritten")
		}
	}
}

func TestSealedPointBufferExpansionBoundAfterLargeBlock(t *testing.T) {
	var scratch SealedPointBuffer
	data, _ := EncodeSealedPoints(sealedReuseFixture(MaxSealedPoints, "random"), 60)
	if _, err := scratch.Decode(data); err != nil {
		t.Fatal(err)
	}
	// A second frame may not use capacity left over from the large block,
	// even if the overall compressed input fits.
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, math.Float64bits(1))
	compressed := z.EncodeAll(body, nil)
	var frame zstd.Header
	if err := frame.Decode(compressed); err != nil {
		t.Fatal(err)
	}
	limit := 20 // one point, when the frame has no content-size field
	if frame.HasFCS {
		limit = int(frame.FrameContentSize)
	}
	compressed = append(compressed, z.EncodeAll(make([]byte, 32), nil)...)
	bad := make([]byte, sealedHeader)
	bad[0], bad[1] = 3, sealedImplicit|sealedZstd
	binary.LittleEndian.PutUint16(bad[2:4], 1)
	binary.LittleEndian.PutUint32(bad[4:8], 1700000040)
	binary.LittleEndian.PutUint32(bad[8:12], 60)
	header := bytes.Clone(bad[:12])
	header[1] &^= sealedZstd
	binary.LittleEndian.PutUint32(bad[12:16], crc32.Update(crc32.Update(0, crc32.IEEETable, header), crc32.IEEETable, body))
	for i := range scratch.body[:cap(scratch.body)] {
		scratch.body[:cap(scratch.body)][i] = 0xcc
	}
	if points, err := scratch.Decode(append(bad, compressed...)); err == nil || points != nil {
		t.Fatal("concatenated expansion accepted")
	}
	for _, v := range scratch.body[:cap(scratch.body)][limit:] {
		if v != 0xcc {
			t.Fatal("decoder wrote beyond per-frame expansion bound")
		}
	}
	if _, err := scratch.Decode(data); err != nil {
		t.Fatal("error poisoned scratch", err)
	}
}

func TestSealedPointBufferDecimalReuse(t *testing.T) {
	var scratch SealedPointBuffer
	for _, n := range []int{4096, 128, 8192, 1} {
		for _, linear := range []bool{false, true} {
			want := sealedReuseFixture(n, "decimal")
			if linear {
				for i := range want {
					want[i].Value = float64(i*25) / 100
				}
			}
			data, err := EncodeSealedPointsDecimal(want, 60)
			if err != nil {
				t.Fatal(err)
			}
			got, err := scratch.Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range want {
				if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
					t.Fatalf("decimal/%d point %d differs", n, i)
				}
			}
		}
	}
}

func BenchmarkSealedDecodeReuse(b *testing.B) {
	for _, n := range []int{128, 4096} {
		for _, kind := range []string{"counter", "decimal", "random"} {
			b.Run(fmt.Sprintf("%s/%d", kind, n), func(b *testing.B) {
				data, err := EncodeSealedPoints(sealedReuseFixture(n, kind), 60)
				if err != nil {
					b.Fatal(err)
				}
				var scratch SealedPointBuffer
				if _, err := scratch.Decode(data); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				cpu := sealedCPU()
				b.ResetTimer()
				for range b.N {
					if _, err := scratch.Decode(data); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(sealedCPU()-cpu)/float64(b.N), "cpu-ns/op")
				b.ReportMetric(float64(len(data)), "encoded-B/block")
			})
		}
	}
}
