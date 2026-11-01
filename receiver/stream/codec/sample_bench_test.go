package codec

import (
	"bytes"
	"compress/gzip"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type samplePoint struct {
	name  []byte
	value float64
	ts    int64
}

// BenchmarkSample measures an actual capture from a fresh connection, including
// name definitions. No sample names or contents are written to benchmark output.
// Set STREAM_SAMPLE_FILE to plaintext or gzip-compressed Graphite plaintext.
func loadSample(b testing.TB) ([]byte, []samplePoint) {
	b.Helper()
	path := os.Getenv("STREAM_SAMPLE_FILE")
	if path == "" {
		b.Skip("set STREAM_SAMPLE_FILE to a private capture")
	}
	plain, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	if bytes.HasPrefix(plain, []byte{0x1f, 0x8b}) {
		z, err := gzip.NewReader(bytes.NewReader(plain))
		if err != nil {
			b.Fatal(err)
		}
		plain, err = io.ReadAll(z)
		_ = z.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
	var samples []samplePoint
	var incomplete int
	for rest := plain; len(rest) > 0; {
		var line []byte
		var complete bool
		line, rest, complete = bytes.Cut(rest, []byte{'\n'})
		if !complete {
			incomplete++
			break
		}
		fields := bytes.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 3 {
			b.Fatal("capture contains a non-point line")
		}
		value, err := strconv.ParseFloat(string(fields[1]), 64)
		if err != nil || math.IsNaN(value) {
			b.Fatal("capture contains an invalid value")
		}
		ts, err := strconv.ParseFloat(string(fields[2]), 64)
		if err != nil || math.IsNaN(ts) {
			b.Fatal("capture contains an invalid timestamp")
		}
		samples = append(samples, samplePoint{fields[0], value, int64(ts)})
	}
	if len(samples) == 0 {
		b.Fatal("empty capture")
	}
	b.Logf("capture: %d points; %d plaintext bytes; %d incomplete trailing lines ignored", len(samples), len(plain), incomplete)
	return plain, samples
}

func BenchmarkSample(b *testing.B) {
	plain, samples := loadSample(b)
	encode := func(b *testing.B, flags byte, retain bool) ([]byte, int64) {
		e, err := NewEncoder(Options{Flags: flags})
		if err != nil {
			b.Fatal(err)
		}
		defer e.Close()
		var wire []byte
		var wireBytes int64
		for _, p := range samples {
			if err := e.Add(p.name, p.value, p.ts); err != nil {
				b.Fatal(err)
			}
			if e.Ready() {
				start := len(wire)
				wire = flush(b, e, wire)
				wireBytes += int64(len(wire) - start)
				if !retain {
					wire = wire[:0]
				}
			}
		}
		start := len(wire)
		wire = flush(b, e, wire)
		wireBytes += int64(len(wire) - start)
		return wire, wireBytes
	}
	for _, flags := range []byte{0, FlagZstd} {
		label := "raw"
		if flags != 0 {
			label = "zstd"
		}
		b.Run(label, func(b *testing.B) {
			b.Run("encode", func(b *testing.B) {
				var size int64
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_, size = encode(b, flags, false)
				}
				b.StopTimer()
				b.ReportMetric(float64(size)/float64(len(samples)), "wire-B/point")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(samples)), "ns/point")
			})
			b.Run("decode", func(b *testing.B) {
				wire, _ := encode(b, flags, true)
				frames := splitFrames(b, wire)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					d, err := NewDecoder(Options{Flags: flags})
					if err != nil {
						b.Fatal(err)
					}
					for _, frame := range frames {
						if _, err := d.Decode(frame, benchmarkVisit); err != nil {
							b.Fatal(err)
						}
					}
					d.Close()
				}
				b.StopTimer()
				b.ReportMetric(float64(len(wire))/float64(len(samples)), "wire-B/point")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(samples)), "ns/point")
			})
		})
	}
	for _, level := range []int{1, 3} {
		b.Run("plaintext_zstd_level"+strconv.Itoa(level), func(b *testing.B) {
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
			b.ReportMetric(float64(len(plain))/float64(len(samples)), "plain-B/point")
			b.ReportMetric(float64(len(dst))/float64(len(samples)), "wire-B/point")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(samples)), "ns/point")
		})
	}
	b.ReportMetric(float64(len(samples)), "capture-points")
	b.ReportMetric(float64(len(plain))/float64(len(samples)), "plain-B/point")
}

// TestSampleRoundTrip independently checks every captured point against stable
// sorting of the original rows, without including private data in test output.
func TestSampleRoundTrip(t *testing.T) {
	_, samples := loadSample(t)
	o := Options{Flags: FlagZstd, MaxNames: max(DefaultMaxNames, len(samples))}
	e, d := encoder(t, o), decoder(t, o)
	type expected struct {
		id uint32
		p  samplePoint
	}
	var want []expected
	var wire []byte
	checked := 0
	check := func() {
		sort.SliceStable(want, func(i, j int) bool { return want[i].id < want[j].id })
		wire = flush(t, e, wire[:0])
		offset := 0
		decodeAll(t, d, wire, func(id uint32, name []byte, value float64, ts int64, _ *any) {
			if offset >= len(want) {
				t.Fatal("decoder emitted an extra sample point")
			}
			w := want[offset]
			if id != w.id || !bytes.Equal(name, w.p.name) || math.Float64bits(value) != math.Float64bits(w.p.value) || ts != w.p.ts {
				t.Fatalf("sample point %d differs", checked+offset)
			}
			offset++
		})
		if offset != len(want) {
			t.Fatal("decoder omitted sample points")
		}
		checked += offset
		want = want[:0]
	}
	for _, p := range samples {
		if err := e.Add(p.name, p.value, p.ts); err != nil {
			t.Fatal(err)
		}
		want = append(want, expected{id: e.rows[len(e.rows)-1].id, p: p})
		if e.Ready() {
			check()
		}
	}
	check()
	if checked != len(samples) {
		t.Fatal("sample point count differs")
	}
}
