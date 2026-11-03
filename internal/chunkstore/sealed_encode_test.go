package chunkstore

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"syscall"
	"testing"
)

// legacyEncodeSealedPoints is the encoder at df9e1a4a, retained as a size and
// decoded-value oracle for encoder-only changes. It uses the same wire envelope.
func legacyEncodeSealedPoints(points []Point, step int) ([]byte, error) {
	if len(points) == 0 || len(points) > MaxSealedPoints || step <= 0 || uint64(step) > math.MaxUint32 {
		return nil, errInvalidChunk
	}
	flags := byte(sealedImplicit)
	integer := true
	for i, p := range points {
		if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || i > 0 && p.Timestamp <= points[i-1].Timestamp {
			return nil, errInvalidChunk
		}
		if i > 0 && p.Timestamp-points[i-1].Timestamp != int64(step) {
			flags &^= sealedImplicit
		}
		if !(p.Value >= -float64(exactIntegerLimit) && p.Value <= float64(exactIntegerLimit)) || math.Float64bits(float64(int64(p.Value))) != math.Float64bits(p.Value) {
			integer = false
		}
	}
	body := make([]byte, 0, len(points)*9)
	if flags&sealedImplicit == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	timeEnd := len(body)
	if integer {
		flags |= sealedInteger
		var previous int64
		for _, p := range points {
			value := int64(p.Value)
			body = binary.AppendVarint(body, value-previous)
			previous = value
		}
	} else {
		for plane := uint(0); plane < 8; plane++ {
			for _, p := range points {
				body = append(body, byte(math.Float64bits(p.Value)>>(plane*8)))
			}
		}
	}
	encode := func(flags byte, body []byte) []byte {
		return encodeSealedBody(len(points), points[0].Timestamp, step, flags, body)
	}
	best := encode(flags, body)
	if integer {
		dod := append([]byte(nil), body[:timeEnd]...)
		var previous, delta int64
		for _, p := range points {
			value := int64(p.Value)
			next := value - previous
			dod = binary.AppendVarint(dod, next-delta)
			previous, delta = value, next
		}
		if candidate := encode(flags|sealedDoD, dod); len(candidate) < len(best) {
			best = candidate
		}
	}
	return best, nil
}

// The yield families reproduce cold-pack/yield_test.go's integer/fractional
// formulas. The remaining families exercise exact/decimal candidate boundaries;
// the equal mix is a sensitivity screen, not a measured fleet distribution.
func sealedEncodeFixture(kind string, n int, seed int64) []Point {
	r := rand.New(rand.NewSource(seed))
	p := make([]Point, n)
	counter := int64(1000000)
	for i := range p {
		v := float64((i*17 + int(r.Int31n(7))) % 1000)
		switch kind {
		case "constant":
			v = 7
		case "linear":
			v = float64(1000000 + i*123)
		case "counter", "reset":
			counter += int64(r.Intn(128))
			if kind == "reset" && i%997 == 996 {
				counter = 0
			}
			v = float64(counter)
		case "gauge":
			v = float64(r.Intn(1000000) - 500000)
		case "smooth":
			v = math.Round(10000 + 1000*math.Sin(float64(i)*.01))
		case "rounded":
			v = float64(r.Intn(100000)) / 100
		case "fractional":
			v = 1000 + math.Sin(float64(i)*.013)*31.123456789 + float64(r.Int31n(1000))/997
		case "float":
			v = math.Float64frombits(0x3ff0000000000000 | r.Uint64()>>12)
		}
		p[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: v}
	}
	return p
}
func sealedCPU() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return (u.Utime.Sec+u.Stime.Sec)*1e9 + int64(u.Utime.Usec+u.Stime.Usec)*1000
}
func BenchmarkSealedEncode(b *testing.B) {
	for _, n := range []int{128, 4096} {
		for _, kind := range []string{"constant", "linear", "counter", "reset", "gauge", "smooth", "integer", "fractional", "rounded", "float"} {
			p := sealedEncodeFixture(kind, n, 42)
			for _, enc := range []struct {
				name string
				fn   func([]Point, int) ([]byte, error)
			}{
				{"before", legacyEncodeSealedPoints}, {"after", EncodeSealedPoints},
			} {
				b.Run(fmt.Sprintf("%d/%s/%s", n, kind, enc.name), func(b *testing.B) {
					data, err := enc.fn(p, 60)
					if err != nil {
						b.Fatal(err)
					}
					size := len(data)
					b.ReportAllocs()
					b.ResetTimer()
					cpu := sealedCPU()
					for b.Loop() {
						data, err = enc.fn(p, 60)
						if err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(sealedCPU()-cpu)/float64(b.N), "cpu-ns/op")
					b.ReportMetric(float64(size)/float64(n), "encoded-B/point")
					b.ReportMetric(float64(size), "encoded-B/block")
				})
			}
		}
	}
}
func TestSealedEncodeOracle(t *testing.T) {
	for _, n := range []int{1, 2, 3, 127, 128, 129, 4096, MaxSealedPoints} {
		for _, kind := range []string{"constant", "linear", "counter", "reset", "gauge", "smooth", "integer", "fractional", "rounded", "float"} {
			p := sealedEncodeFixture(kind, n, 42)
			for _, fn := range []func([]Point, int) ([]byte, error){legacyEncodeSealedPoints, EncodeSealedPoints} {
				data, err := fn(p, 60)
				if err != nil {
					t.Fatal(err)
				}
				got, err := DecodeSealedPoints(data)
				if err != nil {
					t.Fatal(err)
				}
				for i := range p {
					if p[i].Timestamp != got[i].Timestamp || math.Float64bits(p[i].Value) != math.Float64bits(got[i].Value) {
						t.Fatalf("%s/%d/%d", kind, n, i)
					}
				}
			}
		}
	}
}

func TestSealedIntegerRunsExact(t *testing.T) {
	for _, values := range [][]float64{
		{0, 0, 0, 0}, {-7, -7, -7, -7}, {1, 2, 3, 4}, {9, 6, 3, 0, -3},
		{-0x1p53, -0x1p53 + 1, -0x1p53 + 2, -0x1p53 + 3},
		{0x1p53, 0x1p53 - 1, 0x1p53 - 2, 0x1p53 - 3},
		{-0x1p53, 0, 0x1p53},
	} {
		for _, gaps := range []bool{false, true} {
			points := make([]Point, len(values))
			for i, v := range values {
				points[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: v}
				if gaps {
					points[i].Timestamp += int64(i / 2 * 60)
				}
			}
			data, err := EncodeSealedPoints(points, 60)
			if err != nil {
				t.Fatal(err)
			}
			if len(values) >= 4 && data[1]&sealedDecimal == 0 {
				t.Fatal("exact run did not use existing decimal mode")
			}
			got, err := DecodeSealedPoints(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range points {
				if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
					t.Fatal("run changed value bits/timestamps")
				}
			}
		}
	}
	// A late invalid point must invalidate a promising prefix, including -0 and
	// integers just outside the accepted range. The entire block stays exact.
	for _, v := range []float64{math.Copysign(0, -1), 0x1p53 + 2, -0x1p53 - 2, math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000123), math.SmallestNonzeroFloat64, 1.5} {
		points := sealedEncodeFixture("constant", 4096, 1)
		points[len(points)-1].Value = v
		data, err := EncodeSealedPoints(points, 60)
		if err != nil {
			t.Fatal(err)
		}
		if data[1]&(sealedInteger|sealedDecimal) != 0 {
			t.Fatalf("noninteger %x used integer mode", math.Float64bits(v))
		}
		got, err := DecodeSealedPoints(data)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range points {
			if math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
				t.Fatal("fallback changed bits")
			}
		}
	}
	// Timestamp validation still scans the end of a constant run.
	largeStep := int64(math.MaxUint32) + 1
	for _, step := range []int{0, -1, int(largeStep)} {
		if _, err := EncodeSealedPoints(sealedEncodeFixture("constant", 4, 1), step); err == nil {
			t.Fatal("invalid step")
		}
	}
	points := sealedEncodeFixture("constant", 4096, 1)
	points[len(points)-1].Timestamp = points[len(points)-2].Timestamp
	if _, err := EncodeSealedPoints(points, 60); err == nil {
		t.Fatal("duplicate timestamp")
	}
}

func TestSealedDeltaSelectionSizes(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 127, 128, 129, 256, 4096, 8192} {
		for _, kind := range []string{"constant", "linear", "counter", "reset", "gauge", "smooth", "integer", "fractional", "rounded", "float"} {
			for seed := range 20 {
				points := sealedEncodeFixture(kind, n, int64(seed))
				before, err := legacyEncodeSealedPoints(points, 60)
				if err != nil {
					t.Fatal(err)
				}
				after, err := EncodeSealedPoints(points, 60)
				if err != nil {
					t.Fatal(err)
				}
				// This is a corpus guard, not a universal compression-size guarantee.
				if len(after) > len(before) {
					t.Fatalf("%s/%d/%d grew %d -> %d", kind, n, seed, len(before), len(after))
				}
				got, err := DecodeSealedPoints(after)
				if err != nil {
					t.Fatal(err)
				}
				for i, p := range points {
					if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
						t.Fatal("round trip changed bits")
					}
				}
			}
		}
	}
	for _, kind := range []string{"counter", "gauge", "integer"} {
		_, ambiguous := sealedDeltaChoice(sealedEncodeFixture(kind, 4096, 42))
		if ambiguous {
			t.Fatalf("%s not classified", kind)
		}
	}
	_, ambiguous := sealedDeltaChoice(sealedEncodeFixture("smooth", 4096, 42))
	if !ambiguous {
		t.Fatal("smooth gauge lost exhaustive fallback")
	}
}

func TestSealedVarintLen(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 63, 64, -64, -65, 8191, 8192, -8192, -8193, math.MinInt64, math.MaxInt64} {
		if got, want := sealedVarintLen(v), len(binary.AppendVarint(nil, v)); got != want {
			t.Fatalf("%d: %d != %d", v, got, want)
		}
	}
}

func legacyEncodeSealedPointsDecimal(points []Point, step int) ([]byte, error) {
	exact, err := legacyEncodeSealedPoints(points, step)
	if err != nil {
		return nil, err
	}
	exponent := 309
	for _, p := range points {
		e, ok := decimalExponent(p.Value)
		if !ok {
			return exact, nil
		}
		if p.Value != 0 && e < exponent {
			exponent = e
		}
	}
	if exponent == 309 {
		exponent = 0
	}
	values := make([]int64, len(points))
	scale := math.Pow10(exponent)
	for i, p := range points {
		m, ok := decimalMantissa(p.Value, scale)
		if !ok {
			return exact, nil
		}
		values[i] = m
	}
	flags := exact[1]&sealedImplicit | sealedDecimal
	var body []byte
	if flags&sealedImplicit == 0 {
		for i := 1; i < len(points); i++ {
			body = binary.AppendUvarint(body, uint64(points[i].Timestamp-points[i-1].Timestamp))
		}
	}
	body = appendDecimalIntegers(body, values, exponent)
	candidate := encodeSealedBody(len(points), points[0].Timestamp, step, flags, body)
	if len(candidate) < len(exact) {
		return candidate, nil
	}
	return exact, nil
}

func BenchmarkSealedEncodeDecimal(b *testing.B) {
	for _, kind := range []string{"constant", "linear", "fractional"} {
		p := sealedEncodeFixture(kind, 4096, 42)
		for _, enc := range []struct {
			name string
			fn   func([]Point, int) ([]byte, error)
		}{{"before", legacyEncodeSealedPointsDecimal}, {"after", EncodeSealedPointsDecimal}} {
			b.Run(kind+"/"+enc.name, func(b *testing.B) {
				data, err := enc.fn(p, 60)
				if err != nil {
					b.Fatal(err)
				}
				size := len(data)
				b.ReportAllocs()
				b.ResetTimer()
				cpu := sealedCPU()
				for b.Loop() {
					if _, err := enc.fn(p, 60); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(sealedCPU()-cpu)/float64(b.N), "cpu-ns/op")
				b.ReportMetric(float64(size), "encoded-B/block")
			})
		}
	}
}
