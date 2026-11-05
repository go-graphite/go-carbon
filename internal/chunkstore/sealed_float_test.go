package chunkstore

import (
	"encoding/hex"
	"math"
	"math/rand/v2"
	"testing"
)

func assertFloatPoints(t testing.TB, want, got []Point) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatal("count")
	}
	for i, p := range want {
		if p.Timestamp != got[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(got[i].Value) {
			t.Fatalf("point %d: %+v/%016x != %+v/%016x", i, p, math.Float64bits(p.Value), got[i], math.Float64bits(got[i].Value))
		}
	}
}
func TestSealedFloatIEEE(t *testing.T) {
	specials := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0xfff0000000000001, 0x7ff0000000000001, 0x000fffffffffffff, 0x0010000000000000}
	for _, n := range []int{1, 2, 31, 128, 129, 257, 4096, 8192} {
		for family := 0; family < 5; family++ {
			r := rand.New(rand.NewPCG(uint64(n), uint64(family)))
			p := make([]Point, n)
			for i := range p {
				v := r.Uint64()
				switch family {
				case 0:
					v = specials[i%len(specials)]
				case 1:
					v = math.Float64bits(float64(r.IntN(100000)) / 100)
				case 2:
					v = math.Float64bits(float64(r.IntN(16)) / 100)
				case 3:
					if i%17 != 0 {
						v = math.Float64bits(float64(i) / 10)
					}
				}
				p[i] = Point{Timestamp: 1700000000 + int64((i+i/7)*60), Value: math.Float64frombits(v)}
			}
			b, e := EncodeSealedPointsFloat(p, 60)
			if e != nil {
				t.Fatal(e)
			}
			base, e := EncodeSealedPoints(p, 60)
			if e != nil {
				t.Fatal(e)
			}
			if len(b) > len(base) {
				t.Fatal("fallback grew")
			}
			got, e := DecodeSealedPoints(b)
			if e != nil {
				t.Fatal(e)
			}
			assertFloatPoints(t, p, got)
			var scratch SealedPointBuffer
			for range 2 {
				got, e = scratch.Decode(b)
				if e != nil {
					t.Fatal(e)
				}
				assertFloatPoints(t, p, got)
			}
			for k := 0; k < len(b); k += max(1, len(b)/31) {
				if _, e := DecodeSealedPoints(b[:k]); e == nil {
					t.Fatal("accepted truncation")
				}
			}
		}
	}
}
func TestSealedFloatGolden(t *testing.T) {
	// Uncompressed vectors pin version/flags, timestamp framing, codec dispatch,
	// exception positions, dictionary ordering, ID bit order and zero padding.
	vectors := []struct {
		hex   string
		words []uint64
	}{
		{"0404020000f153653c00000072ab4bf302020000000000000080341200000000f87f02", []uint64{0x8000000000000000, 0x7ff8000000001234}},
		{"0404020000f153653c00000086a086e0010101010000000000f07f0300fa01", []uint64{math.Float64bits(1.25), 0x7ff0000000000001}},
		{"0404020000f153653c00000020febca103000000000000f03f0000", []uint64{0x3ff0000000000000, 0x3ff0000000000000}},
	}
	for _, v := range vectors {
		b, e := hex.DecodeString(v.hex)
		if e != nil {
			t.Fatal(e)
		}
		p, e := DecodeSealedPoints(b)
		if e != nil {
			t.Fatal(e)
		}
		for i, w := range v.words {
			if p[i].Timestamp != 1700000000+int64(i*60) || math.Float64bits(p[i].Value) != w {
				t.Fatal("golden mismatch")
			}
		}
	}
}
func FuzzSealedFloatRoundTrip(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7}, true)
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255}, false)
	f.Fuzz(func(t *testing.T, data []byte, gaps bool) {
		if len(data) == 0 {
			return
		}
		n := min(8192, len(data))
		p := make([]Point, n)
		for i := range p {
			var v uint64
			for j := 0; j < 8; j++ {
				v |= uint64(data[(i+j)%len(data)]) << uint(j*8)
			}
			ts := i
			if gaps {
				ts += i / 5
			}
			p[i] = Point{Timestamp: 1700000000 + int64(ts*60), Value: math.Float64frombits(v)}
		}
		b, e := EncodeSealedPointsFloat(p, 60)
		if e != nil {
			t.Fatal(e)
		}
		got, e := DecodeSealedPoints(b)
		if e != nil {
			t.Fatal(e)
		}
		assertFloatPoints(t, p, got)
	})
}
func FuzzSealedFloatDecode(f *testing.F) {
	f.Add([]byte{4, 4, 1, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		var scratch SealedPointBuffer
		_, _ = scratch.Decode(b)
		if len(b) < maxSealedBody {
			for _, mode := range []byte{sealedFloatALPMode, sealedFloatDictionaryMode, sealedFloatChimpMode} {
				body := append([]byte{mode}, b...)
				frame := encodeSealedBodyVersion(129, 1700000000, 60, sealedImplicit, body, 4)
				_, _ = scratch.Decode(frame)
			}
		}

	})
}

func TestSealedFloatCandidateOracles(t *testing.T) {
	for _, n := range []int{1, 2, 129, 4096, 8192} {
		r := rand.New(rand.NewPCG(uint64(n), 19))
		p := make([]Point, n)
		for i := range p {
			p[i] = Point{Timestamp: 1700000000 + int64(i*60), Value: math.Float64frombits(r.Uint64())}
		}
		p[0].Value = .3
		for _, mode := range []byte{sealedFloatALPMode, sealedFloatDictionaryMode, sealedFloatChimpMode} {
			body := []byte{mode}
			switch mode {
			case sealedFloatALPMode:
				body = appendSealedALP(body, p, 1)
			case sealedFloatDictionaryMode:
				body = appendSealedDictionary(body, p)
			case sealedFloatChimpMode:
				body = appendSealedChimp(body, p)
			}
			frame := encodeSealedBodyVersion(n, p[0].Timestamp, 60, sealedImplicit, body, 4)
			got, err := DecodeSealedPoints(frame)
			if err != nil {
				t.Fatalf("mode=%d n=%d: %v", mode, n, err)
			}
			assertFloatPoints(t, p, got)
		}
	}
}

func TestSealedFloatBitWindows(t *testing.T) {
	r := rand.New(rand.NewPCG(37, 61))
	var w sealedBitWriter
	var values []uint64
	var widths []uint
	for repeat := 0; repeat < 31; repeat++ {
		for width := uint(0); width <= 64; width++ {
			v := r.Uint64()
			if width < 64 {
				v &= (uint64(1) << width) - 1
			}
			w.put(v, width)
			values = append(values, v)
			widths = append(widths, width)
		}
	}
	reader := sealedBitReader{data: w.finish()}
	for i, v := range values {
		if got := reader.get(widths[i]); got != v {
			t.Fatalf("width=%d got=%x want=%x", widths[i], got, v)
		}
	}
	if !reader.done() {
		t.Fatal("padding")
	}
	reader.get(64)
	if !reader.failed {
		t.Fatal("out-of-bounds read accepted")
	}
}

func BenchmarkSealedFloatSelectors(b *testing.B) {
	points := make([]Point, 4096)
	for i := range points {
		points[i] = Point{Timestamp: int64(i*60 + 60), Value: float64(i%31) / 3}
	}
	for _, tt := range []struct {
		name string
		enc  func([]Point, int) ([]byte, error)
	}{{"prefix", EncodeSealedPointsFloat}, {"exhaustive", EncodeSealedPointsFloatMin}} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(points)))
			for i := 0; i < b.N; i++ {
				if _, err := tt.enc(points, 60); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestExhaustiveFloatPolicy(t *testing.T) {
	if _, err := NormalizeColdPack(ColdPackOptions{FloatExhaustive: true}); err == nil {
		t.Fatal("exhaustive without float codec accepted")
	}
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}})
	defer closeTestStore(t, s)
	s.StopBackground()
	o := ColdPackOptions{Mode: "exact", FloatCodec: true}
	if err := s.ConfigureColdPack(o); err != nil {
		t.Fatal(err)
	}
	s.coldPack.mu.Lock()
	s.coldPack.stats.Cursor = "old.policy"
	s.coldPack.stats.BlockCursor = 123
	s.coldPack.mu.Unlock()
	o.FloatExhaustive = true
	if err := s.ConfigureColdPack(o); err != nil {
		t.Fatal(err)
	}
	if st := s.ColdPackStats(); st.Cursor != "" || st.BlockCursor != 0 {
		t.Fatal("selector change retained old cursor", st)
	}
}
