package v2part

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestDensePredictedOrders(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 57))
	for _, n := range []int{2, 3, 7, 8, 9, 16, 17, 63, 64, 65, MaxDenseSlots} {
		for _, pattern := range []string{"constant", "rising", "falling", "wrap", "random"} {
			rs := make([]sealrollup.Record, n)
			for i := range rs {
				var order uint64
				switch pattern {
				case "constant":
					order = math.MaxUint64
				case "rising":
					order = uint64(1 + i*10000003 + i%17)
				case "falling":
					order = math.MaxUint64 - uint64(i*10000003+i%17)
				case "wrap":
					order = math.MaxUint64 - uint64(n/2) + uint64(i)
					if order == 0 {
						order = 17
					}
				case "random":
					order = rng.Uint64() | 1
				}
				rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: int64(i + 1), Order: order, Sample: sealrollup.Sample{Time: int64(1700000000 + i), Bits: rng.Uint64(), Sequence: order}}, Correction: i%3 == 0}
			}
			b, e := appendSlotsOrder(nil, rs, nil, true)
			if e != nil {
				t.Fatal(e)
			}
			got, e := DecodeSlots(1, 1, b)
			if e != nil || !reflect.DeepEqual(got, rs) {
				t.Fatalf("n=%d %s changed order/value bits: %v", n, pattern, e)
			}
			// The order header follows n slots and aligned timestamp base/step.
			p := 5
			_, k := binary.Uvarint(b[p:])
			p += k
			for i := 0; i < n+4; i++ {
				_, k = binary.Uvarint(b[p:])
				p += k
			}
			width := p
			bad := append([]byte(nil), b...)
			bad[width] = 65
			if _, e = DecodeSlots(1, 1, bad); e == nil {
				t.Fatal("accepted width >64")
			}
			bad = append([]byte(nil), b...)
			bad[4] &^= denseModeOrderLinear
			if _, e = DecodeSlots(1, 1, bad); e == nil {
				t.Fatal("accepted bit planes without prediction")
			}
			if tail := uint((n - 1) % 8); tail != 0 && b[width] > 0 {
				bad = append([]byte(nil), b...)
				stride := (n - 1 + 7) / 8
				bad[width+stride] |= 1 << tail
				if _, e = DecodeSlots(1, 1, bad); e == nil {
					t.Fatal("accepted nonzero plane padding")
				}
			}
			bad = append([]byte(nil), b...)
			bad[4] |= denseModeOrderDoD
			if _, e = DecodeSlots(1, 1, bad); e == nil {
				t.Fatal("accepted two order codecs")
			}
			for cut := width; cut < len(b); cut += max(1, (len(b)-width)/31) {
				if _, e = DecodeSlots(1, 1, b[:cut]); e == nil {
					t.Fatalf("accepted truncated predicted run at %d", cut)
				}
			}
		}
	}
}

// An old merged run keeps its DoD order column readable after the writer moves
// to prediction. This literal pins the former on-disk layout independently.
func TestDenseLegacyOrderDoD(t *testing.T) {
	b := []byte{'G', 'C', 'D', 1, denseTagged | denseAligned | denseModeOrderDoD, 3,
		1, 1, 1, // physical slots
		100, 1, // aligned timestamp base and step
		20, 20, 10, // zigzag order differences: 10, 10, 15
		0,                    // correction flags
		valueDelta1, 2, 2, 2} // exponent tag and decimal value deltas
	got, err := DecodeSlots(1, 1, b)
	if err != nil {
		t.Fatal(err)
	}
	for i, order := range []uint64{10, 20, 35} {
		if got[i].Order != order || got[i].Sample.Sequence != order || got[i].Sample.Bits != math.Float64bits(float64(i+1)) {
			t.Fatalf("legacy record %d changed: %+v", i, got[i])
		}
	}
}

// A literal from the byte-plane predictor preserves compatibility independently
// of the current writer. Orders 10, 20, 35 use step 12 and residuals -2,+1.
func TestDenseLegacyOrderLinear(t *testing.T) {
	b := []byte{'G', 'C', 'D', 1, denseTagged | denseAligned | denseModeOrderLinear, 3,
		1, 1, 1, 100, 1, 10, 24, 1, 3, 2, 0, valueDelta1, 2, 2, 2}
	got, err := DecodeSlots(1, 1, b)
	if err != nil {
		t.Fatal(err)
	}
	for i, order := range []uint64{10, 20, 35} {
		if got[i].Order != order || got[i].Sample.Sequence != order || got[i].Sample.Bits != math.Float64bits(float64(i+1)) {
			t.Fatalf("legacy record %d changed: %+v", i, got[i])
		}
	}
}

func TestTransposeOrderBitsOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(93, 11))
	for i := 0; i < 1000; i++ {
		x := rng.Uint64()
		var want uint64
		for row := 0; row < 8; row++ {
			for col := 0; col < 8; col++ {
				want |= ((x >> uint(row*8+col)) & 1) << uint(col*8+row)
			}
		}
		if got := transposeOrderBits(x); got != want || transposeOrderBits(got) != x {
			t.Fatalf("transpose %x: got %x want %x", x, got, want)
		}
	}
}

// The predicted order column is pinned bit by bit, independently of the
// transposing writer: bit b of residual i is bit (i-1)%8 of byte
// b*stride+(i-1)/8 after the width byte.
func TestPredictedOrderPlanesOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 19))
	for _, n := range []int{2, 3, 8, 9, 17, 66, 4096} {
		for _, spread := range []uint64{0, 1, 1 << 7, 1 << 20, 1 << 33, math.MaxUint64} {
			rs := make([]sealrollup.Record, n)
			for i := range rs {
				var noise uint64
				if spread != 0 {
					noise = rng.Uint64() % spread
					if spread == math.MaxUint64 {
						noise = rng.Uint64()
					}
				}
				rs[i].Order = uint64(i)*104729 + noise + 1
			}
			first := rs[0].Order
			step := int64(rs[n-1].Order-first) / int64(n-1)
			want := binary.AppendVarint(binary.AppendUvarint(nil, first), step)
			res := make([]uint64, n-1)
			width := 0
			for i := range res {
				res[i] = zigzag(int64(rs[i+1].Order - first - uint64(step)*uint64(i+1)))
				for res[i]>>uint(width) > 0 && width < 64 {
					width++
				}
			}
			want = append(want, byte(width))
			for b := 0; b < width; b++ {
				for i := 0; i < len(res); i += 8 {
					var x byte
					for j := 0; j < 8 && i+j < len(res); j++ {
						x |= byte(res[i+j]>>uint(b)&1) << uint(j)
					}
					want = append(want, x)
				}
			}
			if got := appendPredictedOrders(nil, rs); string(got) != string(want) {
				t.Fatalf("n=%d spread=%d: order column differs from bit oracle", n, spread)
			}
		}
	}
}
