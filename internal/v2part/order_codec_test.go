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
	for _, n := range []int{2, 3, 64, MaxDenseSlots} {
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
			b, e := appendSlotsOrder(nil, rs, nil, PredictedOrders)
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
			bad[width] = 9
			if _, e = DecodeSlots(1, 1, bad); e == nil {
				t.Fatal("accepted width >8")
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
