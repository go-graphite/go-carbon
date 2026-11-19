package v2part

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func valueRun(bits ...uint64) []sealrollup.Record {
	rs := make([]sealrollup.Record, len(bits))
	for i, b := range bits {
		rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Slot: int64(i + 1), Order: uint64(i + 1),
			Sample: sealrollup.Sample{Time: int64(1000 + 10*i), Bits: b, Sequence: uint64(i + 1)}}}
	}
	return rs
}

func floats(fs ...float64) []uint64 {
	out := make([]uint64, len(fs))
	for i, f := range fs {
		out[i] = math.Float64bits(f)
	}
	return out
}

// TestDenseValueCodecFloatEdges round-trips the value bits that decimal
// conversion must never alter and checks which codec each run selects.
func TestDenseValueCodecFloatEdges(t *testing.T) {
	counter := make([]float64, 300)
	gauge := make([]float64, 300)
	for i := range counter {
		counter[i] = float64(1_000_000 + 1000*i)
		gauge[i] = float64(i%7) * 0.25
	}
	for _, tc := range []struct {
		name string
		bits []uint64
		tag  byte
	}{
		{"counter", floats(counter...), valueDelta2},
		{"decimal gauge", floats(gauge...), valueDelta1 | 2},
		{"cents", floats(0.1, 0.2, 0.35, 12.07), valueDelta1 | 2},
		{"negative zero", floats(1, math.Copysign(0, -1), 2), valuePlanes},
		{"positive zero", floats(0, 0, 0), valueDelta1},
		{"quiet NaN payload", []uint64{0x7ff8000000000001, math.Float64bits(1)}, valuePlanes},
		{"signalling NaN", []uint64{0x7ff0000000000001}, valuePlanes},
		{"negative NaN", []uint64{0xfff8000000000000}, valuePlanes},
		{"infinities", floats(math.Inf(1), math.Inf(-1)), valuePlanes},
		{"above 2^53", floats(1<<53+2, 1<<53+4), valuePlanes},
		{"2^53", floats(1<<53, 1<<53, 1<<53, 1<<53, -(1 << 53), -(1 << 53), -(1 << 53), -(1 << 53)), valueDelta1},
		{"subnormal", []uint64{1, 0x000fffffffffffff}, valuePlanes},
		{"max float", floats(math.MaxFloat64), valuePlanes},
		{"true floats", floats(1.0/3, math.Pi, math.E, math.Sqrt2), valuePlanes},
		{"wide binary fractions", floats(1250.03125, 31249.96875, 7.5, 20000.40625), valuePlanes},
		{"tiny decimal", floats(1e-18, 2e-18), valueDelta1 | 18},
	} {
		v, err := EncodeSlots(valueRun(tc.bits...))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if v[4] != denseTagged {
			t.Fatalf("%s: mode %#x", tc.name, v[4])
		}
		got, err := DecodeSlots(0, 1, v)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for i := range tc.bits {
			if got[i].Sample.Bits != tc.bits[i] {
				t.Fatalf("%s: value %d bits %#x != %#x", tc.name, i, got[i].Sample.Bits, tc.bits[i])
			}
		}
		flags := len(v) - len(appendValues(nil, valueRun(tc.bits...)))
		if tag := v[flags]; tag != tc.tag {
			t.Fatalf("%s: tag %#x, want %#x", tc.name, tag, tc.tag)
		}
	}
}

// TestDenseValueCodecLegacyRuns decodes untagged mode 0 (byte planes) and
// mode 1 (signed varint integer differences) runs written before the tag.
func TestDenseValueCodecLegacyRuns(t *testing.T) {
	for _, tc := range []struct {
		mode byte
		bits []uint64
	}{
		{1, floats(5, 3, -7, 1<<53)},
		{0, []uint64{0x7ff8000000000001, math.Float64bits(math.Copysign(0, -1)), math.Float64bits(0.1)}},
	} {
		rs := valueRun(tc.bits...)
		v, err := EncodeSlots(rs)
		if err != nil {
			t.Fatal(err)
		}
		head := len(v) - len(appendValues(nil, rs))
		legacy := append([]byte(nil), v[:head]...)
		legacy[4] = tc.mode
		if tc.mode == 1 {
			var prev int64
			for _, b := range tc.bits {
				x := int64(math.Float64frombits(b))
				legacy = binary.AppendVarint(legacy, x-prev)
				prev = x
			}
		} else {
			for plane := uint(0); plane < 8; plane++ {
				for _, b := range tc.bits {
					legacy = append(legacy, byte(b>>(plane*8)))
				}
			}
		}
		got, err := DecodeSlots(0, 1, legacy)
		if err != nil {
			t.Fatalf("mode %d: %v", tc.mode, err)
		}
		for i := range tc.bits {
			if got[i].Sample.Bits != tc.bits[i] {
				t.Fatalf("mode %d: value %d bits %#x != %#x", tc.mode, i, got[i].Sample.Bits, tc.bits[i])
			}
		}
	}
}

func TestDenseValueCodecRejectsCorruptTags(t *testing.T) {
	rs := valueRun(floats(1, 2, 3)...)
	v, err := EncodeSlots(rs)
	if err != nil {
		t.Fatal(err)
	}
	tagAt := len(v) - len(appendValues(nil, rs))
	for _, tag := range []byte{valueDelta1 | 19, 3 << 5, valuePlanes | 1} {
		bad := append([]byte(nil), v...)
		bad[tagAt] = tag
		if _, err := DecodeSlots(0, 1, bad); err == nil {
			t.Fatalf("tag %#x accepted", tag)
		}
	}
	if _, err := DecodeSlots(0, 1, v[:tagAt]); err == nil {
		t.Fatal("missing tag accepted")
	}
}
