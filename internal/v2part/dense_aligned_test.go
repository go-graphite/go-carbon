package v2part

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func alignedRun(slots []int64, times func(slot int64) int64) []sealrollup.Record {
	rs := make([]sealrollup.Record, len(slots))
	for i, s := range slots {
		rs[i] = sealrollup.Record{Delta: sealrollup.Delta{Slot: s, Order: uint64(i*7 + 1), Sample: sealrollup.Sample{Time: times(s), Bits: math.Float64bits(float64(i) * 1.5)}}, Correction: i%5 == 0}
	}
	return rs
}

// TestDenseAlignedTimestamps pins which runs drop the timestamp column and that
// every run, aligned or not, decodes to the exact input records.
func TestDenseAlignedTimestamps(t *testing.T) {
	const t0, step = int64(1_700_000_000), int64(60)
	ring := func(n int64, newest int64) func(int64) int64 {
		// Physical ring of n slots: slots <= newest hold the current cycle,
		// later slots the previous one.
		return func(s int64) int64 {
			if s <= newest {
				return t0 + n*step + s*step
			}
			return t0 + s*step
		}
	}
	cases := []struct {
		name    string
		slots   []int64
		times   func(int64) int64
		aligned bool
	}{
		{"contiguous", []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}, func(s int64) int64 { return t0 + (s-10)*step }, true},
		{"gaps", []int64{3, 4, 9, 100, 4095}, func(s int64) int64 { return t0 + s*10 }, true},
		{"second step", []int64{0, 2}, func(s int64) int64 { return t0 + s*3600 }, true},
		{"single slot", []int64{7}, func(s int64) int64 { return t0 }, false},
		{"unaligned point", []int64{1, 2, 3, 4}, func(s int64) int64 { return t0 + s*step + map[bool]int64{true: 1}[s == 3] }, false},
		{"inexact step", []int64{0, 2, 4}, func(s int64) int64 { return t0 + s*7/2 }, false},
		{"wrap mid-run", []int64{0, 1, 2, 3, 4, 5, 6, 7}, ring(8, 2), false},
		{"wrap first pair", []int64{0, 1, 2}, ring(8, 0), false},
		{"future to max", []int64{0, 1, 5}, func(s int64) int64 { return math.MaxUint32 - (5-s)*step }, true},
		{"future misaligned at max", []int64{0, 1, 5, 6}, func(s int64) int64 {
			if s == 6 {
				return math.MaxUint32
			}
			return math.MaxUint32 - 1 - (5-s)*step
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := alignedRun(tc.slots, tc.times)
			enc, err := EncodeSlots(in)
			if err != nil {
				t.Fatal(err)
			}
			if got := enc[4]&denseAligned != 0; got != tc.aligned {
				t.Fatalf("aligned=%v, want %v", got, tc.aligned)
			}
			out, err := DecodeSlots(0, tc.slots[0], enc)
			if err != nil {
				t.Fatal(err)
			}
			for i := range in {
				in[i].Sample.Sequence = in[i].Order
			}
			if !slices.Equal(in, out) {
				t.Fatalf("round trip:\n got %+v\nwant %+v", out, in)
			}
		})
	}
}

func TestDenseAlignedRejectsCorruptHeader(t *testing.T) {
	run := func(count int, t0, step uint64) []byte {
		b := append(slices.Clone(denseMagic), denseAligned)
		b = binary.AppendUvarint(b, uint64(count))
		for i := 0; i < count; i++ {
			b = binary.AppendUvarint(b, uint64(min(i, 1)))
		}
		b = binary.AppendUvarint(b, t0)
		b = binary.AppendUvarint(b, step)
		for i := 0; i < count; i++ {
			b = binary.AppendUvarint(b, uint64(1+2*min(i, 1)))
		}
		b = append(b, make([]byte, (count+7)/8)...)
		return append(b, bytes.Repeat([]byte{0}, 8*count)...)
	}
	if _, err := DecodeSlots(0, 0, run(2, 1_700_000_000, 60)); err != nil {
		t.Fatalf("valid aligned run rejected: %v", err)
	}
	for name, b := range map[string][]byte{
		"one slot":      run(1, 1_700_000_000, 60),
		"zero step":     run(2, 1_700_000_000, 0),
		"zero time":     run(2, 0, 60),
		"time overflow": run(2, math.MaxUint32, 1),
		"step overflow": run(2, 1, math.MaxUint32),
		"unknown mode":  append(append(slices.Clone(denseMagic), 8), run(2, 1, 1)[5:]...),
	} {
		if _, err := DecodeSlots(0, 0, b); err == nil {
			t.Errorf("%s: corrupt run accepted", name)
		}
	}
}
