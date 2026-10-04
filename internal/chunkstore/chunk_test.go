package chunkstore

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/points"
)

func TestChunkRoundTripPreservesSparsePoints(t *testing.T) {
	var want chunk
	want.set(0, points.Point{Timestamp: math.MinInt64, Value: math.Float64frombits(0x7ff8000000000042)})
	want.set(63, points.Point{Timestamp: -1, Value: math.Copysign(0, -1)})
	want.set(64, points.Point{Timestamp: 0, Value: math.Inf(1)})
	want.set(127, points.Point{Timestamp: math.MaxInt64, Value: math.Float64frombits(0xfff0000000000001)})

	encoded := encodeChunk(&want)
	var got chunk
	if err := decodeChunk(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertChunkEqual(t, &want, &got)
}

func TestChunkRoundTripEmptyAndWrappingTimestamps(t *testing.T) {
	var empty chunk
	encoded := encodeChunk(&empty)
	var got chunk
	if err := decodeChunk(encoded, &got); err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	assertChunkEqual(t, &empty, &got)

	var want chunk
	want.set(1, points.Point{Timestamp: math.MaxInt64, Value: 0})
	want.set(2, points.Point{Timestamp: math.MinInt64, Value: math.Copysign(0, -1)})
	want.set(3, points.Point{Timestamp: math.MaxInt64, Value: math.Float64frombits(0x7ff8000000000001)})
	if err := decodeChunk(encodeChunk(&want), &got); err != nil {
		t.Fatalf("decode wrapping timestamps: %v", err)
	}
	assertChunkEqual(t, &want, &got)
}

func TestChunkRoundTripDenseCompressed(t *testing.T) {
	var want chunk
	for slot := 0; slot < chunkSlots; slot++ {
		want.set(slot, points.Point{Timestamp: int64(slot) * 60, Value: 1})
	}
	encoded := encodeChunk(&want)
	if encoded[1]&chunkCompressed == 0 {
		t.Fatal("dense repetitive chunk was not compressed")
	}
	var got chunk
	if err := decodeChunk(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertChunkEqual(t, &want, &got)
}

func TestDecodeChunkRejectsMalformedInput(t *testing.T) {
	var c chunk
	c.set(1, points.Point{Timestamp: 1, Value: 2})
	valid := encodeChunk(&c)
	cases := [][]byte{
		nil,
		{chunkVersion, 0},
		append([]byte{2, 0}, valid[2:]...),
		append([]byte{chunkVersion, 2}, valid[2:]...),
		append(append([]byte(nil), valid...), 0),
	}
	tooMany := append([]byte{chunkVersion, 0}, make([]byte, 0, 17)...)
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], materializeAt)
	tooMany = append(tooMany, b[:n]...)
	tooMany = append(tooMany, make([]byte, 16)...)
	cases = append(cases, tooMany)
	for i, input := range cases {
		before := c
		if err := decodeChunk(input, &c); err == nil {
			t.Fatalf("case %d accepted invalid input", i)
		}
		assertChunkEqual(t, &before, &c)
	}
}

func TestChunkMergerDirectionsAndInputOwnership(t *testing.T) {
	base := chunk{}
	base.set(5, points.Point{Timestamp: 100, Value: 1})
	older := delta(5, points.Point{Timestamp: 200, Value: 2})
	newer := delta(5, points.Point{Timestamp: 1, Value: 3})

	baseBytes, olderBytes, newerBytes := encodeChunk(&base), encodeChunk(&older), encodeChunk(&newer)
	m, err := chunkMerger.Merge(nil, baseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MergeNewer(olderBytes); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeNewer(newerBytes); err != nil {
		t.Fatal(err)
	}
	for i := range baseBytes {
		baseBytes[i] ^= 0xff
	}
	for i := range olderBytes {
		olderBytes[i] ^= 0xff
	}
	for i := range newerBytes {
		newerBytes[i] ^= 0xff
	}
	got := finish(t, m)
	if got.Deltas != 2 || got.Points[5].Value != 3 || got.Points[5].Timestamp != 1 {
		t.Fatalf("newest mutation did not win: %#v", got)
	}

	m, err = chunkMerger.Merge(nil, encodeChunk(&newer))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MergeOlder(encodeChunk(&older)); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeOlder(encodeChunk(&base)); err != nil {
		t.Fatal(err)
	}
	got = finish(t, m)
	if got.Deltas != 2 || got.Points[5].Value != 3 {
		t.Fatalf("MergeOlder did not retain newer point: %#v", got)
	}
}

func TestChunkMergerPartialGroupingsAreAssociative(t *testing.T) {
	a := delta(1, points.Point{Timestamp: 1, Value: 1})
	b := delta(1, points.Point{Timestamp: 2, Value: 2})
	c := delta(2, points.Point{Timestamp: 3, Value: 3})

	ab := mergeNewer(t, &a, &b)
	abc := mergeNewer(t, &ab, &c)
	bc := mergeNewer(t, &b, &c)
	grouped := mergeNewer(t, &a, &bc)
	assertChunkEqual(t, &abc, &grouped)
	if abc.Deltas != 3 || abc.Points[1].Value != 2 || abc.Points[2].Value != 3 {
		t.Fatalf("unexpected merged data: %#v", abc)
	}
}

func TestChunkMergerRejectsMaterializationOverflow(t *testing.T) {
	a := delta(1, points.Point{Timestamp: 1, Value: 1})
	a.Deltas = materializeAt - 1
	b := delta(2, points.Point{Timestamp: 2, Value: 2})
	m, err := chunkMerger.Merge(nil, encodeChunk(&a))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MergeNewer(encodeChunk(&b)); err == nil {
		t.Fatal("accepted too many deltas")
	}
}

func FuzzDecodeChunk(f *testing.F) {
	var c chunk
	c.set(0, points.Point{Timestamp: 0, Value: 0})
	f.Add(encodeChunk(&c))
	f.Add([]byte{chunkVersion, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		var got chunk
		if err := decodeChunk(input, &got); err == nil {
			encoded := encodeChunk(&got)
			var roundTrip chunk
			if err := decodeChunk(encoded, &roundTrip); err != nil {
				t.Fatalf("valid decode did not round-trip: %v", err)
			}
			assertChunkEqual(t, &got, &roundTrip)
		}
	})
}

func FuzzChunkMergeGroupings(f *testing.F) {
	f.Add([]byte{1, 0, 1, 2, 3})
	f.Add([]byte{31, 127, 0xff, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, seed []byte) {
		count := int(fuzzByte(seed, 0)%uint8(materializeAt-1)) + 1
		operands := make([]chunk, count)
		var want chunk
		want.Deltas = uint32(count)
		for i := range operands {
			slot := int(fuzzByte(seed, 1+i*3)) % chunkSlots
			p := points.Point{
				Timestamp: int64(fuzzUint64(seed, 2+i*7)),
				Value:     math.Float64frombits(fuzzUint64(seed, 6+i*11)),
			}
			operands[i] = delta(slot, p)
			// The reference deliberately uses write order rather than timestamp
			// order: newer Pebble operands replace older collisions.
			want.set(slot, p)
		}

		forward := mergeOperandSequence(t, operands, false)
		assertChunkEqual(t, &want, &forward)

		backward := mergeOperandSequence(t, operands, true)
		assertChunkEqual(t, &want, &backward)

		if count > 1 {
			split := 1 + int(fuzzByte(seed, 3)%uint8(count-1))
			left := mergeOperandSequence(t, operands[:split], false)
			right := mergeOperandSequence(t, operands[split:], false)
			grouped := mergeNewer(t, &left, &right)
			assertChunkEqual(t, &want, &grouped)
		}
	})
}

func mergeOperandSequence(t *testing.T, operands []chunk, older bool) chunk {
	t.Helper()
	if older {
		m, err := chunkMerger.Merge(nil, encodeChunk(&operands[len(operands)-1]))
		if err != nil {
			t.Fatal(err)
		}
		for i := len(operands) - 2; i >= 0; i-- {
			if err := m.MergeOlder(encodeChunk(&operands[i])); err != nil {
				t.Fatal(err)
			}
		}
		return finish(t, m)
	}
	m, err := chunkMerger.Merge(nil, encodeChunk(&operands[0]))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(operands); i++ {
		if err := m.MergeNewer(encodeChunk(&operands[i])); err != nil {
			t.Fatal(err)
		}
	}
	return finish(t, m)
}

func fuzzByte(seed []byte, offset int) byte {
	if len(seed) == 0 {
		return 0
	}
	return seed[offset%len(seed)]
}

func fuzzUint64(seed []byte, offset int) uint64 {
	var value uint64
	for i := 0; i < 8; i++ {
		value |= uint64(fuzzByte(seed, offset+i)) << (8 * i)
	}
	return value
}

func delta(slot int, p points.Point) chunk {
	var c chunk
	c.Deltas = 1
	c.set(slot, p)
	return c
}

func mergeNewer(t *testing.T, first, second *chunk) chunk {
	t.Helper()
	m, err := chunkMerger.Merge(nil, encodeChunk(first))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MergeNewer(encodeChunk(second)); err != nil {
		t.Fatal(err)
	}
	return finish(t, m)
}

func finish(t *testing.T, merger pebble.ValueMerger) chunk {
	t.Helper()
	data, closer, err := merger.Finish(false)
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		defer func() {
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
		}()
	}
	var c chunk
	if err := decodeChunk(data, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func assertChunkEqual(t *testing.T, want, got *chunk) {
	t.Helper()
	if want.Deltas != got.Deltas || want.Present != got.Present {
		t.Fatalf("chunk header differs: want %#v got %#v", want, got)
	}
	for slot := 0; slot < chunkSlots; slot++ {
		if !want.has(slot) {
			continue
		}
		if want.Points[slot].Timestamp != got.Points[slot].Timestamp || math.Float64bits(want.Points[slot].Value) != math.Float64bits(got.Points[slot].Value) {
			t.Fatalf("slot %d differs: want %#v got %#v", slot, want.Points[slot], got.Points[slot])
		}
	}
}
