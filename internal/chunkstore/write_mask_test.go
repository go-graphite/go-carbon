package chunkstore

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"
)

func TestWriteChunkDeltaMatchesPreviousRepresentation(t *testing.T) {
	t.Parallel()
	for _, exact := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "exact"}[exact], func(t *testing.T) {
			w := newChunkWriter(&Store{exactBlocks: exact}, Metadata{MetricConfig: MetricConfig{
				Retentions: []Retention{{Step: 1, Count: 256}, {Step: 60, Count: 256}},
			}})
			previous := make(map[chunkAddress]*chunk)
			for archive := range w.metadata.Retentions {
				for index := range 2 {
					a := chunkAddress{archive, index}
					c := &writeChunk{}
					// Existing values must not leak into the new merge operand.
					for slot := range chunkSlots {
						c.current.set(slot, Point{Timestamp: int64(slot), Value: 123.5})
					}
					w.chunks[a] = c
					previous[a] = &chunk{Deltas: 1}
				}
			}
			check := func() {
				t.Helper()
				for a, c := range w.chunks {
					if !c.dirty {
						continue
					}
					got := c.mergeDelta()
					want := previous[a]
					assertChunkEqual(t, want, &got)
					if !bytes.Equal(w.store.encodeChunk(want), w.store.encodeChunk(&got)) {
						t.Fatal("merge bytes changed from the previous delta representation")
					}
				}
			}
			random := rand.New(rand.NewPCG(42, 17))
			special := []uint64{0, 0x8000000000000000, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042}
			for i := range 4096 {
				archive := i % len(w.metadata.Retentions)
				r := w.metadata.Retentions[archive]
				// Repeated circular slots include old/new epochs and both mask words.
				timestamp := int(random.Uint32() % 4096 * uint32(r.Step))
				if i < 256 {
					timestamp = (i % 128) * r.Step
				}
				valueBits := random.Uint64()
				if i%3 == 0 {
					valueBits = special[(i/3)%len(special)]
				}
				point := Point{Timestamp: int64(timestamp), Value: math.Float64frombits(valueBits)}
				if err := w.setPoint(archive, timestamp, point.Value); err != nil {
					t.Fatal(err)
				}
				slot := timestamp / r.Step % r.Count
				previous[chunkAddress{archive, slot / chunkSlots}].set(slot%chunkSlots, point)
				if i%19 == 0 {
					check()
				}
			}
			check()
		})
	}
}
