package whisperio

import (
	"context"
	"math/rand"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSpliceMaskOwnersAgainstPointUnion(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		random := rand.New(rand.NewSource(seed))
		r := store.Retention{Step: 60, Count: []int{130, 307, 1000}[seed%3]}
		var sources [2][]spliceFragment
		type owner struct {
			timestamp          int64
			priority, sequence int
			payload            *splicePayload
			ordinal            int
		}
		want := map[int]owner{}
		for side := range sources {
			for sequence := 1; sequence <= 16; sequence++ {
				count := random.Intn(600) + 1
				start := int64(1790000000/60+random.Intn(1200)) * 60
				points := make([]store.Point, count)
				irregular := sequence%3 == 0
				for i := range points {
					ts := start + int64(i)*60
					if irregular {
						ts += int64(i/3) * 60
					}
					points[i] = store.Point{Timestamp: ts, Value: float64(sequence*1000 + i)}
				}
				payload := &splicePayload{points: points}
				f := spliceFragment{first: start, count: count, priority: side * 2, sequence: sequence, payload: payload, irregular: irregular}
				sources[side] = append(sources[side], f)
				for ordinal, p := range points {
					slot := int(p.Timestamp / 60 % int64(r.Count))
					old, exists := want[slot]
					if !exists || p.Timestamp > old.timestamp || p.Timestamp == old.timestamp && (f.priority > old.priority || f.priority == old.priority && f.sequence >= old.sequence) {
						want[slot] = owner{p.Timestamp, f.priority, f.sequence, payload, ordinal}
					}
				}
			}
		}
		parts, base, _, err := selectSpliceOwners(context.Background(), sources[0], sources[1], r)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int]bool{}
		var oldest int64
		for _, part := range parts {
			for i := 0; i < part.count; i++ {
				slot := part.slot + i
				ordinal := part.ordinal + i
				timestamp := spliceOrdinalTime(&part.fragment, ordinal, r.Step)
				expected := want[slot]
				if seen[slot] || timestamp != expected.timestamp || part.fragment.payload != expected.payload || ordinal != expected.ordinal {
					t.Fatalf("seed=%d slot=%d timestamp=%d want=%+v duplicate=%t", seed, slot, timestamp, expected, seen[slot])
				}
				seen[slot] = true
				if oldest == 0 || timestamp < oldest {
					oldest = timestamp
				}
			}
		}
		if len(seen) != len(want) || base != oldest {
			t.Fatalf("seed=%d got %d base %d, want %d base %d", seed, len(seen), base, len(want), oldest)
		}
	}
}
