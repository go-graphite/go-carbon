package whisperio

import (
	"context"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSpliceLongRunsAcrossHolesAndWrap(t *testing.T) {
	r := store.Retention{Step: 60, Count: 307}
	points := make([]store.Point, 512)
	for i := range points {
		if i >= 100 && i < 105 || i == 300 {
			continue
		}
		points[i] = store.Point{Timestamp: 1790000040 + int64(i)*60, Value: float64(i)}
		if i >= 200 {
			points[i].Timestamp += 60
		}
	}
	payload := &splicePayload{}
	sequence := 0
	fragments, ok := appendSplicePoints(nil, points, payload, 0, r, &sequence)
	if !ok || len(fragments) != 4 || len(payload.points) != 0 {
		t.Fatalf("fragments=%d retained=%d ok=%t", len(fragments), len(payload.points), ok)
	}
	want := map[int]int64{}
	for _, p := range points {
		if p.Timestamp > 0 {
			slot := int(p.Timestamp / 60 % int64(r.Count))
			want[slot] = max(want[slot], p.Timestamp)
		}
	}
	parts, _, _, err := selectSpliceOwners(context.Background(), fragments, nil, r)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, part := range parts {
		for i := 0; i < part.count; i++ {
			slot := part.slot + i
			ts := spliceOrdinalTime(&part.fragment, part.ordinal+i, 60)
			if seen[slot] || want[slot] != ts || points[part.ordinal+i].Timestamp != ts {
				t.Fatal(slot, ts, want[slot])
			}
			seen[slot] = true
		}
	}
	if len(seen) != len(want) {
		t.Fatal(len(seen), len(want))
	}
	// Dense intervals must remain valid after the borrowed decoder is reused.
	clear(points)
	for _, f := range fragments {
		if f.first <= 0 || f.irregular {
			t.Fatal(f)
		}
	}
}
