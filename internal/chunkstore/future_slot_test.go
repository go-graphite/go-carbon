package chunkstore

import (
	"context"
	"math"
	"testing"
)

func TestFutureSlotTimestampPrecedence(t *testing.T) {
	r := Retention{Step: 3600, Count: 216}
	const now int64 = 1794759182
	const valid int64 = 1794042000
	const future int64 = 4026531600
	for _, c := range []struct {
		name               string
		candidate, current int64
		want               bool
	}{
		{"repair", valid, future, true}, {"protect", future, valid, false},
		{"expired", valid - 3600*216, future, false}, {"boundary", now - 3600*216, future, false},
		{"newer", valid + 3600, valid, true}, {"older", valid, valid + 3600, false},
		{"equal", valid, valid, true}, {"unrelated future", future + 3600*216, future, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := PreferArchiveTimestamp(c.candidate, c.current, r, now); got != c.want {
				t.Fatal(got)
			}
		})
	}
	points := []Point{{Timestamp: valid, Value: math.Copysign(0, -1)}, {Timestamp: valid + 3600, Value: 1.0 / 3}, {Timestamp: future, Value: 0}, {Timestamp: future + 7200, Value: 7}}
	got := retainTransferSlotsAt(points, r, now)
	if len(got) != 3 || got[0].Timestamp != valid || math.Float64bits(got[0].Value) != math.Float64bits(math.Copysign(0, -1)) || got[2].Timestamp != future+7200 {
		t.Fatal(got)
	}
}

func TestFutureSlotOrdinaryChunkWrites(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	ctx := context.Background()
	cfg := MetricConfig{Name: "future.write", Retentions: []Retention{{Step: 60, Count: 120}}, AggregationMethod: Average}
	if _, err := s.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	valid := int64(now/60*60 - 60)
	future := valid + 60*120*100
	for _, p := range []Point{{Timestamp: future, Value: 0}, {Timestamp: valid, Value: math.Copysign(0, -1)}, {Timestamp: future, Value: 3}} {
		if err := s.UpdateManyForArchive(ctx, cfg.Name, []Point{p}, 60*120); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 1 || got.Archives[0].Points[0].Timestamp != valid || math.Float64bits(got.Archives[0].Points[0].Value) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal(got)
	}
}
