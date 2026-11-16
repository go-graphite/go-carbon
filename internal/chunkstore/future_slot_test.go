package chunkstore

import (
	"context"
	"fmt"
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

// UpdateMany follows classic Whisper's batch semantics: unlike Update, it
// accepts future points, whose circular slots replace the previous epoch.
// Explicit archive writes keep the import/repair preference tested above.
func TestFutureSlotAutomaticChunkWrites(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact=%t", exact), func(t *testing.T) {
			s, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: exact, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, s)
			ctx := context.Background()
			cfg := MetricConfig{Name: "future.automatic", Retentions: []Retention{{Step: 1, Count: 60}}, AggregationMethod: Average}
			if _, err := s.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			valid, future := int64(now-50), int64(now+10)
			// Exercise a protected write before the automatic write, including
			// reuse of the pooled writer's operation-specific state.
			for _, p := range []Point{{Timestamp: valid, Value: 7}, {Timestamp: future, Value: 3}} {
				if err := s.UpdateManyForArchive(ctx, cfg.Name, []Point{p}, 60); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.UpdateMany(ctx, cfg.Name, []Point{{Timestamp: future, Value: 9}}); err != nil {
				t.Fatal(err)
			}
			got, err := s.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Archives[0].Points) != 1 || got.Archives[0].Points[0] != (Point{Timestamp: future, Value: 9}) {
				t.Fatalf("automatic future write did not replace the old circular owner: %+v", got.Archives[0].Points)
			}
			visible, err := s.Fetch(ctx, cfg.Name, int(valid-1), int(valid))
			if err != nil || visible == nil || len(visible.Values) != 1 || !math.IsNaN(visible.Values[0]) {
				t.Fatalf("classic would hide the replaced epoch: %+v, %v", visible, err)
			}
		})
	}
}
