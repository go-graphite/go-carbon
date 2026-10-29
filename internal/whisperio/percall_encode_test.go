package whisperio

import (
	"context"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestPerCallPointBufferByteParity(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
	schemas := append([]struct {
		name       string
		retentions []store.Retention
	}(nil), percallSchemas...)
	for _, schema := range productionSchemas {
		schemas = append(schemas, struct {
			name       string
			retentions []store.Retention
		}{schema.name, schema.retentions})
	}
	for _, schema := range schemas {
		t.Run(schema.name, func(t *testing.T) {
			now := time.Unix(1790000000, 0)
			clock := func() time.Time { return now }
			m := store.Metadata{MetricConfig: store.MetricConfig{Name: "parity", Retentions: schema.retentions, AggregationMethod: store.Average, XFilesFactor: .5}}
			plain := &appendTailKV{data: make(map[string][]byte)}
			if err := codec.Create(ctx, plain, m, clock); err != nil {
				t.Fatal(err)
			}
			buffered := plain.clone()
			step := int64(schema.retentions[0].Step)
			base := now.Unix() / step * step
			for round := range 16 {
				stamp := base + int64(round+1)*210*step
				now = time.Unix(stamp+step, 0)
				input := make([]store.Point, 210)
				for i := range input {
					input[i] = store.Point{Timestamp: stamp - int64(210-i)*step, Value: float64((round+i)%19) + .25}
				}
				if round%3 == 2 {
					input = append(input, store.Point{Timestamp: stamp - 90*step, Value: math.Copysign(0, -1)}, store.Point{Timestamp: stamp - 90*step, Value: 1.75})
				}
				coalescePointWrites = false
				err := codec.Update(ctx, plain, m, input, -1, clock, nil)
				coalescePointWrites = true
				if err != nil {
					t.Fatal(err)
				}
				if err := codec.Update(ctx, buffered, m, input, -1, clock, nil); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(plain.data, buffered.data) {
					t.Fatalf("round %d encoded state differs", round)
				}
			}
		})
	}
}

func TestPerCallTailSlotsAvoidReads(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1790000000, 0)
	db := stateCacheStore(t, t.TempDir(), true, 128<<20, func() time.Time { return now })
	defer db.Close()
	const name = "tail.hot"
	if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: percallSchemas[3].retentions, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	for round := range 24 {
		now = now.Add(32 * 300 * time.Second)
		input := make([]store.Point, 32)
		for i := range input {
			input[i] = store.Point{Timestamp: now.Unix() - int64(32-i)*300, Value: float64(round + i)}
		}
		reads := store.CountStoreReadsForTest()
		if err := db.UpdateMany(ctx, name, input); err != nil {
			t.Fatal(err)
		}
		count := reads()
		if round > 0 && count != 0 {
			t.Fatalf("round %d: %d LSM reads for a known tail", round, count)
		}
	}
	stats := db.Stats().StateCache
	if stats.Misses != 0 || stats.Evictions != 0 {
		t.Fatalf("cache %+v", stats)
	}
	t.Logf("24 rounds, 32 points/update, zero steady reads; cache bytes/metric=%d", stats.Bytes)
}
