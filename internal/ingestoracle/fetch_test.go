package ingestoracle

import (
	"context"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type renderFixture struct{ value float64 }

func (f renderFixture) Fetch(context.Context, string, int, int) (*store.Series, error) {
	return &store.Series{FromTime: 999990, Step: 10, Values: []float64{f.value}}, nil
}

func TestFetchOracleRejectsInvisibleStoredSparsePoint(t *testing.T) {
	r := store.Retention{Step: 10, Count: 60}
	s := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "sparse", Retentions: []store.Retention{r}}}, Archives: []store.Archive{{Retention: r, Points: []store.Point{{Timestamp: 999990, Value: math.Copysign(0, -1)}}}}}
	for _, value := range []float64{math.NaN(), 0, 1} {
		if _, _, err := VerifyFetch(context.Background(), renderFixture{value}, s, 1000080); err == nil {
			t.Fatalf("wrong render accepted: %016x", math.Float64bits(value))
		}
	}
	if _, n, err := VerifyFetch(context.Background(), renderFixture{math.Copysign(0, -1)}, s, 1000080); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
