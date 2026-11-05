package whisperio

import (
	"context"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestColdPackStoredPointsRecomputation(t *testing.T) {
	db, err := store.Open(t.TempDir(), coldOptions(&ContinuationCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	coldFixture(t, db)
	// Empty metrics count toward coverage even though they have no descriptors.
	if _, err := db.Create(context.Background(), store.MetricConfig{Name: "empty", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		records, err := db.ExportTransfer(context.Background(), []string{"cold.metric", "empty"})
		if err != nil {
			t.Fatal(err)
		}
		var points uint64
		for _, record := range records {
			for _, kv := range record.Keys {
				if len(kv.Key) == 16 && kv.Key[0] == 'f' && kv.Key[9] == 'b' {
					desc, err := decodeBlockDescriptor(kv.Value)
					if err != nil {
						t.Fatal(err)
					}
					points += uint64(desc.count)
				}
			}
		}
		if st := db.ColdPackStats(); points == 0 || st.StoredPoints != points || st.StoredMetrics != 2 {
			t.Fatalf("full recomputation=%d, sweep=%+v", points, st)
		}
	}
	// Hot ranges are counted even when the pass rewrites nothing.
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: 100 * 365 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunColdPack(context.Background()); err != nil {
		t.Fatal(err)
	}
	check()
	if db.ColdPackStats().Blocks != 0 {
		t.Fatal("hot count rewrote ranges")
	}
	for _, mode := range []string{"exact", "exact", "decimal", "unpack"} {
		coldSweep(t, db, mode)
		check() // ordinary, sealed, already sealed, and thawed descriptors
	}
}
