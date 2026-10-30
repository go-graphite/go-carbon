package whisperio

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Generate a time-bucketed image from ten hours of secondly samples and three
// propagated archives. The store fixture replicates these real physical keys.
func TestPrepareExpirationContentionSeed(t *testing.T) {
	path := os.Getenv("EXPIRATION_CONTENTION_SEED")
	if path == "" {
		t.Skip("set EXPIRATION_CONTENTION_SEED to a JSON seed path")
	}
	const end = 1790000000
	now := time.Unix(end-36000, 0)
	clock := func() time.Time { return now }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "seed", Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5}}
	kv := bucketedMapIO{mapContinuationIO{}}
	codec := &ContinuationCodec{ReuseBlockKeys: true}
	ctx := context.Background()
	if err := codec.Create(ctx, kv, m, clock); err != nil {
		t.Fatal(err)
	}
	for first := end - 36000; first < end; first += 210 {
		count := min(210, end-first)
		now = time.Unix(int64(first+count), 0)
		input := make([]store.Point, count)
		for i := range input {
			input[i] = store.Point{Timestamp: int64(first + i), Value: float64((first + i) % 17)}
		}
		if err := codec.Update(ctx, kv, m, input, -1, clock, nil); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i, archive := range snapshot.Archives {
		if len(archive.Points) == 0 {
			t.Fatalf("empty archive %d", i)
		}
	}
	seed := store.TransferMetric{Config: m.MetricConfig, Continuation: true}
	bytes := 0
	if err := kv.Visit(nil, func(k, v []byte) error {
		seed.Keys = append(seed.Keys, store.RelayoutKV{Key: k, Value: v})
		bytes += len(k) + len(v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("seed: %d physical keys, %d key/value bytes, %d archives", len(seed.Keys), bytes, len(snapshot.Archives))
}
