package chunkstore_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Exercise packed continuation state and cached tails, rather than only native
// metadata. Every post-restart update must match a store with no state cache.
func TestMetadataCheckpointContinuationMatchesUncached(t *testing.T) {
	ctx := context.Background()
	const base = int64(1_790_000_000)
	now := time.Unix(base, 0)
	root := t.TempDir()
	open := func(checkpoint bool) *store.Store {
		t.Helper()
		options := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return now }}
		if checkpoint {
			options.MetadataCacheSize, options.MetadataCacheCheckpoint = 8<<20, true
		}
		db, err := store.Open(filepath.Join(root, fmt.Sprint(checkpoint)), options)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	cached, plain := open(true), open(false)
	close := func(db *store.Store) {
		t.Helper()
		if err := db.Close(); err != nil || db.MetadataCheckpointError() != nil {
			t.Fatal(err, db.MetadataCheckpointError())
		}
	}
	config := func(i int) store.MetricConfig {
		return store.MetricConfig{Name: fmt.Sprintf("checkpoint.metric.%02d", i), Retentions: []store.Retention{{Step: 1, Count: 600}, {Step: 10, Count: 360}, {Step: 60, Count: 240}}, AggregationMethod: store.Average, XFilesFactor: .3}
	}
	for i := 15; i >= 0; i-- {
		for _, db := range []*store.Store{cached, plain} {
			if _, err := db.Create(ctx, config(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	update := func(round int) {
		t.Helper()
		now = time.Unix(base+int64(round*20), 0)
		for i := range 16 {
			input := make([]store.Point, 20)
			for j := range input {
				input[j] = store.Point{Timestamp: now.Unix() - int64(j), Value: float64(round*100 + i + j)}
			}
			// Late corrections and overlapping input exercise sidecar state.
			input = append(input, store.Point{Timestamp: base - int64(120+i*60), Value: .5})
			for _, db := range []*store.Store{cached, plain} {
				if err := db.UpdateMany(ctx, config(i).Name, input); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	update(0)
	update(1)
	close(cached)
	close(plain)
	cached, plain = open(true), open(false)
	defer func() { close(cached); close(plain) }()
	stats, err := cached.LoadMetadataCheckpoint(ctx, nil)
	if err != nil || !stats.Valid || stats.Fenced != 16 || stats.Validated != 0 {
		t.Fatal(stats, err)
	}
	for round := 2; round < 5; round++ {
		update(round)
		for i := range 16 {
			got, gotErr := cached.Snapshot(ctx, config(i).Name)
			want, wantErr := plain.Snapshot(ctx, config(i).Name)
			if gotErr != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("round %d metric %d: checkpoint differs from uncached store: %v / %v", round, i, gotErr, wantErr)
			}
		}
	}
}
