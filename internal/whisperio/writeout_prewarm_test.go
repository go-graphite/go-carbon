package whisperio

import (
	"context"
	"fmt"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteoutPrewarmMatchesUncachedUpdates(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_790_000_000, 0)
	root := t.TempDir()
	clock := func() time.Time { return now }
	plain := stateCacheStore(t, filepath.Join(root, "plain"), true, 0, clock)
	cached := stateCacheStore(t, filepath.Join(root, "cached"), true, 16<<20, clock)
	defer plain.Close()
	defer cached.Close()
	names := make([]string, 48)
	for i := range names {
		names[i] = fmt.Sprintf("writeout.m%02d", i)
		for _, db := range []*store.Store{plain, cached} {
			if _, err := db.Create(ctx, prewarmConfig(names[i])); err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateMany(ctx, names[i], []store.Point{{Timestamp: now.Unix() - 1, Value: 1}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for round := range 6 {
		now = now.Add(5 * time.Second)
		cached.ForgetMetadataCacheForTest()
		cached.PrewarmWriteout(ctx, names)
		for i, name := range names {
			if !cached.MetadataCached(name) {
				t.Fatal("prefetch did not prepare", name)
			}
			input := []store.Point{{Timestamp: now.Unix(), Value: float64(i + round)}}
			if i%3 == 0 {
				input = append(input, store.Point{Timestamp: now.Unix() - 50, Value: float64(round)})
			}
			for _, db := range []*store.Store{plain, cached} {
				if err := db.UpdateMany(ctx, name, input); err != nil {
					t.Fatal(err)
				}
			}
			got, err := cached.Fetch(ctx, name, int(now.Unix())-4000, int(now.Unix()))
			if err != nil {
				t.Fatal(err)
			}
			want, err := plain.Fetch(ctx, name, int(now.Unix())-4000, int(now.Unix()))
			if err != nil {
				t.Fatal(err)
			}
			if !sameCachedSeries(got, want) {
				t.Fatal("prefetched update differs from uncached oracle", name, round)
			}
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	cached.PrewarmWriteout(cancelled, names)
}
