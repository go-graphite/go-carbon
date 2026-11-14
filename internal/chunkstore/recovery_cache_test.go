package chunkstore

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Compare ordinary cache admission with the receiver-gating recovery path,
// through SST flushes, compaction, corrections, reenable and process reopen.
func TestDeferredBlockCachePointParity(t *testing.T) {
	ctx := context.Background()
	opts := Options{ExactBlocks: true, CacheSize: 8 << 20, CacheShards: 4, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(1700000000, 0) }}
	dirs := []string{t.TempDir(), t.TempDir()}
	stores := make([]*Store, 2)
	for i := range stores {
		opts.DeferBlockCache = i == 1
		var err error
		stores[i], err = Open(dirs[i], opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func(i int) {
			if stores[i] != nil {
				closeTestStore(t, stores[i])
			}
		}(i)
	}
	bits := []uint64{0, 0x8000000000000000, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0xfff8000000000017}
	rng := rand.New(rand.NewSource(412))
	for n := 0; n < 64; n++ {
		name := fmt.Sprintf("recovery.%d", n)
		for _, s := range stores {
			if _, err := s.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 3600}}, AggregationMethod: Last}); err != nil {
				t.Fatal(err)
			}
		}
	}
	compare := func() {
		t.Helper()
		for n := 0; n < 64; n++ {
			name := fmt.Sprintf("recovery.%d", n)
			a, err := stores[0].Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			b, err := stores[1].Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if len(a.Archives) != len(b.Archives) {
				t.Fatal("archive count")
			}
			for j, x := range a.Archives {
				y := b.Archives[j]
				if x.Retention != y.Retention || len(x.Points) != len(y.Points) {
					t.Fatal("archive shape")
				}
				for k, p := range x.Points {
					q := y.Points[k]
					if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
						t.Fatalf("%s archive%d point%d differs: %#v %#v", name, j, k, p, q)
					}
				}
			}
		}
	}
	for phase := 0; phase < 3; phase++ {
		for round := 0; round < 4; round++ {
			for n := 0; n < 64; n++ {
				input := make([]Point, 32)
				for k := range input {
					v := rng.Uint64()
					if k < len(bits) {
						v = bits[k]
					}
					input[k] = Point{Timestamp: 1699999700 + int64(rng.Intn(300)), Value: math.Float64frombits(v)}
				}
				for _, s := range stores {
					if err := s.UpdateMany(ctx, fmt.Sprintf("recovery.%d", n), input); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		for _, s := range stores {
			if err := s.db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		compare()
		if phase == 0 {
			if stores[1].cache.Size() != 0 || stores[1].rangeDelCache.Size() != 0 {
				t.Fatal("recovery admitted cached SST blocks")
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(stores[1].EnableBlockCache)
			}
			wg.Wait()
			compare()
			if stores[1].cache.Size() == 0 {
				t.Fatal("normal cache admission did not resume")
			}
		}
		if phase == 1 {
			for i, s := range stores {
				if err := s.Compact(); err != nil {
					t.Fatal(err)
				}
				closeTestStore(t, s)
				stores[i] = nil
				opts.DeferBlockCache = false
				var err error
				stores[i], err = Open(dirs[i], opts)
				if err != nil {
					t.Fatal(err)
				}
			}
			compare()
		}
	}
}
