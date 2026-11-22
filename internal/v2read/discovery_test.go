package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestDiscoveryParallelOwners(t *testing.T) {
	r, err := New(4, 256<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	cfg := testConfig()
	cfg.Shards, cfg.Owners = 4, 4
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, PublishPending: r.PublishPending})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	var want []string
	var batches []v2core.Batch
	for i := range 64 {
		name := fmt.Sprintf("discovery.metric.%03d", i)
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
		if err != nil {
			t.Fatal(err)
		}
		id := uint64(i + 1)
		want = append(want, name)
		batches = append(batches, v2core.Batch{ID: id, Order: id, Clock: 1000, Policy: policy, Points: []v2core.Row{{ID: id, Time: 990, Bits: id, Seq: id}}})
	}
	if err := e.AppendBatches(0, batches, 1, true); err != nil {
		t.Fatal(err)
	}
	// The first call loads all four sealed directories. The expected catalog is
	// independent of their physical order and of the discovery implementation.
	got, masks, err := r.NamesWithMasks(context.Background())
	if err != nil || !slices.Equal(got, want) || len(masks) != 0 {
		t.Fatalf("names=%v masks=%v err=%v", got, masks, err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 8 {
				got, _, err := r.NamesWithMasks(context.Background())
				if err != nil || !slices.Equal(got, want) {
					t.Error("concurrent discovery differs", err)
					return
				}
			}
		})
	}
	wg.Wait()
	for i := range r.caches {
		r.caches[i].mu.Lock()
		retained := r.caches[i].index != nil
		r.caches[i].mu.Unlock()
		if retained {
			t.Fatalf("owner %d retains an all-name directory after enumeration", i)
		}
	}
	// Releasing the discovery directory preserves the registered names and
	// the sealed values consumed by subsequent request-local readers.
	values, err := fetchAll(r, want, 989, 992, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range want {
		if !slices.Contains(values[name].bits, uint64(i+1)) {
			t.Fatalf("name %s lost its exact sealed value after enumeration", name)
		}
	}
	t.Run("pending-does-not-wait-for-discovery", func(t *testing.T) {
		// A released directory must not put fresh-name publication or reads
		// behind another whole-store sweep. Hold every sweep gate throughout.
		for i := range r.caches {
			<-r.caches[i].loadGate
			defer func() { r.caches[i].loadGate <- struct{}{} }()
		}
		const name = "discovery.fresh"
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
		if err != nil {
			t.Fatal(err)
		}
		b := v2core.Batch{ID: 65, Order: 65, Clock: 1000, Policy: policy, Points: []v2core.Row{{ID: 65, Time: 990, Bits: 65, Seq: 65}}}
		if err := e.AppendBatches(0, []v2core.Batch{b}, 2, false); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		await(t, func() bool {
			_, err := r.Metadata(ctx, name)
			return err == nil || ctx.Err() != nil
		})
		err = r.FetchMany(ctx, []string{name}, 989, 992, 1000, func(_ string, s *Series) error {
			if got := math.Float64bits(s.Values[(990-s.From)/s.Step]); got != 65 {
				t.Fatalf("fresh pending bits=%x want=41", got)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := r.NamesWithMasks(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	r.Close()
	if got, _, err := r.NamesWithMasks(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("closed reader: %v %v", got, err)
	}
}
