package v2daemon

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// TestRegisteredBatchesCarryNameHash checks that producers publish every
// registered batch with its identity's name hash, which readers index pending
// heads by, and that a render finds the batch before any seal.
func TestRegisteredBatchesCarryNameHash(t *testing.T) {
	r, err := v2read.New(2, 16<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Identities = &v2core.PolicyCatalog{}
	var published atomic.Int64
	publish := func(owner int, snap *pebble.Snapshot, pending []v2core.Batch, number uint64) error {
		for i := range pending {
			b := &pending[i]
			if b.Registered == nil || b.NameHash != v2core.NameHash(b.Registered.Name()) {
				t.Errorf("batch %d: registered %v, name hash %x", b.ID, b.Registered != nil, b.NameHash)
			}
		}
		published.Add(int64(len(pending)))
		return r.Publish(owner, snap, pending, number)
	}
	s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 2, Shards: 2, MaxCoreRows: 4096, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: publish}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	resolve := func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}}}})
	}
	owners := make([]*applicationOwner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p: p, resolve: resolve}
	}
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("hash.metric.%d", i)
		if err := owners[v2recv.OwnerFor(names[i], len(owners))].Append(names[i:i+1], []points.Point{{Timestamp: 1000, Value: float64(i) + .5}}, nil, 1010); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range owners {
		if err := o.Drain(); err != nil {
			t.Fatal(err)
		}
	}
	for i, name := range names {
		want := math.Float64bits(float64(i) + .5)
		deadline := time.Now().Add(10 * time.Second)
		for {
			var got []uint64
			err := r.FetchMany(context.Background(), []string{name}, 990, 1010, 1010, func(_ string, s *v2read.Series) error {
				for _, v := range s.Values {
					if !math.IsNaN(v) {
						got = append(got, math.Float64bits(v))
					}
				}
				return nil
			})
			if err == nil && len(got) == 1 && got[0] == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %x err %v, want [%x]", name, got, err, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if published.Load() == 0 {
		t.Fatal("no pending batch was published")
	}
}
