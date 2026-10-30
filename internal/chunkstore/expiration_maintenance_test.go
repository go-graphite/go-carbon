package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestPhysicalMaintenanceFencesConditionalExpiration(t *testing.T) {
	for _, repair := range []bool{false, true} {
		name := "relayout"
		if repair {
			name = "sidecar"
		}
		t.Run(name, func(t *testing.T) {
			var codec ContinuationCodec = relayoutCodec{}
			if repair {
				codec = legacySidecarCodec{}
			}
			s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: !repair, Continuation: codec})
			defer closeTestStore(t, s)
			ctx := context.Background()
			old := createTestMetric(t, s, "expired")
			if repair {
				if _, err := s.inspectSidecar(ctx, old.Name, legacySidecarCodec{}, true); err != nil {
					t.Fatal(err)
				}
			} else {
				plan, err := (relayoutCodec{}).RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, old))
				if err != nil {
					t.Fatal(err)
				}
				if err := s.relayoutGroup(ctx, []relayoutItem{{m: old, plan: plan}}); err != nil {
					t.Fatal(err)
				}
			}
			fresh, err := s.Metadata(ctx, old.Name)
			if err != nil || fresh.Revision != old.Revision+1 || !fresh.LastUpdate.Equal(old.LastUpdate) {
				t.Fatalf("maintenance fence/activity: %+v %v", fresh, err)
			}
			outcomes, err := s.DeleteBatchIfUnchanged(ctx, []Metadata{old})
			if err != nil || len(outcomes) != 1 || !errors.Is(outcomes[0], ErrConflict) {
				t.Fatalf("stale expiration: %v %v", outcomes, err)
			}
		})
	}
}
