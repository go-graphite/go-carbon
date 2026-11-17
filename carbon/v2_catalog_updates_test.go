package carbon

import (
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"slices"
	"testing"
)

func TestV2CatalogUpdatesDeduplicateAndRecreate(t *testing.T) {
	u := newV2CatalogUpdates(1)
	raw, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "name.metric", Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
	if err != nil {
		t.Fatal(err)
	}
	batches := []v2core.Batch{{ID: 7, Policy: raw}}
	u.visible(0, batches)
	u.visible(0, batches)
	add, remove := u.drain()
	if !slices.Equal(add, []string{"name.metric"}) || len(remove) != 0 {
		t.Fatal(add, remove)
	}
	u.expire(7, "name.metric")
	u.visible(0, batches)
	u.visible(0, batches)
	add, remove = u.drain()
	if !slices.Equal(add, []string{"name.metric"}) || !slices.Equal(remove, add) {
		t.Fatal(add, remove)
	}
}
