package carbon

import (
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestV2CatalogUpdatesDeduplicateAndRecreate(t *testing.T) {
	u := newV2CatalogUpdates(1)
	raw, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "name.metric", Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
	if err != nil {
		t.Fatal(err)
	}
	batches := []v2core.Batch{{ID: 7, Policy: raw, Clock: 1700000000}}
	u.visible(0, batches)
	u.visible(0, batches)
	add, stamps, remove := u.drain()
	if !slices.Equal(add, []string{"name.metric"}) || len(remove) != 0 {
		t.Fatal(add, remove)
	}
	if len(stamps) != 1 || stamps[0].registered != 1700000000 || stamps[0].visible == 0 {
		t.Fatal(stamps)
	}
	u.expire(7, "name.metric")
	u.visible(0, batches)
	u.visible(0, batches)
	add, _, remove = u.drain()
	if !slices.Equal(add, []string{"name.metric"}) || !slices.Equal(remove, add) {
		t.Fatal(add, remove)
	}
}

func TestV2CatalogUpdatesRetry(t *testing.T) {
	u := newV2CatalogUpdates(1)
	u.changed([]string{"copied.name"})
	added, stamps, changed := u.drain()
	u.retry(append(added, "new.name"), stamps, changed)
	added, stamps, changed = u.drain()
	if !slices.Equal(added, []string{"new.name"}) || !slices.Equal(changed, []string{"copied.name"}) || len(stamps) != 1 || stamps[0].visible != 0 {
		t.Fatal(added, stamps, changed)
	}
}

// Every published new name keeps its registration, reader-visible and
// catalog-published times; reconfirmed names without stamps are skipped.
func TestV2CatalogTelemetryStamps(t *testing.T) {
	var c v2CatalogTelemetry
	now := time.Now()
	c.published(now, []string{"a.b", "copied"}, []v2NameStamp{{registered: now.Unix() - 1, visible: now.UnixNano()}, {}}, 2, 5)
	r := c.report()
	recent := r["recent"].([]v2NameTrace)
	if len(recent) != 1 || recent[0].Name != "a.b" || recent[0].PublishedUnixMs < recent[0].ReaderVisibleUnixMs || r["publications"] != uint64(1) || r["levels"] != 2 || r["names"] != 5 {
		t.Fatal(r)
	}
	if h := r["reader_visible_to_published"].(map[string]interface{}); h["count"] != uint64(1) {
		t.Fatal(h)
	}
}
