package carbon

import (
	"fmt"
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
	u.visible(batches)
	u.visible(batches)
	add, stamps, remove, _ := u.drain(0)
	if !slices.Equal(add, []string{"name.metric"}) || len(remove) != 0 {
		t.Fatal(add, remove)
	}
	if len(stamps) != 1 || stamps[0].registered != 1700000000 || stamps[0].visible == 0 {
		t.Fatal(stamps)
	}
	u.expire(7, "name.metric")
	u.visible(batches)
	u.visible(batches)
	add, _, remove, _ = u.drain(0)
	if !slices.Equal(add, []string{"name.metric"}) || !slices.Equal(remove, add) {
		t.Fatal(add, remove)
	}
}

func TestV2CatalogUpdatesRetry(t *testing.T) {
	u := newV2CatalogUpdates(1)
	u.changed([]string{"copied.name"})
	added, stamps, changed, _ := u.drain(0)
	u.retry(append(added, "new.name"), stamps, changed)
	added, stamps, changed, _ = u.drain(0)
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

func TestV2CatalogUpdatesDrainChunks(t *testing.T) {
	u := newV2CatalogUpdates(1)
	var want []string
	for i := range 10 {
		want = append(want, fmt.Sprintf("a.%d", i))
	}
	u.retry(want, nil, nil)
	var got []string
	for {
		add, stamps, _, more := u.drain(3)
		if len(add) > 3 || len(stamps) != len(add) {
			t.Fatalf("chunk %d names %d stamps", len(add), len(stamps))
		}
		// Callers append confirmed names; that must not clobber the backlog.
		_ = append(add, "x")
		got = append(got, add...)
		if !more {
			break
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
