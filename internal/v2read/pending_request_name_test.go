package v2read

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A sealed ID identity has released its admission bytes. Reusing a request
// name must not allocate the reverse dictionary name for each pending batch.
func TestPendingRequestNameDoesNotReconstruct(t *testing.T) {
	v2part.IDKeys.Store(true)
	t.Cleanup(func() { v2part.IDKeys.Store(false) })
	r, _ := New(1, 1<<28, 1<<20)
	r.Identities = &v2core.PolicyCatalog{}
	cfg := testConfig()
	cfg.SealInterval = time.Hour
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, IDKeys: true, PublishPending: r.PublishPending, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	name := strings.Repeat("pending.identity.", 20) + "metric"
	policy := testPolicy(t, name)
	b := v2core.Batch{ID: 1, Policy: policy, Order: 1, Clock: 1700000003, Points: []v2core.Row{{ID: 1, Time: 1700000001, Bits: math.Float64bits(1), Seq: 1}}}
	if err = e.AppendBatches(0, []v2core.Batch{b}, 1, true); err != nil {
		t.Fatal(err)
	}
	g := pinOwner(t, r, 0)
	defer g.release()
	names := []string{name}
	index, err := g.parts.SnapshotForNames(context.Background(), g.snapshot, names, g.cache.ranges)
	if err != nil {
		t.Fatal(err)
	}
	h := e.ApplicationPolicy(1)
	var got string
	if baseline := testing.AllocsPerRun(10, func() { got = h.Name() }); baseline == 0 || got != name {
		t.Fatal("fixture did not require reverse dictionary reconstruction")
	}
	allocs := testing.AllocsPerRun(100, func() { got, err = pendingRequestName(index, h, nil, names) })
	if err != nil || got != name || allocs != 0 {
		t.Fatalf("request name: got %q err %v allocations %g, want the supplied name without allocation", got, err, allocs)
	}
}
