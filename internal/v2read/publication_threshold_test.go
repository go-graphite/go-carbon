package v2read

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Pending publication must survive the old one-second metadata-maintenance
// deadline. Reads evaluate coarse windows from pending applications; only a
// configured seal threshold or inactivity expiry needs to replace that view.
func TestPendingPublicationWaitsForSealThreshold(t *testing.T) {
	r, _ := New(1, 1<<28, 1<<20)
	cfg := testConfig()
	cfg.SealInterval = time.Hour
	cfg.PublishInterval = time.Millisecond
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{PublishPending: r.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer r.Close()
	policy := testPolicy(t, "threshold.metric")
	for order := uint64(1); order <= 3; order++ {
		b := v2core.Batch{ID: 1, Policy: policy, Order: order, Clock: 1000, Points: []v2core.Row{{ID: 1, Time: 990 + int64(order), Bits: math.Float64bits(float64(order)), Seq: order}}}
		if err := e.AppendBatches(0, []v2core.Batch{b}, order, false); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return r.owners[0].Load().view.Len() == int(order) })
	}
	time.Sleep(1200 * time.Millisecond)
	if got := r.owners[0].Load().view.Len(); got != 3 {
		t.Fatalf("metadata maintenance sealed pending rows: got %d, want 3", got)
	}
	check := func() {
		t.Helper()
		if err := r.FetchMany(context.Background(), []string{"threshold.metric"}, 990, 993, 1000, func(_ string, s *Series) error {
			if len(s.Values) != 3 || s.Values[0] != 1 || s.Values[1] != 2 || s.Values[2] != 3 {
				t.Errorf("render %v", s.Values)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check()
	if err := e.AppendBatches(0, nil, 4, true); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return r.owners[0].Load().view.Len() == 0 })
	check()
}
