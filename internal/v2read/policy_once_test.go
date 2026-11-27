package v2read

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// TestPendingPolicyCheckedAtAdmission pins where the canonical policy check
// lives: admission rejects a noncanonical definition before any reader sees
// it, and a request reads every admitted batch's policy from its registered
// handle, comparing definition bytes only when the handle changes.
func TestPendingPolicyCheckedAtAdmission(t *testing.T) {
	r, _ := New(1, 1<<28, 1<<20)
	// Maintenance may seal a published view immediately. Capture the admitted
	// batches at publication instead of polling a view with no minimum lifetime.
	published := make(chan []v2core.Batch, 1)
	e, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: func(owner int, pin *pebble.Snapshot, view v2core.PendingView, generation uint64, inventory *v2core.RollupInventory) error {
		if err := r.PublishPending(owner, pin, view, generation, inventory); err != nil {
			return err
		}
		if view.Len() == 3 {
			select {
			case published <- view.Batches():
			default:
			}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer r.Close()
	policy := testPolicy(t, "once.metric")
	row := func(order uint64, v float64) []v2core.Row {
		return []v2core.Row{{ID: 1, Time: 990 + int64(order), Bits: math.Float64bits(v), Seq: order}}
	}
	// Valid JSON, wrong encoding: the catalog oracle rejects it at admission.
	bad := append([]byte("{ "), policy[1:]...)
	if err := e.AppendBatches(0, []v2core.Batch{{ID: 1, Policy: bad, Order: 1, Clock: 1000, Points: row(1, 1)}}, 1, false); err == nil || !strings.Contains(err.Error(), "noncanonical") {
		t.Fatalf("noncanonical policy admitted: %v", err)
	}
	for order := uint64(1); order <= 3; order++ {
		if err := e.AppendBatches(0, []v2core.Batch{{ID: 1, Policy: policy, Order: order, Clock: 1000, Points: row(order, float64(order))}}, order, false); err != nil {
			t.Fatal(err)
		}
	}
	var pending []v2core.Batch
	select {
	case pending = <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("three admitted batches were never published")
	}
	for _, b := range pending {
		if b.Registered == nil || len(b.Policy) != 0 {
			t.Fatalf("published batch carries raw policy: registered=%v policy=%q", b.Registered != nil, b.Policy)
		}
	}
	if err := r.FetchMany(context.Background(), []string{"once.metric"}, 990, 993, 1000, func(_ string, s *Series) error {
		if len(s.Values) != 3 || s.Values[2] != 3 {
			t.Errorf("render %v", s.Values)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	load := func(batches []v2core.Batch) error {
		q := &generation{pending: batches, sealed: &v2core.RollupSnapshot{}, reader: r, cache: &r.caches[0]}
		q.loadPendingContext(context.Background())
		return q.err
	}
	// A detached handle with identical bytes takes the definition compare.
	same, err := v2core.NewApplicationPolicy("once.metric", policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := load(append(pending[:2:2], v2core.Batch{ID: 1, Registered: same, Order: 4, Clock: 1000, Points: row(4, 4)})); err != nil {
		t.Fatalf("equal definition rejected: %v", err)
	}
	// Same name, changed definition: rejected even though the ID matches.
	changed, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "once.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Max, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}})
	if err != nil || bytes.Equal(changed, policy) {
		t.Fatal(err)
	}
	other, err := v2core.NewApplicationPolicy("once.metric", changed)
	if err != nil {
		t.Fatal(err)
	}
	if err := load(append(pending[:2:2], v2core.Batch{ID: 1, Registered: other, Order: 4, Clock: 1000, Points: row(4, 4)})); err == nil || err.Error() != "pending identity/policy mismatch" {
		t.Fatalf("changed definition accepted: %v", err)
	}
	if err := load(append(pending[:2:2], v2core.Batch{ID: 2, Registered: pending[0].Registered, Order: 4, Clock: 1000, Points: row(4, 4)})); err == nil || err.Error() != "pending identity/policy mismatch" {
		t.Fatalf("changed ID accepted: %v", err)
	}
	// Raw policy bytes never reach a reader; if they did, the read fails loudly.
	if err := load([]v2core.Batch{{ID: 1, Policy: policy, Order: 4, Clock: 1000, Points: row(4, 4)}}); err == nil || err.Error() != "pending batch without registered policy" {
		t.Fatalf("unregistered batch read: %v", err)
	}
}
