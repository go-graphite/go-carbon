package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestPendingScanWaitsOnlyForNewNames(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		c := New()
		attach := c.AttachPendingRecovery
		if prepared {
			attach = c.AttachPendingRecoveryWithPreparedNames
		}
		if err := attach(pendingFixtureWithKnown(t, true)); err != nil {
			t.Fatal(err)
		}
		if c.PendingScanOutstanding() != 2 {
			t.Fatal("scan may not precede loading history")
		}
		if err := c.RecoverPending(nil, 0); err != nil {
			t.Fatal(err)
		}
		want := uint64(2)
		if prepared {
			want = 1
		}
		if c.PendingScanOutstanding() != want {
			t.Fatalf("prepared=%t: scan pending=%d, want %d", prepared, c.PendingScanOutstanding(), want)
		}
		// A failed new-name batch keeps its gate ownership across merging a newer
		// correction. Confirming that merged batch must release it exactly once.
		p, ok := c.PopNotConfirmed("other")
		if !ok {
			t.Fatal("new history missing")
		}
		c.Add(points.OnePoint("other", 9, 2))
		c.Requeue(p)
		if c.PendingScanOutstanding() != want {
			t.Fatal("retry lost scan ownership")
		}
		p, ok = c.PopNotConfirmed("other")
		if !ok {
			t.Fatal("merged retry missing")
		}
		c.Confirm(p)
		if c.PendingScanOutstanding() != want-1 {
			t.Fatal("confirmation retained scan ownership")
		}
		if c.PendingOutstanding() != 1 || c.PendingUrgentOutstanding() != 1 {
			t.Fatal("existing history lost persistence ownership")
		}
		if err := c.FinishPendingRecovery(); err == nil {
			t.Fatal("scan readiness retired unpersisted history")
		}
		persist(c, []string{"metric"})
		if c.PendingScanOutstanding() != 0 {
			t.Fatal("confirmed history holds scan")
		}
		if err := c.FinishPendingRecovery(); err != nil {
			t.Fatal(err)
		}
	}
}
