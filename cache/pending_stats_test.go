package cache

import "testing"

func TestPendingRecoveryStatsFollowClaimsRetriesAndConfirmations(t *testing.T) {
	c := New()
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	check := func(want PendingRecoveryStats) {
		t.Helper()
		got, ok := c.PendingRecoveryStats()
		if !ok || got != want {
			t.Fatalf("stats %+v/%v, want %+v", got, ok, want)
		}
	}
	check(PendingRecoveryStats{Unclaimed: 2})
	if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
		t.Fatal(err)
	}
	check(PendingRecoveryStats{Confirmations: 2, Urgent: 2})
	p, _ := c.PopNotConfirmed("metric")
	c.Requeue(p)
	check(PendingRecoveryStats{Confirmations: 2, Urgent: 2})
	p, _ = c.PopNotConfirmed("metric")
	c.Confirm(p)
	check(PendingRecoveryStats{Confirmations: 1, Urgent: 1})
	p, _ = c.PopNotConfirmed("other")
	c.Confirm(p)
	check(PendingRecoveryStats{})
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
	check(PendingRecoveryStats{})
}

func TestPendingRecoveryStatsNeverWaitForConfirmation(t *testing.T) {
	c := New()
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	r := c.pending.Load()
	r.writes[0].mu.Lock()
	defer r.writes[0].mu.Unlock()
	if _, ok := c.PendingRecoveryStats(); ok {
		t.Fatal("busy confirmation snapshot reported complete")
	}
}
