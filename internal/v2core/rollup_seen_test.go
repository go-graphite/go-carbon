package v2core

import (
	"testing"
	"time"
)

// A name staged by one seal and idle in the next must reach the next seal's
// idle pass. The staged-ID bitset is retained between seals, so a bit left
// over from the first seal would hide every name it wrote from the second
// seal's expiry.
func TestSealSeenBitsetClearedBetweenSeals(t *testing.T) {
	drainRollupScratchPool()
	defer drainRollupScratchPool()
	const names = 64
	r, batches := sealTimeOnlyFixture(t, names, 1, 1)
	clk := time.Unix(100000, 0)
	r.now = func() time.Time { return clk }
	r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return true }
	r.setExpiryTTL(expiryWindow(time.Minute))
	plan, err := r.planParts(coreSealInput{Batches: batches})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.seen) == 0 {
		t.Fatal("first seal did not use the staged-ID bitset")
	}
	plan.Commit()

	// Seal 2 writes the highest ID only, so the bitset covers every idle
	// name; the others are overdue.
	clk = clk.Add(10 * time.Minute)
	one := batches[0]
	for _, b := range batches {
		if b.ID == names {
			one = b
		}
	}
	tm := clk.Unix()
	one.Clock, one.Order, one.Points = tm, uint64(names+1), []Row{{Time: tm, Bits: 1, Seq: 1}}
	plan, err = r.planParts(coreSealInput{Batches: []Batch{one}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if len(r.seen) == 0 {
		t.Fatal("second seal did not use the staged-ID bitset")
	}
	expired := map[uint64]bool{}
	for i := range r.stages {
		if s := &r.stages[i]; s.batches == 0 && s.next.Expired {
			expired[r.stageMetrics[i].id] = true
		}
	}
	if len(expired) != names-1 || expired[names] {
		t.Fatalf("second seal expired %d idle names (written name: %v), want %d", len(expired), expired[names], names-1)
	}
}
