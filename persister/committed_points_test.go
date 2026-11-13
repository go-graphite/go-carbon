package persister

import (
	"sync"
	"testing"
)

// Restore progress can be scraped while interval collection resets its own
// counters. The progress total also outlives the uint32 interval's range.
func TestCommittedPointsIndependentOfStat(t *testing.T) {
	p := NewWhisper("", nil, nil, nil, nil, nil, nil)
	for range 4 {
		p.recordCommittedPoints(1 << 30)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 1000 {
			p.recordCommittedPoints(3)
		}
	})
	wg.Go(func() {
		for range 1000 {
			p.Stat(func(string, float64) {})
		}
	})
	wg.Wait()
	if got, want := p.CommittedPoints(), uint64(1<<32)+3000; got != want {
		t.Fatalf("successful points = %d, want %d", got, want)
	}
}
