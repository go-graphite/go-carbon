package persister

import (
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

type panicConfirmGuardStore struct {
	failedSharedStore
	mu sync.Mutex
}

func (s *panicConfirmGuardStore) SharedWriteGuard(string) func() {
	s.mu.Lock()
	return s.mu.Unlock
}

func TestSharedConfirmPanicReleasesGuard(t *testing.T) {
	var calls []string
	db := &panicConfirmGuardStore{failedSharedStore: failedSharedStore{calls: &calls}}
	batch := points.OnePoint("guard.panic", 1, time.Now().Unix()-1)
	p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, func(string) (*points.Points, bool) { return batch, true }, func(*points.Points) { panic("injected confirmation panic") }, nil)
	p.SetMetricStore(db)
	func() {
		defer func() {
			if got := recover(); got != "injected confirmation panic" {
				t.Fatalf("unexpected recovered panic: %v", got)
			}
		}()
		p.store(batch.Metric)
	}()
	if !db.mu.TryLock() {
		t.Fatal("confirmation panic left the read-coherence guard locked")
	}
	db.mu.Unlock()
}
