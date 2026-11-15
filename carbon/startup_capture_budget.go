package carbon

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
)

// A byte budget, independent of socket count. The conservative charge includes
// borrowed input, column slack, name copies, sorting and compressed write work.
// Idle pooled columns have a separate fixed 32MiB ceiling. Decoder dictionaries
// and normal input-log encoder state are not receiver-owned staged data.
type captureByteBudget struct {
	mu          sync.Mutex
	limit, used int64
	peak        atomic.Int64
	changed     chan struct{}
}

func (j *startupJournal) acquire(bytes, points int) (func(), error) {
	started := time.Now()
	if bytes < 0 || points < 0 {
		return nil, errors.New("invalid capture reservation")
	}
	charge := int64(bytes)*4 + int64(points)*128 + 64<<10
	b := &j.budget
	b.mu.Lock()
	if charge > b.limit {
		b.mu.Unlock()
		return nil, errors.New("capture batch exceeds byte budget")
	}
	for b.used+charge > b.limit {
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-j.owner.stop:
			return nil, inputlog.ErrClosed
		}
		b.mu.Lock()
	}
	b.used += charge
	for old := b.peak.Load(); b.used > old; old = b.peak.Load() {
		if b.peak.CompareAndSwap(old, b.used) {
			break
		}
	}
	b.mu.Unlock()
	j.admissionTiming.observe(started)
	return func() {
		b.mu.Lock()
		b.used -= charge
		close(b.changed)
		b.changed = make(chan struct{})
		b.mu.Unlock()
	}, nil
}
