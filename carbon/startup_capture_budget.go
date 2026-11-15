package carbon

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// A byte budget, independent of socket count. The conservative charge includes
// borrowed input, column slack, name copies, sorting and compressed write work.
// Idle pooled buffers have a separate fixed 32MiB accounting ceiling. Receiver
// dictionaries and input-log encoder state remain outside this admission limit;
// see doc/startup-group-commit.md for the complete memory inventory.
type captureByteBudget struct {
	mu              sync.Mutex
	limit, used     int64
	peak            atomic.Int64
	changed         chan struct{}
	waits, rejected atomic.Int64
	nextWarning     time.Time
}

func (j *startupJournal) acquire(bytes, points int) (func(), error) {
	started := time.Now()
	if bytes < 0 || points < 0 {
		return nil, errors.New("invalid capture reservation")
	}
	b := &j.budget
	// Check arithmetic before multiplying: an invalid oversized reservation must
	// never wrap negative and bypass admission.
	if int64(bytes) > (math.MaxInt64-(64<<10))/4 || int64(points) > (math.MaxInt64-(64<<10)-4*int64(bytes))/128 {
		b.rejected.Add(1)
		zapwriter.Logger("app").Error("startup capture reservation overflows byte accounting", zap.Int("raw_bytes", bytes), zap.Int("points", points), zap.Int64("limit_bytes", b.limit))
		return nil, errors.New("capture batch exceeds byte budget")
	}
	charge := int64(bytes)*4 + int64(points)*128 + 64<<10
	b.mu.Lock()
	if charge > b.limit {
		b.rejected.Add(1)
		b.mu.Unlock()
		zapwriter.Logger("app").Error("startup capture batch exceeds admission byte budget", zap.Int64("charge_bytes", charge), zap.Int64("limit_bytes", b.limit))
		return nil, errors.New("capture batch exceeds byte budget")
	}
	waited := false
	for b.used+charge > b.limit {
		if !waited {
			b.waits.Add(1)
			waited = true
		}
		if now := time.Now(); !now.Before(b.nextWarning) {
			b.nextWarning = now.Add(30 * time.Second)
			zapwriter.Logger("app").Warn("startup capture admission byte budget exhausted; applying backpressure", zap.Int64("used_bytes", b.used), zap.Int64("charge_bytes", charge), zap.Int64("limit_bytes", b.limit))
		}
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
