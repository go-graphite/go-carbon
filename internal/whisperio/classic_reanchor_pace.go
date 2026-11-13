package whisperio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// ClassicReanchorPacer bounds classic re-anchor repairs per second. A restart
// otherwise repairs the whole one-off backlog inline on each metric's first
// write, inside the warm-up window. Over budget, the write that found the
// damage returns store.ErrReanchorDeferred: nothing is published, the points
// stay in the cache, and the metric waits in a FIFO for the background sweep.
// The write guard still rejects any write onto an un-repaired archive.
type ClassicReanchorPacer struct {
	rate     float64 // tokens per second; <= 0 means unlimited
	burst    float64
	maxDefer time.Duration
	limit    int
	now      func() time.Time

	mu      sync.Mutex
	tokens  float64
	last    time.Time
	pending map[string]classicPending
	queue   []string
	wake    chan struct{}

	backlog                 atomic.Int64
	inline, deferred, swept atomic.Uint64
	aged, overflow          atomic.Uint64
}

type classicPending struct {
	archives []int
	since    time.Time
}

// ClassicReanchorPaceStats counts metric repairs by path. Deferred counts
// writes refused for lack of budget (each metric once per deferral), Aged the
// inline repairs forced because a metric waited longer than max-defer.
type ClassicReanchorPaceStats struct {
	Inline, Deferred, Swept, Aged, Overflow uint64
	Backlog                                 int64
}

type classicReanchorGrantKey struct{}

// WithClassicReanchorGrant marks a sweep repair: the update spends no token.
func WithClassicReanchorGrant(ctx context.Context) context.Context {
	return context.WithValue(ctx, classicReanchorGrantKey{}, true)
}

func classicReanchorGranted(ctx context.Context) bool {
	granted, _ := ctx.Value(classicReanchorGrantKey{}).(bool)
	return granted
}

// NewClassicReanchorPacer returns nil (unlimited, the unpaced behaviour) for a
// non-positive rate. maxDefer bounds how long a metric's points stay cached.
func NewClassicReanchorPacer(perSecond float64, maxDefer time.Duration) *ClassicReanchorPacer {
	if perSecond <= 0 {
		return nil
	}
	return newClassicReanchorPacer(perSecond, maxDefer, time.Now)
}

func newClassicReanchorPacer(perSecond float64, maxDefer time.Duration, now func() time.Time) *ClassicReanchorPacer {
	burst := perSecond
	if burst < 1 {
		burst = 1
	}
	return &ClassicReanchorPacer{
		rate: perSecond, burst: burst, maxDefer: maxDefer, limit: 4 << 20, now: now,
		tokens: burst, last: now(), pending: make(map[string]classicPending), wake: make(chan struct{}, 1),
	}
}

func (p *ClassicReanchorPacer) refill(now time.Time) {
	if elapsed := now.Sub(p.last).Seconds(); elapsed > 0 {
		p.tokens = min(p.burst, p.tokens+elapsed*p.rate)
		p.last = now
	}
}

func (p *ClassicReanchorPacer) take(now time.Time) bool {
	p.refill(now)
	if p.tokens >= 1 {
		p.tokens--
		return true
	}
	return false
}

// Holding reports whether name waits for a paced repair, so the persister
// leaves its points cached instead of retrying a write that must defer.
func (p *ClassicReanchorPacer) Holding(name string) bool {
	if p == nil || p.backlog.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.pending[name]
	return ok && p.now().Sub(e.since) < p.maxDefer
}

// known returns the recorded damaged archives of name and whether the update
// may repair them now. granted=false means defer without opening Whisper.
func (p *ClassicReanchorPacer) known(ctx context.Context, name string) (archives []int, granted bool) {
	if p == nil || p.backlog.Load() == 0 {
		return nil, true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.pending[name]
	if !ok {
		return nil, true
	}
	now := p.now()
	switch {
	case classicReanchorGranted(ctx):
	case now.Sub(e.since) >= p.maxDefer:
		p.aged.Add(1)
	case p.take(now):
	default:
		return e.archives, false
	}
	return e.archives, true
}

// admit decides a newly detected conflict on an unrecorded metric.
func (p *ClassicReanchorPacer) admit(ctx context.Context, name string, archive int) bool {
	if p == nil || classicReanchorGranted(ctx) {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.pending[name]; ok {
		return true // Already paid for by known().
	}
	now := p.now()
	if p.take(now) {
		return true
	}
	if len(p.pending) >= p.limit {
		p.overflow.Add(1)
		return true // Bounded memory: repair inline rather than track.
	}
	p.pending[name] = classicPending{archives: []int{archive}, since: now}
	p.queue = append(p.queue, name)
	p.backlog.Store(int64(len(p.pending)))
	p.deferred.Add(1)
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return false
}

// repaired forgets name once its repair has been staged in the store batch.
// If that batch later fails, the next write detects the damage again.
func (p *ClassicReanchorPacer) repaired(ctx context.Context, name string) {
	if p == nil {
		return
	}
	if classicReanchorGranted(ctx) {
		p.swept.Add(1)
	} else {
		p.inline.Add(1)
	}
	if p.backlog.Load() == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.pending[name]; ok {
		delete(p.pending, name)
		p.backlog.Store(int64(len(p.pending)))
	}
}

// Stats returns the pacer's counters; a nil pacer reports zeros.
func (p *ClassicReanchorPacer) Stats() ClassicReanchorPaceStats {
	if p == nil {
		return ClassicReanchorPaceStats{}
	}
	return ClassicReanchorPaceStats{Inline: p.inline.Load(), Deferred: p.deferred.Load(), Swept: p.swept.Load(),
		Aged: p.aged.Load(), Overflow: p.overflow.Load(), Backlog: p.backlog.Load()}
}

// next pops the oldest still-pending name and spends one token for it. It
// returns "" with the time to wait when the backlog or the budget is empty.
func (p *ClassicReanchorPacer) next() (string, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.queue) > 0 {
		name := p.queue[0]
		if _, ok := p.pending[name]; !ok {
			p.queue[0] = ""
			p.queue = p.queue[1:]
			continue
		}
		now := p.now()
		if !p.take(now) {
			return "", time.Duration((1 - p.tokens) / p.rate * float64(time.Second))
		}
		p.queue[0] = ""
		p.queue = p.queue[1:]
		return name, 0
	}
	p.queue = nil
	return "", -1
}

// requeue returns a name whose sweep repair failed to the back of the queue.
func (p *ClassicReanchorPacer) requeue(name string, forget bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.pending[name]; !ok {
		return
	}
	if forget {
		delete(p.pending, name)
		p.backlog.Store(int64(len(p.pending)))
		return
	}
	p.queue = append(p.queue, name)
}

// Sweep repairs deferred metrics oldest first within the shared budget until
// ctx ends. ready gates the start (for example the post-warm-up maintenance
// gate); repair must run an ordinary store update of name without points under
// the store's mutation lock, with ctx carrying the sweep grant.
func (p *ClassicReanchorPacer) Sweep(ctx context.Context, ready func(context.Context) error, repair func(context.Context, string) error) {
	if p == nil {
		return
	}
	if ready != nil {
		if err := ready(ctx); err != nil {
			return
		}
	}
	grant := WithClassicReanchorGrant(ctx)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for ctx.Err() == nil {
		name, wait := p.next()
		if name == "" {
			if wait < 0 {
				wait = time.Minute
			}
			timer.Reset(max(wait, time.Millisecond))
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
			case <-timer.C:
			}
			continue
		}
		err := repair(grant, name)
		switch {
		case err == nil:
			// repaired() already forgot it inside the codec; drop a metric
			// that turned out healthy or non-classic too.
			p.requeue(name, true)
		case errors.Is(err, store.ErrNotFound):
			p.requeue(name, true)
		case ctx.Err() != nil:
			p.requeue(name, false)
			return
		default:
			zapwriter.Logger("storage").Warn("classic re-anchor sweep", zap.String("metric", name), zap.Error(err))
			p.requeue(name, false)
		}
	}
}
