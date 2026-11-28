package downsize

import (
	"context"
	"sync"
)

// syncCopyPipelineDepth is how many of one donor's absent-name waves copy at
// once. A wave waits for its slowest batch; with one wave at a time most of a
// donor's stream budget idled behind the stragglers and the next wave's
// precheck and probe (measured: 1-2 of 64 pulls in flight between bursts).
// The donor's pull slots still bound the pulls in flight to its budget.
const syncCopyPipelineDepth = 4

// syncWavePipe runs up to depth waves at once. Wave bodies hold the syncer
// lock (s.pipeMu) for all syncer and journal state, and release it only around
// donor and target I/O (s.ioUnlocked), so their bookkeeping stays as serial
// as before. Receipts are unchanged: each wave keeps its own missing-wave row,
// transfer journal and copied row, so a resume replays done waves from their
// receipts and redoes only the unfinished batches of the waves in flight.
type syncWavePipe struct {
	s      *syncer
	mu     *sync.Mutex // nil: depth 1, waves run inline as before
	ctx    context.Context
	cancel context.CancelFunc
	slots  chan struct{}
	wg     sync.WaitGroup
	errMu  sync.Mutex
	err    error
}

func (s *syncer) startWavePipe(ctx context.Context, depth int) *syncWavePipe {
	p := &syncWavePipe{s: s, ctx: ctx}
	if depth <= 1 {
		return p
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.mu, p.slots = &sync.Mutex{}, make(chan struct{}, depth)
	p.mu.Lock()
	s.pipeMu = p.mu
	return p
}

func (p *syncWavePipe) fail(err error) {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	if p.err == nil {
		p.err = err
		if p.cancel != nil {
			p.cancel()
		}
	}
}

func (p *syncWavePipe) failed() bool {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err != nil
}

// run starts one wave once a slot is free, or runs it inline at depth 1. It
// reports false once a wave has failed: start no further waves.
func (p *syncWavePipe) run(wave func(context.Context) error) bool {
	if p.failed() {
		return false
	}
	if p.mu == nil {
		if err := wave(p.ctx); err != nil {
			p.fail(err)
			return false
		}
		return true
	}
	p.mu.Unlock()
	acquired := false
	select {
	case p.slots <- struct{}{}:
		acquired = true
	case <-p.ctx.Done():
	}
	p.mu.Lock()
	if p.ctx.Err() != nil {
		if acquired {
			<-p.slots
		}
		p.fail(p.ctx.Err())
		return false
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.mu.Lock()
		err := wave(p.ctx)
		p.mu.Unlock()
		<-p.slots
		if err != nil {
			p.fail(err)
		}
	}()
	return !p.failed()
}

// wait finishes every started wave and returns the first error. The caller
// keeps the syncer to itself afterwards.
func (p *syncWavePipe) wait() error {
	if p.mu != nil {
		p.mu.Unlock()
		p.wg.Wait()
		p.mu.Lock()
		p.s.pipeMu = nil
		p.mu.Unlock()
		p.mu = nil
		p.cancel()
	}
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}

// ioUnlocked runs donor or target I/O without the wave lock, so overlapping
// waves can transfer while one of them updates syncer state. f must touch no
// syncer state other than the journal (which locks itself) and atomics.
func (s *syncer) ioUnlocked(f func()) {
	if mu := s.pipeMu; mu != nil {
		mu.Unlock()
		defer mu.Lock()
	}
	f()
}
