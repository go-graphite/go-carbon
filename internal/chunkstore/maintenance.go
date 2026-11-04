package chunkstore

import (
	"context"
	"sync"
)

// operationState closes admission, cancels registered contexts and joins their
// callers. The store keeps separate groups for prewarms (StopBackground) and
// explicit maintenance (Close), so stopping its workers does not close the
// owner's maintenance API. Cancellation is synchronous before a join begins.
type operationState struct {
	mu      sync.Mutex
	stopped bool
	next    uint64
	cancels map[uint64]context.CancelFunc
	running sync.WaitGroup
}

func (p *operationState) begin(ctx context.Context) (context.Context, func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(ctx)
	if p.cancels == nil {
		p.cancels = make(map[uint64]context.CancelFunc)
	}
	id := p.next
	p.next++
	p.cancels[id] = cancel
	p.running.Add(1)
	return ctx, func() {
		cancel()
		p.mu.Lock()
		delete(p.cancels, id)
		p.mu.Unlock()
		p.running.Done()
	}, true
}

func (p *operationState) cancel() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	for _, cancel := range p.cancels {
		cancel()
	}
}

func (p *operationState) stop() {
	p.cancel()
	p.running.Wait()
}

func (s *Store) waitMaintenance(ctx context.Context, name string) error {
	if s.maintenanceWait != nil {
		return s.maintenanceWait(ctx, name)
	}
	return ctx.Err()
}
