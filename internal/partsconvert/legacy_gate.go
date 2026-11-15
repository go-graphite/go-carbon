package partsconvert

import (
	"context"
	"errors"
	"sync"
)

// ReaderGate pins entire mixed-reader calls, including streaming consumer
// callbacks. It is separate from writer locks. The owner must take a pin before
// reading its current Reader pointer and retain it until the result is consumed.
// During exclusive legacy cleanup, new calls fail admission and old calls drain.
type ReaderGate struct {
	op      sync.Mutex
	mu      sync.Mutex
	changed chan struct{}
	active  int
	fenced  bool
}

var ErrLegacyFenced = errors.New("legacy readers fenced for physical reclamation")

func (g *ReaderGate) Pin() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced {
		return nil, ErrLegacyFenced
	}
	g.active++
	return sync.OnceFunc(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.active--
		g.notifyLocked()
	}), nil
}

func (g *ReaderGate) notifyLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

// Exclusive closes all read-only legacy handles after draining their readers,
// runs a writable maintenance operation, then reopens the remaining source.
// Callbacks must be idempotent on retry. Reopen installs the latest already
// durable cursor. Reopen failure leaves admissions fenced, never a stale Reader.
func (g *ReaderGate) Exclusive(ctx context.Context, closeReaders, operation, reopenReaders func() error) error {
	if closeReaders == nil || operation == nil || reopenReaders == nil {
		return errors.New("legacy exclusive operation needs close/operate/reopen callbacks")
	}
	g.op.Lock()
	defer g.op.Unlock()
	g.mu.Lock()
	wasFenced := g.fenced
	g.fenced = true
	for g.active != 0 {
		if g.changed == nil {
			g.changed = make(chan struct{})
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.fenced = wasFenced
			g.mu.Unlock()
			return ctx.Err()
		case <-changed:
		}
		g.mu.Lock()
	}
	g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		g.mu.Lock()
		g.fenced = wasFenced
		g.mu.Unlock()
		return err
	}
	if err := closeReaders(); err != nil {
		return err
	}
	err := operation()
	reopenErr := reopenReaders()
	if reopenErr == nil {
		g.mu.Lock()
		g.fenced = false
		g.mu.Unlock()
	}
	return errors.Join(err, reopenErr)
}
