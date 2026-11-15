package daemonstore

import (
	"context"
	"sync"
)

type responseKey struct{}
type responseScope struct {
	mu       sync.Mutex
	closed   bool
	releases []func()
}

// ResponseScope keeps backend response allocations charged through protocol
// serialization. Canceling a nested storage call does not end this lifetime.
// The outer handler calls done after it has finished using all returned slices.
func ResponseScope(ctx context.Context) (context.Context, func()) {
	s := &responseScope{}
	return context.WithValue(ctx, responseKey{}, s), func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.closed = true
		fs := s.releases
		s.releases = nil
		s.mu.Unlock()
		for _, f := range fs {
			f()
		}
	}
}

// HoldResponse returns false when the caller must release an owned Series
// itself. It never silently charges a response to a canceled child context.
func HoldResponse(ctx context.Context, release func()) bool {
	s, ok := ctx.Value(responseKey{}).(*responseScope)
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		release()
		return true
	}
	s.releases = append(s.releases, release)
	return true
}
