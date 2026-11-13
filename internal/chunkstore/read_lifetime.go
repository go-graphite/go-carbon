package chunkstore

import "context"

// ReadError reports whether read admission is closed. Servers also check this
// before returning cached results, so shutdown is not reported as missing data.
func (s *Store) ReadError() error {
	if s.readCtx.Err() != nil {
		return ErrClosed
	}
	return nil
}

// readContext cancels a queued admission or an active read when Close starts.
// An admission alone owns no Pebble resources: Close does not wait for release.
func (s *Store) readContext(ctx context.Context) (context.Context, func(), error) {
	if err := s.ReadError(); err != nil {
		return ctx, func() {}, err
	}
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.readCtx, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// beginRead protects the entire view lifetime, including iterator/value closes.
// Check cancellation under the lock: Close can cancel between readContext and
// registration, and no reader may join after Close starts waiting.
func (s *Store) beginRead(ctx context.Context) (context.Context, func(), error) {
	ctx, cancel, err := s.readContext(ctx)
	if err != nil {
		return ctx, nil, err
	}
	s.readMu.Lock()
	if err := s.ReadError(); err != nil {
		s.readMu.Unlock()
		cancel()
		return ctx, nil, err
	}
	s.readCount++
	s.readMu.Unlock()
	return ctx, func() {
		cancel()
		s.readMu.Lock()
		s.readCount--
		if s.readCount == 0 && s.readCtx.Err() != nil {
			close(s.readDone)
		}
		s.readMu.Unlock()
	}, nil
}
