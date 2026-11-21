package v2recv

import "errors"

// OnOwner schedules a bounded administrative operation on the same serial
// caller as intake. The callback must own its arguments until this returns.
// Controls never close, so shutdown cannot race a send to a closed queue.
func (s *Server) OnOwner(index int, run func() error) error { return s.onOwner(index, run, false) }
func (s *Server) onOwner(index int, run func() error, drain bool) error {
	if index < 0 || index >= len(s.controls) || run == nil {
		return errors.New("invalid receiver owner operation")
	}
	call := ownerCall{run: run, drain: drain, ack: make(chan error, 1)}
	select {
	case s.controls[index] <- call:
	case <-s.done:
		return errors.New("receiver stopped")
	case <-s.finished:
		return errors.New("receiver stopped")
	}
	select {
	case err := <-call.ack:
		return err
	case <-s.finished:
		return errors.New("receiver stopped")
	}
}

// Fence drains decoded buffers, queued batches and durable owner input before
// running maintenance. The callback may use OnOwner; it must not call Fence.
// New socket buffers wait until maintenance finishes. Idle sockets hold no lock.
func (s *Server) Fence(run func() error) error {
	s.intake.Lock()
	defer s.intake.Unlock()
	for _, flush := range s.decoded {
		if err := flush(); err != nil {
			return err
		}
	}
	for i, o := range s.owners {
		err := s.onOwner(i, func() error {
			if f, ok := o.(interface{ Flush() error }); ok {
				return f.Flush()
			}
			if err := o.Seal(); err != nil {
				return err
			}
			return o.Drain()
		}, true)
		if err != nil {
			return err
		}
	}
	return run()
}
