package v2recv

import "errors"

// OnOwner schedules a bounded administrative operation on the same serial
// caller as intake. The callback must own its arguments until this returns.
// Controls never close, so shutdown cannot race a send to a closed queue.
func (s *Server) OnOwner(index int, run func() error) error {
	if index < 0 || index >= len(s.controls) || run == nil {
		return errors.New("invalid receiver owner operation")
	}
	call := ownerCall{run: run, ack: make(chan error, 1)}
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
