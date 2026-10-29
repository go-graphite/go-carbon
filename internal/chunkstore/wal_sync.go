package chunkstore

import (
	"errors"
	"sync"
)

// walSyncer coalesces durability requests into one WAL sync. Each sync is a
// Pebble commit; thousands of concurrent publications issuing their own syncs
// filled the commit pipeline's bounded queue and stalled every other writer.
// A request is satisfied only by a sync that started after it was registered,
// so it covers every commit the requester completed before asking.
type walSyncer struct {
	mu        sync.Mutex
	cond      *sync.Cond
	requested uint64
	completed uint64
	// A failed sync is sticky: Pebble treats WAL errors as fatal and the store
	// must restart before ownership can be published again.
	err     error
	stopped bool
	done    chan struct{}
}

var errStoreClosed = errors.New("store closed")

func (s *Store) startWALSyncer() {
	w := &walSyncer{done: make(chan struct{})}
	w.cond = sync.NewCond(&w.mu)
	s.walSync = w
	go func() {
		defer close(w.done)
		w.mu.Lock()
		defer w.mu.Unlock()
		for {
			for w.completed == w.requested && !w.stopped {
				w.cond.Wait()
			}
			if w.completed == w.requested && w.stopped {
				return
			}
			target := w.requested
			w.mu.Unlock()
			err := s.logSync()
			w.mu.Lock()
			if err != nil && w.err == nil {
				w.err = err
			}
			w.completed = target
			w.cond.Broadcast()
		}
	}()
}

// syncWAL returns once every commit completed by the caller is durable.
func (s *Store) syncWAL() error {
	w := s.walSync
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return errStoreClosed
	}
	w.requested++
	ticket := w.requested
	w.cond.Broadcast()
	for w.completed < ticket {
		w.cond.Wait()
	}
	return w.err
}

func (s *Store) stopWALSyncer() {
	w := s.walSync
	if w == nil {
		return
	}
	w.mu.Lock()
	w.stopped = true
	w.cond.Broadcast()
	w.mu.Unlock()
	<-w.done
}
