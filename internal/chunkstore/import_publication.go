package chunkstore

import (
	"errors"

	"github.com/go-graphite/go-carbon/internal/lockset"
)

// ErrImportPending asks a foreground writer to retain its original input batch
// and retry later. A persister worker must never wait for an import commit.
var ErrImportPending = errors.New("metric import publication pending")

// importPendingLocked is protected by name's metric stripe. Reservations are
// per name: unrelated names hashing to the same stripe continue normally.
func (s *Store) importPendingLocked(name string) chan struct{} {
	return s.importPublishing[s.metricStripe(name)][name]
}

// releaseImportStripes exchanges the held mutexes for exact-name reservations.
// The original mutation remains pending, keeping reader caches fenced. Every
// mutator checks reservations under the stripe before reading its target. Live
// TryUpdateMany and UpdateBatch return ErrImportPending; ordinary synchronous
// operations wait without owning stripes.
// Thus no mutation can overtake publication, and no mutex spans Pebble commit.
// The returned cleanup reacquires only the original stripes and ends mutations.
func (s *Store) releaseImportStripes(names []string, held []bool, unlock func()) func() {
	owned := make([]string, 0, len(names))
	done := make(chan struct{})
	for i, name := range names {
		if held != nil && !held[i] {
			continue
		}
		stripe := s.metricStripe(name)
		if s.importPublishing[stripe] == nil {
			s.importPublishing[stripe] = make(map[string]chan struct{})
		}
		s.importPublishing[stripe][name] = done
		owned = append(owned, name)
	}
	stripes := s.transferStripes(owned)
	for i := len(stripes) - 1; i >= 0; i-- {
		s.metricMu[stripes[i]].Unlock()
	}
	return func() {
		lockset.Lock(s.metricMu[:], stripes)
		for _, name := range owned {
			delete(s.importPublishing[s.metricStripe(name)], name)
		}
		unlock()
		close(done)
	}
}

// lockImportFreeNames obtains a complete set without holding a stripe while
// waiting for publication of a reserved name. try never waits for reservations.
func (s *Store) lockImportFreeNames(names []string, try bool) bool {
	stripes := s.transferStripes(names)
	for {
		if try {
			if !lockset.TryLock(s.metricMu[:], stripes) {
				return false
			}
		} else {
			lockset.Lock(s.metricMu[:], stripes)
		}
		var pending chan struct{}
		for _, name := range names {
			if pending = s.importPendingLocked(name); pending != nil {
				break
			}
		}
		if pending == nil {
			return true
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
		if try {
			return false
		}
		<-pending
	}
}
