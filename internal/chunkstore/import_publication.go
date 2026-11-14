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
	return s.importPublishing[s.metricStripe(name)][name].done
}

type pendingMutation struct {
	done   chan struct{}
	update bool // ordinary writes wait; imports ask foreground writers to retry
}

// releaseImportStripes exchanges held mutexes for exact-name reservations.
// The original mutation remains pending, keeping reader caches fenced. Every
// mutator checks reservations under the stripe before reading its target. Live
// TryUpdateMany and UpdateBatch return ErrImportPending for imports and wait
// without stripes for another ordinary update. No mutation overtakes publication.
// The returned cleanup reacquires the original stripes and invokes unlock.
func (s *Store) releaseImportStripes(names []string, held []bool, unlock func()) func() {
	return s.releaseMutationStripes(names, held, unlock, false)
}

func (s *Store) releaseUpdateStripes(names []string, unlock func()) func() {
	return s.releaseMutationStripes(names, nil, unlock, true)
}

func (s *Store) releaseMutationStripes(names []string, held []bool, unlock func(), update bool) func() {
	owned := make([]string, 0, len(names))
	done := make(chan struct{})
	for i, name := range names {
		if held != nil && !held[i] {
			continue
		}
		stripe := s.metricStripe(name)
		if s.importPublishing[stripe] == nil {
			s.importPublishing[stripe] = make(map[string]pendingMutation)
		}
		s.importPublishing[stripe][name] = pendingMutation{done: done, update: update}
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
