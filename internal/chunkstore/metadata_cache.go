package chunkstore

import "strings"

// A small ring retains the metadata shared by consecutive preparation, policy
// and update calls. It is protected by the corresponding metricMu stripe.
// Entries never survive a mutation of that name, including failed mutations.
const metadataEntriesPerStripe = 8

type metadataRing struct {
	entries [metadataEntriesPerStripe]Metadata
	next    uint8
}

func (s *Store) metadataLocked(name string) (Metadata, error) {
	ring := &s.metadataCache[s.metricStripe(name)]
	for _, m := range ring.entries {
		if m.Name == name && m.ID != 0 {
			m.MetricConfig = cloneConfig(m.MetricConfig)
			return m, nil
		}
	}
	m, err := metadataFrom(s.db, name)
	if err == nil && len(name) <= 4096 && len(m.Retentions) <= 64 {
		cached := m
		cached.MetricConfig = cloneConfig(m.MetricConfig)
		// A short name may be a substring of a large request buffer. Own only
		// its bounded bytes rather than keeping that request alive in the ring.
		cached.Name = strings.Clone(m.Name)
		ring.entries[ring.next] = cached
		ring.next = (ring.next + 1) % metadataEntriesPerStripe
	}
	return m, err
}

// All metadata mutations use this lock. Invalidate before unlocking even on an
// error: an unsuccessful batch must never make a cached revision authoritative.
func (s *Store) lockMetricMutation(name string) func() {
	unlock := s.lockMetric(name)
	s.beginStateMutation(name)
	return s.mutationUnlock(name, unlock)
}

// tryLockMetricMutation is lockMetricMutation without waiting; ok is false when
// another writer holds the name's stripe.
func (s *Store) tryLockMetricMutation(name string) (unlock func(), ok bool) {
	mu := &s.metricMu[s.metricStripe(name)]
	if !mu.TryLock() {
		return nil, false
	}
	s.beginStateMutation(name)
	return s.mutationUnlock(name, mu.Unlock), true
}

func (s *Store) mutationUnlock(name string, unlock func()) func() {
	return func() { s.finishStateMutation(name); unlock() }
}

func (s *Store) finishStateMutation(name string) {
	ring := &s.metadataCache[s.metricStripe(name)]
	for i := range ring.entries {
		if ring.entries[i].Name == name {
			ring.entries[i] = Metadata{}
		}
	}
	s.endStateMutation(name)
}

// beginStateMutation and endStateMutation bracket every mutation of name in
// the metadata cache (see stateCache), under name's stripe.
func (s *Store) beginStateMutation(name string) {
	if c := s.stateCache; c != nil {
		c.beginMutation(s.metricStripe(name), name)
	}
}

func (s *Store) endStateMutation(name string) {
	if c := s.stateCache; c != nil {
		c.endMutation(s.metricStripe(name), name)
	}
}

// lockedState returns name's latest committed state for a mutation holding its
// stripe, with metadata the caller may modify, or nil.
func (s *Store) lockedState(name string) (*metricState, Metadata) {
	c := s.stateCache
	if c == nil {
		return nil, Metadata{}
	}
	st := c.locked(s.metricStripe(name), name)
	if st == nil {
		return nil, Metadata{}
	}
	m := st.meta
	m.MetricConfig = cloneConfig(m.MetricConfig)
	return st, m
}

// newStateView returns the state view for an io of metric m that starts from
// base (nil when unknown), or nil when the cache keeps no continuation state.
func (s *Store) newStateView(base *metricState, m Metadata) *stateView {
	c := s.stateCache
	if c == nil || c.keys == nil {
		return nil
	}
	if base != nil && (base.meta.ID != m.ID || base.meta.Generation != m.Generation) {
		base = nil
	}
	return &stateView{keys: c.keys, base: base, meta: m}
}

// installState records, for the mutation of m.Name holding its stripe, the
// state it committed: m as its metadata and the continuation keys of v (nil
// for none known). Call it only after a successful commit, or when the
// mutation committed nothing.
func (s *Store) installState(v *stateView, m Metadata) {
	c := s.stateCache
	if c == nil {
		return
	}
	// The metadata a fresh read would decode: revision bytes round-trip the
	// activity time to the nanosecond, without a monotonic reading.
	if err := decodeRevision(revisionBytes(m), &m); err != nil {
		return
	}
	var st *metricState
	if v != nil {
		st = v.final(m)
	} else {
		st = newMetricState(m).seal()
	}
	c.installMutation(s.metricStripe(m.Name), m.Name, st)
}

// metadataLockedOrAbsent is metadataLocked for a writer holding the name's
// stripe, answering without a database lookup for a name the complete catalog
// filter has never seen. Every catalog write adds its name before committing
// under that stripe, so such a name cannot be in the catalog.
func (s *Store) metadataLockedOrAbsent(name string) (Metadata, error) {
	if !s.catalogMembership.mayContain(name) {
		return Metadata{}, ErrNotFound
	}
	return s.metadataLocked(name)
}
