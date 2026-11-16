package main

import "slices"

// Portable IDs are identities, not an allocation bound. A sorted directory
// indexes dense owner-local state without retaining a hash table or allocating
// by the largest ID. It is consulted once per metric, never once per point.
type rollupMetricStates struct {
	ids    []uint64
	values []*atomicRollupState
}

func (s *rollupMetricStates) get(id uint64) *atomicRollupState {
	i, ok := slices.BinarySearch(s.ids, id)
	if !ok {
		return nil
	}
	return s.values[i]
}

func (s *rollupMetricStates) set(id uint64, value *atomicRollupState) {
	i, ok := slices.BinarySearch(s.ids, id)
	if ok {
		s.values[i] = value
		return
	}
	s.ids = slices.Insert(s.ids, i, id)
	s.values = slices.Insert(s.values, i, value)
}
