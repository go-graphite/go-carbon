package chunkstore

import (
	"math"
	"testing"
)

func TestAnchorMutationEpochWithoutCacheAndSaturation(t *testing.T) {
	s := &Store{}
	stripe := s.metricStripe("epoch")
	start := s.mutationEpoch[stripe].Load()
	s.beginStateMutation("epoch")
	if s.mutationEpoch[stripe].Load() == start {
		t.Fatal("active uncached writer is invisible")
	}
	active := s.mutationEpoch[stripe].Load()
	s.endStateMutation("epoch")
	if s.mutationEpoch[stripe].Load() == active {
		t.Fatal("finishing uncached writer is invisible")
	}
	s.mutationEpoch[stripe].Store(math.MaxUint64 - 1)
	s.beginStateMutation("epoch")
	s.endStateMutation("epoch")
	if s.mutationEpoch[stripe].Load() != math.MaxUint64 {
		t.Fatal("epoch wrapped")
	}
}
