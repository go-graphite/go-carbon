package chunkstore

import "fmt"

// CatalogCheckpointToken identifies an exact durable store state. The caller
// must compare it before and after saving its discovery checkpoint. Stores
// without a persistent metadata identity cannot certify restart reuse.
func (s *Store) CatalogCheckpointToken() string {
	s.initNativeContext()
	// Catalog notifications follow commit under a mutation stripe. An
	// in-flight writer may have committed without notifying discovery yet;
	// sequence equality alone cannot certify that gap. Never wait on reads or
	// writers here: any held stripe conservatively disables this accelerator.
	locked := 0
	defer func() {
		for i := locked - 1; i >= 0; i-- {
			s.metricMu[i].Unlock()
		}
	}()
	for i := range s.metricMu {
		if !s.metricMu[i].TryLock() {
			return ""
		}
		locked++
	}
	// Imports may release their stripes during WAL sync, with notifications
	// following afterwards. Reservations also fence ordinary async mutations.
	if len(s.nativeAdmission) != 0 || s.nativeFailure.Load() != nil {
		return ""
	}
	for i := range s.importPublishing {
		if len(s.importPublishing[i]) != 0 {
			return ""
		}
	}
	return s.catalogCheckpointToken(s.db.VisibleSequence())
}

// OpenCatalogCheckpointToken describes the state before startup consumes the
// metadata checkpoint stamp or recovery mutates the store. CatalogChanges from
// cursor 1 includes mutations made after that point, including during Open.
func (s *Store) OpenCatalogCheckpointToken() string {
	return s.catalogCheckpointToken(s.catalogOpenSequence)
}

func (s *Store) catalogCheckpointToken(sequence uint64) string {
	if s.checkpoint.identity == [16]byte{} {
		return ""
	}
	return fmt.Sprintf("%x:%d", s.checkpoint.identity, sequence)
}
