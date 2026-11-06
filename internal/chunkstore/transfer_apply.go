package chunkstore

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
)

type preparedTransfer struct {
	batch           *pebble.Batch
	expected, metas []Metadata
	proofs          []TransferProof
	sidecars        []SidecarRecord
}

// Recompute after a concurrent target mutation; never merge a moving view or
// fall back to decoding under writer stripes. The reserved source ID stays
// immutable until the successful publication of this group.
func (s *Store) publishTransfer(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		began := time.Now()
		view := s.db.NewSnapshot()
		prepared, err := s.prepareTransfer(ctx, records, metas, view)
		_ = view.Close()
		s.transferApply.prepare.Add(uint64(time.Since(began)))
		if err != nil {
			return nil, err
		}
		if s.transferApplyTestHook != nil {
			s.transferApplyTestHook()
		}
		committed, err := s.commitPreparedTransfer(ctx, prepared)
		_ = prepared.batch.Close()
		if err != nil {
			return nil, err
		}
		if committed {
			return prepared.proofs, nil
		}
	}
}

func (s *Store) commitPreparedTransfer(ctx context.Context, p *preparedTransfer) (bool, error) {
	names := make([]string, len(p.metas))
	absent := false
	for i, m := range p.metas {
		names[i] = m.Name
		absent = absent || p.expected[i].ID == 0
	}
	wait := time.Now()
	unlock := s.lockTransfer(names, nil)
	locked := time.Now()
	s.transferApply.wait.Add(uint64(locked.Sub(wait)))
	defer func() { unlock(); s.transferApply.recordHold(time.Since(locked)) }()
	// Normal Create and phase1 native publication serialize absence with mu.
	// Acquire it in the same stripe -> creation order as ordinary Create.
	if absent {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return false, err
	}
	for i, name := range names {
		current, err := metadataFrom(s.db, name)
		if errors.Is(err, ErrNotFound) {
			if p.expected[i].ID != 0 {
				return false, nil
			}
		} else if err != nil {
			return false, err
		} else if !sameTransferTarget(current, p.expected[i]) {
			return false, nil
		}
	}
	if err := s.commitSidecarRecords(p.batch, p.sidecars, func(b *pebble.Batch) error { return b.Commit(pebble.NoSync) }); err != nil {
		return false, err
	}
	s.pendingSync.Store(true)
	for i, m := range p.metas {
		if old := p.expected[i]; old.ID != 0 {
			s.canonicalRun(old.ID).Clear(old.ID)
		}
		s.catalogChanged(m.Name)
	}
	return true, nil
}

func sameTransferTarget(a, b Metadata) bool {
	return transferVersion(a) == transferVersion(b) && samePolicy(a.MetricConfig, b.MetricConfig) && a.Continuation == b.Continuation && a.FileMigration == b.FileMigration
}
