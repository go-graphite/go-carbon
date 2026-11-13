package chunkstore

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
)

var errTransferSplit = errors.New("transfer publication needs single-name queues")

type preparedTransfer struct {
	batch           *pebble.Batch
	expected, metas []Metadata
	proofs          []TransferProof
	sidecars        []SidecarRecord
	// epochs are the metas' stripe epochs captured before the preparation
	// view opened; commit skips the catalog re-read of unchanged stripes.
	epochs []uint64
}

// Recompute after a concurrent target mutation; never merge a moving view or
// fall back to decoding under writer stripes. The reserved source ID stays
// immutable until the successful publication of this group.
func (s *Store) publishTransfer(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	// Busy stripes usually stay busy on a draining survivor. Avoid preparing a
	// large batch only to discard it on the same contention after decoding.
	if len(records) > 1 {
		names := make([]string, len(records))
		for i := range records {
			names[i] = records[i].Config.Name
		}
		if s.transferBusy(names) {
			return s.publishTransferSingles(ctx, records, metas)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		began := time.Now()
		epochs := s.transferEpochs(metas)
		view := s.db.NewSnapshot()
		prepared, err := s.prepareTransfer(ctx, records, metas, view)
		_ = view.Close()
		if prepared != nil {
			prepared.epochs = epochs
		}
		s.transferApply.prepare.Add(uint64(time.Since(began)))
		if err != nil {
			return nil, err
		}
		if s.transferApplyTestHook != nil {
			s.transferApplyTestHook()
		}
		committed, err := s.commitPreparedTransfer(ctx, prepared)
		_ = prepared.batch.Close()
		if errors.Is(err, errTransferSplit) {
			return s.publishTransferSingles(ctx, records, metas)
		}
		if err != nil {
			return nil, err
		}
		if committed {
			return prepared.proofs, nil
		}
	}
}

func (s *Store) publishTransferSingles(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	s.transferApply.splits.Add(1)
	proofs := make([]TransferProof, 0, len(records))
	for i := range records {
		group, err := s.publishTransfer(ctx, records[i:i+1], metas[i:i+1])
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, group...)
	}
	return proofs, nil
}

func (s *Store) commitPreparedTransfer(ctx context.Context, p *preparedTransfer) (bool, error) {
	names := make([]string, len(p.metas))
	absent := false
	for i, m := range p.metas {
		names[i] = m.Name
		absent = absent || p.expected[i].ID == 0
	}
	wait := time.Now()
	unlock, stripes, unchanged := s.acquireTransfer(names, len(names) > 1, p.epochs)
	if unlock == nil {
		return false, errTransferSplit
	}
	s.transferApply.stripeHolds[stripes].Add(1)
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
	// Each catalog/revision re-read probes every LSM level's filter block under
	// these stripes. Under concurrent copies those probes queue on one hot
	// block-cache shard, stretch stripe holds and split batches. An unchanged
	// epoch already proves the view's metadata current, so skip the re-read.
	for i, name := range names {
		if !s.transferRereadMetadata && unchanged != nil && unchanged[i] {
			s.transferApply.epochValidated.Add(1)
			continue
		}
		current, err := metadataFrom(s.db, name)
		switch {
		case errors.Is(err, ErrNotFound):
			if p.expected[i].ID != 0 {
				if len(names) > 1 {
					return false, errTransferSplit
				}
				return false, nil
			}
		case err != nil:
			return false, err
		case !sameTransferTarget(current, p.expected[i]):
			if len(names) > 1 {
				return false, errTransferSplit
			}
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
