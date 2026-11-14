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
			return s.publishTransferSplit(ctx, records, metas)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		began := time.Now()
		epochs := s.transferEpochs(metas)
		view, err := s.newTransferView()
		if err != nil {
			return nil, err
		}
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
			return s.publishTransferSplit(ctx, records, metas)
		}
		if err != nil {
			return nil, err
		}
		if committed {
			return prepared.proofs, nil
		}
	}
}

func (s *Store) publishTransferSplit(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	if s.transferSplitSingles {
		return s.publishTransferSingles(ctx, records, metas)
	}
	return s.publishTransferPartitioned(ctx, records, metas)
}

// publishTransferPartitioned publishes a contended group without falling back
// to one snapshot, preparation and commit per name. Every name is prepared into
// its own batch, all pending names from one snapshot. Each round commits, in
// one batch, the names whose stripes are free and whose targets are unchanged.
// A name on a busy stripe keeps its preparation for the next round; only a
// name whose target changed is prepared again. When no stripe is free the
// round waits through lockset.Lock, which never sleeps holding a partial set.
func (s *Store) publishTransferPartitioned(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	s.transferApply.splits.Add(1)
	proofs := make([]TransferProof, len(records))
	parts := make([]*preparedTransfer, len(records))
	defer func() {
		for _, p := range parts {
			if p != nil {
				_ = p.batch.Close()
			}
		}
	}()
	pending := make([]int, len(records))
	for i := range pending {
		pending[i] = i
	}
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := s.prepareTransferParts(ctx, records, metas, parts, pending); err != nil {
			return nil, err
		}
		committed, err := s.commitTransferParts(ctx, parts, pending)
		if err != nil {
			return nil, err
		}
		next := make([]int, 0, len(pending))
		for j, i := range pending {
			switch committed[j] {
			case partCommitted:
				proofs[i] = parts[i].proofs[0]
				_ = parts[i].batch.Close()
				parts[i] = nil
			case partChanged:
				_ = parts[i].batch.Close()
				parts[i] = nil
				next = append(next, i)
			default:
				next = append(next, i)
			}
		}
		pending = next
	}
	return proofs, nil
}

// prepareTransferParts prepares every pending name without a preparation from
// one snapshot, each into its own batch.
func (s *Store) prepareTransferParts(ctx context.Context, records []TransferMetric, metas []Metadata, parts []*preparedTransfer, pending []int) error {
	var stale []int
	for _, i := range pending {
		if parts[i] == nil {
			stale = append(stale, i)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	began := time.Now()
	epochs := make([]uint64, len(stale))
	for j, i := range stale {
		epochs[j] = s.mutationEpoch[s.metricStripe(metas[i].Name)].Load()
	}
	view, err := s.newTransferView()
	if err != nil {
		return err
	}
	for j, i := range stale {
		var p *preparedTransfer
		if p, err = s.prepareTransfer(ctx, records[i:i+1], metas[i:i+1], view); err != nil {
			break
		}
		p.epochs = epochs[j : j+1]
		parts[i] = p
	}
	_ = view.Close()
	s.transferApply.prepare.Add(uint64(time.Since(began)))
	if err == nil && s.transferApplyTestHook != nil {
		s.transferApplyTestHook()
	}
	return err
}

type partOutcome uint8

const (
	partBusy partOutcome = iota
	partCommitted
	partChanged
)

// commitTransferParts commits, in one batch, the pending parts whose stripes
// it acquires without waiting and whose targets are unchanged.
func (s *Store) commitTransferParts(ctx context.Context, parts []*preparedTransfer, pending []int) ([]partOutcome, error) {
	names := make([]string, len(pending))
	epochs := make([]uint64, len(pending))
	absent := false
	for j, i := range pending {
		names[j] = parts[i].metas[0].Name
		epochs[j] = parts[i].epochs[0]
		absent = absent || parts[i].expected[0].ID == 0
	}
	wait := time.Now()
	unlock, held, unchanged, stripes := s.acquireTransferFree(names, epochs)
	s.transferApply.stripeHolds[min(stripes, len(s.transferApply.stripeHolds)-1)].Add(1)
	locked := time.Now()
	s.transferApply.wait.Add(uint64(locked.Sub(wait)))
	defer func() { unlock(); s.transferApply.recordHold(time.Since(locked)) }()
	if absent {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	out := make([]partOutcome, len(pending))
	batch := s.db.NewBatch()
	defer batch.Close()
	var sidecars []SidecarRecord
	for j, i := range pending {
		if !held[j] {
			continue
		}
		current, err := s.transferTargetCurrent(names[j], parts[i].expected[0], unchanged[j])
		if err != nil {
			return nil, err
		}
		if !current {
			out[j] = partChanged
			continue
		}
		if err := batch.Apply(parts[i].batch, nil); err != nil {
			return nil, err
		}
		sidecars = append(sidecars, parts[i].sidecars...)
		out[j] = partCommitted
	}
	if batch.Empty() && len(sidecars) == 0 {
		return out, nil
	}
	if err := s.commitSidecarRecords(batch, sidecars, func(b *pebble.Batch) error { return b.Commit(pebble.NoSync) }); err != nil {
		return nil, err
	}
	s.pendingSync.Store(true)
	for j, i := range pending {
		if out[j] != partCommitted {
			continue
		}
		if old := parts[i].expected[0]; old.ID != 0 {
			s.canonicalRun(old.ID).Clear(old.ID)
		}
		s.catalogChanged(names[j])
	}
	return out, nil
}

// transferTargetCurrent reports whether name's target still matches the
// metadata its preparation read. Call it holding name's stripe.
func (s *Store) transferTargetCurrent(name string, expected Metadata, unchanged bool) (bool, error) {
	if unchanged && !s.transferRereadMetadata {
		s.transferApply.epochValidated.Add(1)
		return true, nil
	}
	current, err := metadataFrom(s.db, name)
	switch {
	case errors.Is(err, ErrNotFound):
		return expected.ID == 0, nil
	case err != nil:
		return false, err
	default:
		return sameTransferTarget(current, expected), nil
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
