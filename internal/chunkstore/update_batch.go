package chunkstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/lockset"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// MetricUpdate is one ordered input batch for an existing continuation metric.
type MetricUpdate struct {
	Name   string
	Points []Point
}

// UpdateBatch commits several metric updates atomically, under the same sync
// policy as UpdateMany. No caller may acknowledge any input until it returns.
// ErrImportPending leaves the whole input untouched for later retry.
// Each metric keeps a small private index; combining their representations
// avoids making every codec read and write search a group-sized index.
// Acquire the complete stripe set without sleeping on an incomplete set;
// ordinary writers only hold one. Duplicate names see preceding writes in
// the batch and advance their revision in input order.
func (s *Store) UpdateBatch(ctx context.Context, updates []MetricUpdate, maintenance *ContinuationMaintenance) error {
	timer := stagetime.FromContext(ctx)
	timer.Enter(stagetime.Route)
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	// A missing name takes the single-update create route. Check the complete
	// catalog's negative proof before marking the other metrics pending.
	for _, update := range updates {
		if !s.catalogMembership.mayContain(update.Name) {
			return ErrNotFound
		}
	}
	stripes := make([]uint64, 0, len(updates))
	names := make(map[string]bool, len(updates))
	for _, update := range updates {
		stripes = append(stripes, s.metricStripe(update.Name))
		names[update.Name] = true
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	timer.Enter(stagetime.LockWait)
	for {
		lockset.Lock(s.metricMu[:], stripes)
		var pending pendingMutation
		for name := range names {
			if pending = s.importPublishing[s.metricStripe(name)][name]; pending.done != nil {
				break
			}
		}
		if pending.done == nil {
			break
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
		if !pending.update {
			return ErrImportPending
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pending.done:
		}
	}
	timer.Enter(stagetime.Route)
	for name := range names {
		s.beginStateMutation(name)
	}
	unlock := func() {
		for name := range names {
			s.finishStateMutation(name)
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}
	defer func() { unlock() }()
	type prepared struct {
		meta    Metadata
		state   *metricState
		view    *stateView
		batch   *pebble.Batch
		sidecar *SidecarSummary
	}
	states := make(map[string]prepared, len(names))
	batch := s.db.NewBatch()
	defer batch.Close()
	defer func() {
		for _, p := range states {
			if p.batch != nil {
				p.batch.Close()
			}
		}
	}()
	defer timer.Enter(stagetime.Publish)
	// Exact-name reservations preserve the latest committed states without
	// retaining sibling stripes across cache decoding or database reads.
	reserved := make([]string, 0, len(names))
	for name := range names {
		reserved = append(reserved, name)
	}
	unlock = s.releaseUpdateStripes(reserved)
	timer.Enter(stagetime.Route)
	for name := range names {
		p := prepared{}
		p.state, p.meta = s.lockedState(name)
		var err error
		if p.meta.ID == 0 {
			p.meta, err = metadataFrom(s.db, name)
		}
		if err != nil {
			return fmt.Errorf("prepare update %s: %w", name, err)
		}
		if err := s.checkFileOwnership(p.meta); err != nil {
			return err
		}
		if !p.meta.Continuation || s.continuation == nil {
			return ErrFormat
		}
		p.batch = s.db.NewIndexedBatch()
		states[name] = p
	}
	var expiry proofExpiry
	changed := false
	for _, update := range updates {
		timer.Enter(stagetime.Route)
		p := states[update.Name]
		if p.view != nil {
			p.state = p.view.final(p.meta)
		}
		m, view, sidecar, wrote, err := s.prepareContinuationUpdate(ctx, p.batch, p.meta, p.state, true, update.Points, -1, maintenance, &expiry)
		if err != nil {
			return fmt.Errorf("prepare update %s: %w", update.Name, err)
		}
		p.meta, p.view = m, view
		if sidecar != nil {
			p.sidecar = sidecar
		}
		states[update.Name] = p
		changed = changed || wrote
	}
	timer.Enter(stagetime.Commit)
	var sidecars []SidecarRecord
	for _, p := range states {
		if err := batch.Apply(p.batch, nil); err != nil {
			return err
		}
		if p.sidecar != nil {
			sidecars = append(sidecars, SidecarRecord{Name: p.meta.Name, SidecarSummary: *p.sidecar})
		}
	}
	if changed {
		if err := s.commitSidecarRecords(batch, sidecars, s.commit); err != nil {
			return err
		}
	}
	s.recordProofExpiry(expiry)
	timer.Enter(stagetime.Publish)
	for _, p := range states {
		s.installState(p.view, p.meta)
	}
	return nil
}
