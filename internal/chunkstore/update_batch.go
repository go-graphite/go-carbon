package chunkstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// MetricUpdate is one ordered input batch for an existing continuation metric.
type MetricUpdate struct {
	Name   string
	Points []Point
}

// UpdateBatch commits several metric updates atomically, under the same sync
// policy as UpdateMany. No caller may acknowledge any input until it returns.
// Each metric keeps a small private index; combining their representations
// avoids making every codec read and write search a group-sized index.
// Stripes are taken in order; ordinary writers only hold one. Duplicate names
// see preceding writes in the batch and advance their revision in input order.
func (s *Store) UpdateBatch(ctx context.Context, updates []MetricUpdate, maintenance *ContinuationMaintenance) error {
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
	timer := stagetime.FromContext(ctx)
	timer.Enter(stagetime.LockWait)
	stripes := make([]uint64, 0, len(updates))
	names := make(map[string]bool, len(updates))
	for _, update := range updates {
		stripes = append(stripes, s.metricStripe(update.Name))
		names[update.Name] = true
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, stripe := range stripes {
		s.metricMu[stripe].Lock()
	}
	for name := range names {
		s.beginStateMutation(name)
	}
	defer func() {
		for name := range names {
			s.finishStateMutation(name)
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}()
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
			p.batch.Close()
		}
	}()
	changed := false
	for _, update := range updates {
		timer.Enter(stagetime.Route)
		p, ok := states[update.Name]
		if !ok {
			var err error
			p.state, p.meta = s.lockedState(update.Name)
			if p.state == nil {
				p.meta, err = s.metadataLockedOrAbsent(update.Name)
			}
			if err != nil {
				return fmt.Errorf("prepare update %s: %w", update.Name, err)
			}
			if err := s.checkFileOwnership(p.meta); err != nil {
				return err
			}
			if !p.meta.Continuation || s.continuation == nil {
				return ErrFormat
			}
			p.batch = s.db.NewIndexedBatch()
			states[update.Name] = p
		} else if p.view != nil {
			p.state = p.view.final(p.meta)
		}
		m, view, sidecar, wrote, err := s.prepareContinuationUpdate(ctx, p.batch, p.meta, p.state, true, update.Points, -1, maintenance)
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
	for _, p := range states {
		s.installState(p.view, p.meta)
	}
	timer.Leave()
	return nil
}
