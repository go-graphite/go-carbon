package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

func (s *syncer) initOwnership() error {
	if s.o.ring == "" {
		return nil
	}
	owner, err := ringplan.TargetOwner(s.o.ring, s.o.to)
	if err == nil {
		s.owner = owner
	}
	return err
}

// Ownership narrows backend work, never the immutable catalog or its line IDs.
// Completed observations retain their accounting even when their names are now
// outside the target's responsibility. Unproven exclusions reduce only Total.
func (s *syncer) ownsSyncName(name string, mask uint64) bool {
	return s.owner == nil || mask != 0 && s.owner(name)
}

func (s *syncer) narrowOwnedRound(ctx context.Context, round int, input *syncNameSource, receipts *legacyRoundProgress) error {
	if s.owner == nil || input.rewind == nil {
		return nil
	}
	defer input.rewind()
	if receipts != nil {
		defer func() {
			receipts.cursor = 0
			if receipts.unscoped != nil {
				receipts.unscoped.cursor = 0
			}
		}()
	}
	excluded, retained := 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, mask, err := input.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if s.ownsSyncName(name, mask) {
			continue
		}
		if receipts.contains(name, mask) {
			retained++
		} else {
			excluded++
		}
	}
	total := input.total - excluded
	if round == s.resume.Round && s.resume.Phase == "verify" && int64(total) < s.resume.Done {
		return fmt.Errorf("ownership scope would lower published progress: done %d exceeds total %d", s.resume.Done, total)
	}
	input.total = total
	// A per-round gauge, not an increment on every restart. The durable receipt
	// also makes exclusions visible when the process exits before its next tick.
	if err := s.j.append(syncRow{Kind: "ownership-scope", Round: round, Names: total, NotOwned: excluded, OnTarget: retained}); err != nil {
		return err
	}
	event("sync_ownership_scope", map[string]any{"round": round, "excluded_pending": excluded, "retained_done_non_owned": retained, "total": total, "saved_done": s.resume.Done})
	return nil
}

// Historical fill reconstruction keeps its original selection/identity. Only
// the active verification's results determine future repairs and residuals.
func (s *syncer) ownedResults(round int, holes []syncHole, unindexed []string, skipped []syncSkip) ([]syncHole, []string, []syncSkip) {
	if s.owner == nil {
		return holes, unindexed, skipped
	}
	if phaseOrder("verify", round) < phaseOrder(s.resume.Phase, s.resume.Round) {
		// A fill entered before this upgrade keeps its broad parent identity.
		// A fill scheduled after ownership narrowing must keep that narrowed
		// identity when a later round reconstructs its observations.
		narrowed := len(s.j.find(func(r syncRow) bool { return r.Kind == "ownership-scope" && r.Round == round })) > 0
		if !narrowed {
			return holes, unindexed, skipped
		}
	}
	hs := holes[:0]
	for _, h := range holes {
		if s.owner(h.Name) {
			hs = append(hs, h)
		}
	}
	ns := unindexed[:0]
	for _, n := range unindexed {
		if s.owner(n) {
			ns = append(ns, n)
		}
	}
	ks := skipped[:0]
	for _, k := range skipped {
		if s.owner(k.Name) {
			ks = append(ks, k)
		}
	}
	return hs, ns, ks
}

// Completed ring rounds can contain both original broad batches and newer
// sparse batches. Check coverage against stable catalog membership, rather than
// interpreting a changed batch boundary as permission to redo proven names.
func (s *syncer) replayOwnedRound(ctx context.Context, round int, input syncNameSource) ([]syncHole, []string, []syncSkip, error) {
	defer input.close()
	receipts, err := s.legacyRoundReceipts(round, true, false, true)
	if err != nil {
		return nil, nil, nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		n, mask, err := input.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, nil, err
		}
		if s.ownsSyncName(n, mask) && !receipts.contains(n, mask) {
			return nil, nil, nil, s.receiptContradiction("completed owned round has an unproven name")
		}
	}
	var holes []syncHole
	var unindexed []string
	var skipped []syncSkip
	receipts.observations(func(r syncRow) {
		holes = append(holes, r.Holes...)
		unindexed = append(unindexed, r.Unindexed...)
		skipped = append(skipped, r.Skipped...)
	})
	holes, unindexed, skipped = s.ownedResults(round, holes, unindexed, skipped)
	return holes, unindexed, skipped, nil
}
