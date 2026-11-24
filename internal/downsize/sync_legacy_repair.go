package downsize

import "fmt"

// The transition is a new receipt; original union scopes and observations remain
// immutable. Its exact selection survives restarts and spilled checkpoints.
func (s *syncer) legacyRepairScope(round int) *syncRow {
	s.j.mu.Lock()
	defer s.j.mu.Unlock()
	for i := range s.j.rows {
		r := s.j.rows[i]
		if r.Kind == "legacy-round-repair" && r.Round == round {
			return &r
		}
	}
	return nil
}

func (s *syncer) prepareLegacyRepair(set []string, scoped bool, filled map[syncKey]bool) ([]string, bool, error) {
	round := s.resume.Round
	if s.o.rescanAll || s.windowOnly() || s.o.readyFrontier || s.resume.Phase != "verify" || round-s.verificationBase < 2 {
		return set, false, nil
	}
	s.phaseMu.Lock()
	defer s.phaseMu.Unlock()
	transition := s.legacyRepairScope(round)
	created := transition == nil
	if transition == nil {
		if scoped || !s.adoptingLegacyRound() {
			return set, false, nil
		}
		previous, err := s.legacyRoundReceipts(round-1, false, false)
		if err != nil {
			return nil, false, err
		}
		if len(previous.rows) == 0 {
			return nil, false, fmt.Errorf("legacy repair round %d: preceding round has no observations", round)
		}
		wanted := map[string]bool{}
		previous.observations(func(r syncRow) {
			for _, h := range r.Holes {
				wanted[h.Name] = true
			}
			for _, k := range r.Skipped {
				wanted[k.Name] = true
			}
			for _, n := range r.Unindexed {
				wanted[n] = true
			}
		})
		for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "fill" && r.Round == round-1 }) {
			for _, n := range r.Selected {
				wanted[n] = true
			}
			for _, n := range r.Failed {
				wanted[n] = true
			}
			for _, v := range r.Resolved {
				wanted[v.Name] = true
			}
		}
		set = sortedKeys(wanted)
		// Mark the safe full-union observations before appending a scope change.
		// This also covers a kill before the first repair observation is written.
		if rows := s.j.find(func(r syncRow) bool { return r.Kind == "legacy-round-adoption" && r.Round == round }); len(rows) == 0 {
			if err := s.j.append(syncRow{Kind: "legacy-round-adoption", Round: round}); err != nil {
				return nil, false, err
			}
		}
		p := syncPhasePosition{Phase: "verify", Round: round, Total: int64(len(set)), Options: syncCheckpointOptions(s.o), Summary: s.phaseSummary()}
		r := syncRow{Kind: "legacy-round-repair", Round: round, Phase: "repairs", Names: len(set), Selected: set, SelectionComplete: true, Position: &p}
		if err := s.j.append(r); err != nil {
			return nil, false, err
		}
		transition = &r
		event("sync_legacy_round_repair", map[string]any{"round": round, "from": "union", "to": "repairs", "union_names": s.sum.Discovered, "repair_names": len(set), "previous_receipts": len(previous.rows)})
	}
	// Keep successful fill history for residual classification without replaying
	// imports or reading any names outside the repair set.
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "fill" && r.Round == round-1 }) {
		if r.Forbidden {
			continue
		}
		bad := map[string]bool{}
		for _, n := range r.Failed {
			bad[n] = true
		}
		for _, n := range r.Selected {
			if !bad[n] {
				filled[syncKey{n, r.Source}] = true
			}
		}
	}
	if created {
		s.resume = *transition.Position
		s.prog.beginReplay(fmt.Sprintf("verify round %d", round), len(transition.Selected), 0)
	}
	return transition.Selected, true, nil
}

// Project range observations onto the exact repair membership. Clean current
// observations suppress earlier dirty results, and unrelated union names never
// become pending work or leak into the residual report.
func (p *legacyRoundProgress) restrict(names []string) *legacyRoundProgress {
	q := &legacyRoundProgress{rows: p.rows, owned: p.owned}
	if p.unscoped != nil {
		q.unscoped = p.unscoped.restrict(names)
	}
	for _, n := range names {
		if i := p.owner(n); i >= 0 {
			q.spans = append(q.spans, legacyRoundSpan{n, n + "\x00", i})
		}
	}
	return q
}
