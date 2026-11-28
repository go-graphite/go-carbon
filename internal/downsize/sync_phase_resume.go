package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Phase positions describe scheduling, never clean-data proof. The journal
// identity binds them to the donor/catalog/window scope. Per-name receipts and
// mutation seals still decide reuse within the current verification phase.
type syncPhasePosition struct {
	Phase    string                `json:"phase"`
	Round    int                   `json:"round,omitempty"`
	Done     int64                 `json:"done"`
	Total    int64                 `json:"total"`
	Complete bool                  `json:"complete,omitempty"`
	Options  string                `json:"options"`
	Summary  syncCheckpointSummary `json:"summary"`
}

func phaseOrder(phase string, round int) int {
	switch phase {
	case "":
		return 0
	case "discover":
		return 1
	case "copy", "window missing":
		return 2
	case "window retry":
		return 3
	case "verify":
		return 4 + 2*round
	case "fill":
		return 5 + 2*round
	case "complete":
		return int(^uint(0) >> 1)
	}
	return -1
}

func parseSyncPhase(phase string, round int) (string, int) {
	if phase == "ready" {
		return "complete", round
	}
	if strings.HasPrefix(phase, "verify round ") {
		if _, err := fmt.Sscanf(phase, "verify round %d", &round); err != nil {
			return "invalid", 0
		}
		return "verify", round
	}
	return phase, round
}

func (s *syncer) phaseBeforeResume(phase string, round int) bool {
	order, saved := phaseOrder(phase, round), phaseOrder(s.resume.Phase, s.resume.Round)
	return order < saved || order == saved && s.resume.Complete
}

func (s *syncer) phaseSummary() syncCheckpointSummary {
	return syncCheckpointSummary{s.sum.Discovered, s.sum.OnTarget, s.sum.Missing, s.sum.NotOwned, s.sum.Short, s.both, s.sum.Copied, s.sum.CopyFailed, append([]syncResidual(nil), s.sum.residual...)}
}

func (s *syncer) savePhase(phase string, round, done, total int) error {
	p := syncPhasePosition{Phase: phase, Round: round, Done: int64(done), Total: int64(total), Options: syncCheckpointOptions(s.o), Summary: s.phaseSummary()}
	return s.j.append(syncRow{Kind: "phase-position", Position: &p})
}

func (s *syncer) beginPhase(phase string, round, total int) error {
	s.phaseMu.Lock()
	defer s.phaseMu.Unlock()
	if s.phaseBeforeResume(phase, round) {
		return nil
	}
	floor := int64(0)
	if phase == s.resume.Phase && (phase != "verify" && phase != "fill" || round == s.resume.Round) {
		floor = s.resume.Done
	}
	s.prog.mu.Lock()
	currentPhase, currentRound := parseSyncPhase(s.prog.phase, round)
	if phase == currentPhase && round == currentRound {
		floor = max(floor, s.prog.durableFloor, s.prog.done.Load()+s.prog.copyActive)
	}
	s.prog.mu.Unlock()
	if s.phaseJournal {
		if err := s.savePhase(phase, round, int(floor), total); err != nil {
			return err
		}
	}
	label := phase
	if phase == "verify" {
		label = fmt.Sprintf("verify round %d", round)
	}
	s.prog.beginReplay(label, total, floor)
	return nil
}

func (s *syncer) finishPhase() error {
	if !s.phaseJournal {
		return nil
	}
	s.phaseMu.Lock()
	defer s.phaseMu.Unlock()
	s.prog.mu.Lock()
	phase, round := parseSyncPhase(s.prog.phase, 0)
	if phase == "fill" {
		round = s.sum.Rounds + s.verificationBase
	}
	p := syncPhasePosition{Phase: phase, Round: round, Done: max(s.prog.done.Load(), s.prog.durableFloor), Total: int64(s.prog.total), Complete: true, Options: syncCheckpointOptions(s.o), Summary: s.phaseSummary()}
	s.prog.mu.Unlock()
	if s.phaseBeforeResume(phase, round) {
		return nil
	}
	return s.j.append(syncRow{Kind: "phase-position", Position: &p})
}

// Restore before either progress publisher starts. Legacy progress contributes
// only a display floor; entering a later phase proves traversal of earlier
// phases, but never proves a pending name clean.
func (s *syncer) restorePhase() error {
	s.phaseJournal = true
	var saved syncPhasePosition
	hasPosition := false
	for _, r := range s.j.rows {
		if (r.Kind == "phase-position" || r.Kind == "phase-scope" || r.Kind == "legacy-round-repair") && r.Position != nil {
			if phaseOrder(r.Position.Phase, r.Position.Round) < 1 || r.Position.Round < 0 || r.Position.Done < 0 || r.Position.Total < 0 {
				return errors.New("ambiguous journal phase or counters; refusing fresh start")
			}
			hasPosition = true
			saved = *r.Position
		}
		if r.Kind == "phase-progress" && r.Position != nil && r.Position.Phase == saved.Phase && r.Position.Round == saved.Round {
			saved.Done = max(saved.Done, r.Position.Done)
			saved.Total = r.Position.Total
		}
	}
	if saved.Phase != "" && !s.compatiblePhaseOptions(saved.Options) {
		return errors.New("phase execution scope changed (settled windows or native selection batch); retain the original options or use a new state directory for the new scope")
	}
	data, err := os.ReadFile(filepath.Join(s.o.state, "progress.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var old progressSnapshot
	if err == nil {
		if err = json.Unmarshal(data, &old); err != nil {
			return fmt.Errorf("ambiguous saved progress: %w", err)
		}
		phase, round := parseSyncPhase(old.Phase, old.Round)
		if phaseOrder(phase, round) < 0 || old.Done < 0 || old.Total < 0 {
			return errors.New("ambiguous saved phase or counters")
		}
		// Modern journal positions supersede stale snapshots from a previous phase.
		if !(phase == "verify" && s.legacyRepairScope(round) != nil) && phaseOrder(phase, round) >= phaseOrder(saved.Phase, saved.Round) {
			if phase == saved.Phase && round == saved.Round {
				saved.Done = max(saved.Done, old.Done)
			} else if saved.Phase == "" {
				saved.Phase, saved.Round, saved.Done, saved.Total = phase, round, old.Done, old.Total
			}
		}
	}
	// Legacy journals may have no progress snapshot, or an older snapshot. A
	// later round's durable receipt is stronger evidence than that observation.
	if saved.Phase == "" {
		saved.Phase = "discover"
	}
	for _, r := range s.j.rows {
		if hasPosition {
			break // explicit positions supersede legacy inference, including new scopes
		}
		phase, round := "", 0
		switch {
		case r.Kind == "copied" && (r.Phase == "missing" || r.Phase == "missing-batch"):
			phase = "copy"
			if s.windowOnly() {
				phase = "window missing"
			}
		case r.Kind == "verified":
			phase, round = "verify", r.Round
		case r.Kind == "copied" && r.Phase == "fill":
			phase, round = "fill", r.Round
		case r.Kind == "window-retry-visited" || r.Kind == "window-retry-floor":
			phase = "window retry"
		}
		if phaseOrder(phase, round) > phaseOrder(saved.Phase, saved.Round) {
			saved = syncPhasePosition{Phase: phase, Round: round}
		}
	}
	if !hasPosition && saved.Done == 0 {
		// Old clients persisted batch receipts before publishing progress. Recover
		// their durable cursor even if the observation file was lost in the kill.
		var durable int64
		seen := map[string]bool{}
		var batches []syncRow
		for _, r := range s.j.rows {
			switch {
			case (saved.Phase == "copy" || saved.Phase == "window missing") && r.Kind == "copied" && (r.Phase == "missing" || r.Phase == "missing-batch"):
				key := r.Source + "\x00" + r.First
				if !seen[key] {
					batches = append(batches, r)
					seen[key] = true
				}
			case saved.Phase == "discover" && r.Kind == "discover_checkpoint" && r.Discovery != nil:
				durable = max(durable, int64(r.Discovery.Names))
			case saved.Phase == "window retry" && r.Kind == "window-retry-floor":
				durable = max(durable, int64(r.Names))
			case saved.Phase == "verify" && r.Kind == "verified" && r.Round == saved.Round:
				if !seen[r.First] {
					batches = append(batches, r)
					seen[r.First] = true
				}
			case saved.Phase == "fill" && r.Kind == "copied" && r.Phase == "fill" && r.Round == saved.Round:
				key := r.Source + "\x00" + r.First
				if !seen[key] {
					batches = append(batches, r)
					seen[key] = true
				}
			}
		}
		sort.Slice(batches, func(i, j int) bool {
			if batches[i].Source != batches[j].Source {
				return batches[i].Source < batches[j].Source
			}
			return batches[i].First < batches[j].First
		})
		for i, r := range batches {
			if i > 0 && r.Source == batches[i-1].Source && r.First <= batches[i-1].Last {
				return errors.New("ambiguous overlapping legacy phase receipts without saved progress; refusing counter reset")
			}
			durable += int64(r.Names)
		}
		saved.Done = max(saved.Done, durable)
	}
	s.resume = saved // retain the attempted position even when reporting a contradiction
	if s.adoptingLegacyRound() && s.legacyRepairScope(saved.Round) == nil {
		// READY-off defaults introduced source-only filtering after this legacy
		// union was entered. Restore its original catalog size, including when
		// an intermediate binary checkpointed a smaller total.
		if saved.Summary.Discovered > 0 {
			saved.Total = int64(saved.Summary.Discovered)
		}
		if s.j.checkpoint != nil {
			saved.Total = int64(s.j.checkpoint.Summary.Discovered)
		}
		for _, r := range s.j.rows {
			if r.Kind == "discovered" {
				saved.Total = int64(r.Names)
			}
		}
		s.resume = saved
	}
	// A ring scope can shrink an entered legacy round without changing its
	// immutable catalog. Restore the narrowed total before either publisher
	// starts; the legacy catalog size must not inflate it again on restart.
	if saved.Phase == "verify" {
		for _, r := range s.j.rows {
			if r.Kind == "ownership-scope" && r.Round == saved.Round {
				saved.Total = int64(r.Names)
			}
		}
		s.resume = saved
	}
	var verifyReceipts int64
	if saved.Phase == "verify" {
		seen := map[[2]string]bool{}
		for _, r := range s.j.rows {
			key := [2]string{r.First, r.Last}
			if (r.Kind == "verified" || r.Kind == "receipt-reverified") && r.Round == saved.Round && !seen[key] {
				verifyReceipts += int64(r.Names)
				seen[key] = true
			}
		}
		if saved.Done > verifyReceipts {
			return s.receiptContradiction("saved verify done exceeds journal receipts")
		}
	}
	if saved.Total > 0 && saved.Done > saved.Total {
		return s.receiptContradiction("saved done exceeds phase total")
	}
	if phaseOrder(saved.Phase, saved.Round) < 0 {
		return errors.New("unknown journal phase")
	}
	// Discovery's completed row is sufficient to restore its summary without a
	// list call. A phase-position additionally preserves provisional residuals.
	for _, r := range s.j.rows {
		if r.Kind == "discovered" {
			s.sum.Discovered, s.sum.Missing, s.sum.NotOwned, s.sum.OnTarget, s.both = r.Names, r.Missing, r.NotOwned, r.OnTarget, r.Both
		}
	}
	if saved.Summary.Discovered > 0 {
		c := saved.Summary
		s.sum.Discovered, s.sum.Missing, s.sum.NotOwned, s.sum.OnTarget, s.sum.Short, s.both = c.Discovered, c.Missing, c.NotOwned, c.OnTarget, c.Short, c.Both
		s.sum.Copied, s.sum.CopyFailed = c.Copied, c.CopyFailed
		s.sum.residual = append([]syncResidual(nil), c.Residual...)
	}
	if phaseOrder(saved.Phase, saved.Round) >= 3 {
		for _, r := range s.j.rows {
			if r.Kind == "copied" && (r.Phase == "missing" || r.Phase == "missing-batch") {
				s.legacyCopyReceipts = s.windowOnly()
				for _, n := range r.Selected {
					s.copied[n] = true
				}
			}
		}
	}
	if saved.Phase == "" {
		saved.Phase = "discover"
	}
	s.resume = saved
	label := saved.Phase
	if label == "verify" {
		label = fmt.Sprintf("verify round %d", saved.Round)
	}
	s.prog.beginReplay(label, int(saved.Total), saved.Done)
	if !hasPosition {
		p := saved
		p.Options = syncCheckpointOptions(s.o)
		p.Summary = s.phaseSummary()
		if err := s.j.append(syncRow{Kind: "phase-position", Position: &p}); err != nil {
			return err
		}
	}
	if saved.Phase != "" {
		event("sync_phase_resumed", map[string]any{"phase": label, "round": saved.Round, "done": saved.Done, "complete": saved.Complete})
	}
	return nil
}

// A successful proof remains complete until a holder's mutation/schema fence
// changes. Check metadata in a separate scope-validation step, then schedule
// only invalidated names as new work; never reopen a completed data phase.
func (s *syncer) reopenChangedScope(ctx context.Context) (changed bool, err error) {
	// A finished full-history pass is a report, not a live convergence job.
	if !s.o.rescanAll && !s.o.readyFrontier && !s.windowOnly() {
		return false, nil
	}
	if _, ok := s.b.(syncProofBackend); !ok {
		return false, nil
	}
	if err = s.openFrontier(ctx); err != nil {
		return false, err
	}
	if s.frontier == nil {
		return false, nil
	}
	defer func() {
		if !changed {
			err = errors.Join(err, s.frontier.file.Close())
			s.frontier = nil
		}
	}()
	source := s.unionSource()
	defer source.close()
	for {
		var names []string
		var ids []int64
		for len(names) < s.o.batch {
			name, mask, e := source.next()
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				return false, e
			}
			if !s.ownsSyncName(name, mask) {
				continue
			}
			names = append(names, name)
			ids = append(ids, source.id())
		}
		if len(names) == 0 {
			break
		}
		stamps, e := s.proofStamps(ctx, names)
		if e != nil {
			return false, e
		}
		for i, name := range names {
			var record [32]byte
			if _, e := s.frontier.file.ReadAt(record[:], ids[i]*32); e != nil && !errors.Is(e, io.EOF) {
				return false, e
			}
			if stamps[i] == (syncProof{}) || record != s.frontier.record(name, stamps[i]) {
				s.newScope = append(s.newScope, name)
			}
		}
	}
	if len(s.newScope) == 0 {
		return false, nil
	}
	s.verificationBase = s.resume.Round
	if err = s.j.append(syncRow{Kind: "verification-retry", Round: s.verificationBase, Selected: s.newScope, SelectionComplete: true, InvalidationReason: "completed_scope_mutation_or_schema_changed"}); err != nil {
		return false, err
	}
	if err = s.startPhaseScope(syncPhasePosition{Phase: "verify", Round: s.verificationBase + 1, Total: int64(len(s.newScope))}, "completed_scope_mutation_or_schema_changed"); err != nil {
		return false, err
	}
	event("sync_scope_changed", map[string]any{"reason": "holder mutation or schema fence changed", "names": len(s.newScope), "round": s.resume.Round, "prior_completed_phases_retained": true})
	return true, nil
}

// Only an explicitly named new work scope can reset the monotonic journal
// guard. The old scope's receipts remain immutable and locally replayable.
func (s *syncer) startPhaseScope(p syncPhasePosition, reason string) error {
	s.phaseMu.Lock()
	defer s.phaseMu.Unlock()
	p.Options, p.Summary = syncCheckpointOptions(s.o), s.phaseSummary()
	if err := s.j.append(syncRow{Kind: "phase-scope", Position: &p, InvalidationReason: reason}); err != nil {
		return err
	}
	s.resume = p
	label := p.Phase
	if p.Phase == "verify" {
		label = fmt.Sprintf("verify round %d", p.Round)
	}
	s.prog.beginReplay(label, int(p.Total), p.Done)
	return nil
}

// Caller holds j.mu. Cache the authoritative row rather than rescanning a
// multi-gigabyte journal at every progress tick.
func (j *syncJournal) observePhase(r syncRow, row int) {
	if r.Position == nil {
		return
	}
	if r.Kind == "phase-position" || r.Kind == "phase-scope" || r.Kind == "legacy-round-repair" || r.Kind == "phase-progress" && j.phaseGuard != nil && r.Position.Phase == j.phaseGuard.Phase && r.Position.Round == j.phaseGuard.Round {
		p := *r.Position
		j.phaseGuard, j.phaseGuardRow = &p, row
	}
}

func (j *syncJournal) guardPhase(r syncRow) error {
	if r.Position == nil || r.Kind != "phase-position" && r.Kind != "phase-progress" {
		return nil
	}
	if j.phaseGuard == nil {
		for i, old := range j.rows {
			j.observePhase(old, i+1)
		}
	}
	if j.phaseGuard == nil {
		return nil
	}
	saved, next := *j.phaseGuard, *r.Position
	same := saved.Phase == next.Phase && saved.Round == next.Round
	if phaseOrder(next.Phase, next.Round) >= phaseOrder(saved.Phase, saved.Round) && (!same || next.Done >= saved.Done) && (r.Kind != "phase-progress" || same) {
		return nil
	}
	err := fmt.Errorf("phase regression refused: %s record %d saved %s round %d done %d; attempted %s round %d done %d", j.file.Name(), j.phaseGuardRow, saved.Phase, saved.Round, saved.Done, next.Phase, next.Round, next.Done)
	denied := syncRow{Kind: "phase-regression", InvalidationReason: err.Error()}
	data, _ := json.Marshal(denied)
	_, writeErr := j.file.Write(append(data, '\n'))
	if writeErr == nil {
		j.rows = append(j.rows, denied)
		j.progress.apply(denied)
		writeErr = j.file.Sync()
	}
	event("sync_phase_regression_refused", map[string]any{"level": "ERROR", "error": err.Error(), "record": j.phaseGuardRow, "phase_regressions": j.progress.PhaseRegressions})
	return errors.Join(err, writeErr)
}

func (s *syncer) hasPendingWindowSelections() bool {
	if !s.windowOnly() {
		return false
	}
	resolved := s.resolvedWindowSelections()
	for _, r := range s.j.rows {
		if r.Kind != "window-selected" {
			continue
		}
		for _, skip := range r.Skipped {
			if !resolved[syncWindowSelectionKey{r.Round, r.Source}][skip.Name] && !s.o.issues.excluded(skip.Name) {
				return true
			}
		}
	}
	return false
}

// Stream budgets and verification batch boundaries do not invalidate a data
// proof. Native missing-window selections alone freeze their batching contract.
// Windows saved before they settled match the requested windows clipped at the
// older horizon; settling more of the requested period is not a scope change.
func (s *syncer) compatiblePhaseOptions(options string) bool {
	var saved, current struct {
		Settle  int64
		Batch   int
		Windows syncRanges
	}
	if json.Unmarshal([]byte(options), &saved) != nil || json.Unmarshal([]byte(syncCheckpointOptions(s.o)), &current) != nil {
		return false
	}
	if saved.Settle != current.Settle || !s.settledWindowsCompatible(saved.Windows) {
		return false
	}
	if saved.Batch != current.Batch {
		for _, r := range s.j.rows {
			if r.Kind == "window-policy" {
				return false
			}
		}
	}
	return true
}
