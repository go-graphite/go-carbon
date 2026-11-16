package downsize

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"
)

// An old completed observation is the authority for its result, including holes.
// Missing modern metadata is not an invalidation. The journal identity already
// binds donors, target, inventory and windows; changing that identity is refused.
// Only recorded destructive changes or restart intervals can reopen this work.
func (s *syncer) adoptLegacyObservation(ctx context.Context, round int, names []string, ids []int64, prior syncRow, before, donorBefore []syncProof, proof string, targetInfos ...map[string][]syncArchive) (syncRow, bool, error) {
	if prior.Kind != "verified" || prior.Round != round || prior.Names != len(names) || prior.First != names[0] || prior.Last != names[len(names)-1] || !prior.LegacyObservation && (prior.MutationObserved || prior.ObservationProof != "" || len(prior.HoleProofs) != 0) {
		return syncRow{}, false, nil
	}
	observed, _ := time.Parse(time.RFC3339Nano, prior.Time)
	keyFor := func(r syncRow) string {
		if r.Identity != "" {
			return r.Identity
		}
		key, _ := gapfillKey([]any{r.Time, r.Invalidation})
		return key
	}
	invalidations := append([]syncRow(nil), s.legacyInvalidations...)
	expiry := maps.Clone(prior.LegacyExpiry)
	if expiry == nil {
		expiry = map[string]int64{}
	}
	// Expiration is independently recorded by the target, including its time.
	// Writes do not enter this ledger and must not invalidate an old comparison.
	if b, ok := s.b.(expirationEvidenceBackend); ok {
		evidence, err := b.ExpirationEvidence(ctx, s.o.to, names, true)
		if err != nil {
			return syncRow{}, true, err
		}
		var expired []string
		for _, n := range names {
			if e := evidence[n]; e.Receipt != nil && e.Receipt.DeletedAt >= observed.Unix() && e.Receipt.DeletedAt > 0 && e.Receipt.DeletedAt > expiry[n] {
				expiry[n] = e.Receipt.DeletedAt
				expired = append(expired, n)
			}
		}
		if len(expired) > 0 {
			r := syncRow{Kind: "verification-invalidated", Identity: rand.Text(), Invalidation: &syncInvalidation{Reason: "target_expiration", Names: expired}}
			if err := s.j.append(r); err != nil {
				return syncRow{}, true, err
			}
			r.Time = time.Now().UTC().Format(time.RFC3339Nano)
			invalidations = append(invalidations, r)
		}
	}
	// Once adopted, donor fences remain selective while target writes are
	// monotonic for hole coverage. Target disappearance is destructive.
	var targetInfo map[string][]syncArchive
	var err error
	if len(targetInfos) > 0 {
		targetInfo = targetInfos[0]
	} else {
		targetInfo, err = s.b.Info(ctx, s.o.to, names)
		if err != nil {
			return syncRow{}, true, err
		}
	}
	restartRecorded := false
	for _, r := range invalidations {
		if r.Invalidation == nil || r.Invalidation.Reason != "store_restart_gap" {
			continue
		}
		if slices.Contains(prior.LegacyInvalidations, keyFor(r)) {
			continue
		}
		at, _ := time.Parse(time.RFC3339Nano, r.Time)
		if !r.Invalidation.At.IsZero() {
			at = r.Invalidation.At
		}
		if !at.Before(observed) {
			restartRecorded = true
		}
	}
	var deleted, donorChanged []string
	for i, n := range names {
		if slices.Contains(prior.LegacyTargetPresent, n) && len(targetInfo[n]) == 0 {
			deleted = append(deleted, n)
		}
		if !restartRecorded && len(donorBefore) > i && donorBefore[i] != (syncProof{}) && prior.LegacyDonorProofs[n] != "" && prior.LegacyDonorProofs[n] != hex.EncodeToString(donorBefore[i][:]) {
			donorChanged = append(donorChanged, n)
		}
	}
	for reason, changed := range map[string][]string{"target_delete": deleted, "donor_change": donorChanged} {
		if len(changed) == 0 {
			continue
		}
		r := syncRow{Kind: "verification-invalidated", Identity: rand.Text(), Invalidation: &syncInvalidation{Reason: reason, Names: changed}}
		if err := s.j.append(r); err != nil {
			return syncRow{}, true, err
		}
		r.Time = time.Now().UTC().Format(time.RFC3339Nano)
		invalidations = append(invalidations, r)
	}
	type selection struct {
		full    bool
		windows syncRanges
	}
	selected := map[string]*selection{}
	reasons := map[string]bool{}
	applied := slices.Clone(prior.LegacyInvalidations)
	for _, r := range invalidations {
		if r.Invalidation == nil {
			continue
		}
		key := keyFor(r)
		if slices.Contains(prior.LegacyInvalidations, key) {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, r.Time)
		if !r.Invalidation.At.IsZero() {
			at, err = r.Invalidation.At, nil
		}
		if err != nil {
			return syncRow{}, true, fmt.Errorf("invalid verification invalidation time: %w", err)
		}
		if at.Before(observed) {
			continue
		}
		inv := r.Invalidation
		switch inv.Reason {
		case "target_delete", "target_expiration", "store_restart_gap", "donor_change":
		default:
			return syncRow{}, true, fmt.Errorf("unknown verification invalidation %q", inv.Reason)
		}
		for _, n := range names {
			if len(inv.Names) > 0 && !slices.Contains(inv.Names, n) {
				continue
			}
			var windows syncRanges
			for _, w := range inv.Windows {
				if w.From < 0 || w.Until <= w.From {
					return syncRow{}, true, fmt.Errorf("invalid recorded restart window: %+v", w)
				}
				scopes := s.o.windows
				if !s.windowOnly() && s.sampled(n) {
					scopes = append(slices.Clone(scopes), syncRange{Name: "history", From: 1, Until: s.verificationHorizon()})
				}
				for _, scope := range scopes {
					overlap := syncRange{Name: w.Name, From: max(w.From, scope.From), Until: min(w.Until, scope.Until, s.verificationHorizon())}
					if overlap.From < overlap.Until {
						windows = append(windows, overlap)
					}
				}
			}
			if len(inv.Windows) > 0 && len(windows) == 0 {
				continue
			}
			if inv.Reason == "store_restart_gap" && len(inv.Windows) == 0 {
				return syncRow{}, true, fmt.Errorf("restart invalidation requires a bounded window")
			}
			p := selected[n]
			if p == nil {
				p = &selection{}
				selected[n] = p
			}
			p.full = p.full || len(inv.Windows) == 0
			p.windows = append(p.windows, windows...)
			reasons[inv.Reason] = true
			if !slices.Contains(applied, key) {
				applied = append(applied, key)
			}
		}
	}
	row := prior
	row.LegacyInvalidations = applied
	if row.LegacyObservationTime == "" {
		row.LegacyObservationTime = prior.Time
	}
	row.LegacyExpiry = expiry
	if len(selected) > 0 {
		row.Identity = ""
		row.LegacyObservationTime = ""
	} // new destructive work gets a fresh repair generation
	row.Work = nil
	row.LegacyRevalidated = 0
	row.LegacyAdopted, row.LegacyReverified = 0, len(selected)
	if !prior.LegacyObservation {
		row.LegacyAdopted = len(names) - len(selected)
	}
	row.LegacyObservation = true
	row.MutationObserved = true
	row.LegacyTargetPresent = nil
	row.LegacyDonorProofs = map[string]string{}
	for i, n := range names {
		if len(targetInfo[n]) > 0 {
			row.LegacyTargetPresent = append(row.LegacyTargetPresent, n)
		}
		if len(donorBefore) > i && donorBefore[i] != (syncProof{}) {
			row.LegacyDonorProofs[n] = hex.EncodeToString(donorBefore[i][:])
		} else if old := prior.LegacyDonorProofs[n]; old != "" {
			row.LegacyDonorProofs[n] = old
		}
	}
	row.Reused = len(names) - len(selected)
	row.Holes = nil
	row.Unindexed = nil
	row.Short = nil
	row.Skipped = nil
	row.Resolved = nil
	for _, h := range prior.Holes {
		p := selected[h.Name]
		if p == nil {
			row.Holes = append(row.Holes, h)
			continue
		}
		if p.full {
			continue
		}
		parts := []syncHole{h}
		for _, w := range p.windows {
			var kept []syncHole
			for _, part := range parts {
				if part.Last < w.From || part.First >= w.Until {
					kept = append(kept, part)
					continue
				}
				if part.First < w.From {
					left := part
					left.Last = (w.From - 1) / part.Step * part.Step
					left.Points = min(left.Points, int((left.Last-left.First)/left.Step+1))
					kept = append(kept, left)
				}
				if part.Last >= w.Until {
					right := part
					right.First = (w.Until + part.Step - 1) / part.Step * part.Step
					right.Points = min(right.Points, int((right.Last-right.First)/right.Step+1))
					kept = append(kept, right)
				}
			}
			parts = kept
		}
		row.Holes = append(row.Holes, parts...)
	}
	keep := func(n string) bool { return selected[n] == nil || !selected[n].full }
	for _, n := range prior.Unindexed {
		if keep(n) {
			row.Unindexed = append(row.Unindexed, n)
		}
	}
	for _, n := range prior.Short {
		if keep(n) {
			row.Short = append(row.Short, n)
		}
	}
	for _, k := range prior.Skipped {
		if keep(k.Name) {
			row.Skipped = append(row.Skipped, k)
		}
	}
	for _, k := range prior.Resolved {
		if keep(k.Name) {
			row.Resolved = append(row.Resolved, k)
		}
	}
	var full []string
	type windowBatch struct {
		window syncRange
		names  []string
	}
	var batches []windowBatch
	for _, n := range names {
		p := selected[n]
		if p == nil {
			continue
		}
		redoReason(ctx, n, "legacy_observation_invalidated")
		if p.full {
			full = append(full, n)
			continue
		}
		for _, w := range p.windows {
			i := slices.IndexFunc(batches, func(b windowBatch) bool { return b.window.From == w.From && b.window.Until == w.Until })
			if i < 0 {
				batches = append(batches, windowBatch{window: w})
				i = len(batches) - 1
			}
			if !slices.Contains(batches[i].names, n) {
				batches[i].names = append(batches[i].names, n)
			}
		}
	}
	if len(full) > 0 {
		fresh, err := s.verifyBatch(ctx, round, full)
		if err != nil {
			return row, true, err
		}
		row.Holes = append(row.Holes, fresh.Holes...)
		row.Unindexed = append(row.Unindexed, fresh.Unindexed...)
		row.Short = append(row.Short, fresh.Short...)
		row.Skipped = append(row.Skipped, fresh.Skipped...)
		row.Resolved = append(row.Resolved, fresh.Resolved...)
	}
	for _, b := range batches {
		fresh := syncRow{}
		if err := s.checkWindow(ctx, &fresh, b.names, nil, b.window); err != nil {
			return row, true, err
		}
		for _, h := range fresh.Holes {
			h.Window = b.window.Name
			row.Holes = append(row.Holes, h)
		}
		row.Skipped = append(row.Skipped, fresh.Skipped...)
	}
	after := before
	if s.frontier != nil {
		// A missing before fence cannot be sealed by any after value. Do not
		// repeat metadata work for names whose observation remains unsealed.
		after = make([]syncProof, len(names))
		var sealNames []string
		var sealPositions []int
		for i, stamp := range before {
			if stamp != (syncProof{}) {
				sealNames = append(sealNames, names[i])
				sealPositions = append(sealPositions, i)
			}
		}
		if len(sealNames) > 0 {
			stamps, _, err := s.proofStampsVersion(ctx, sealNames)
			if err != nil {
				return row, true, err
			}
			for i, stamp := range stamps {
				after[sealPositions[i]] = stamp
			}
		}
	}
	// Seal only stable metadata. Unknown holders retain the adopted observation,
	// without manufacturing a clean frontier or a READY proof for that name.
	row.ObservationProof = proof
	row.HoleProofs = nil
	dirty := map[string]bool{}
	for _, h := range row.Holes {
		dirty[h.Name] = true
	}
	for _, n := range row.Unindexed {
		dirty[n] = true
	}
	for _, n := range row.Short {
		dirty[n] = true
	}
	for _, k := range row.Skipped {
		dirty[k.Name] = true
	}
	for _, k := range row.Resolved {
		dirty[k.Name] = true
	}
	var records []byte
	contiguous := len(ids) == len(names) && len(ids) > 0
	for i, id := range ids {
		contiguous = contiguous && id == ids[0]+int64(i)
	}
	if s.frontier != nil && contiguous {
		records = make([]byte, len(ids)*32)
		if _, err := s.frontier.file.ReadAt(records, ids[0]*32); err != nil && !errors.Is(err, io.EOF) {
			return row, true, err
		}
	}
	changedRecords := false
	for i, n := range names {
		if s.frontier == nil {
			break
		}
		if before[i] != after[i] || after[i] == (syncProof{}) {
			row.ObservationProof = ""
			continue
		}
		if dirty[n] {
			continue
		}
		record := s.frontier.record(n, after[i])
		if records != nil {
			copy(records[i*32:], record[:])
			changedRecords = true
		} else {
			if _, err := s.frontier.file.WriteAt(record[:], ids[i]*32); err != nil {
				return row, true, err
			}
			changedRecords = true
		}
	}
	if records != nil && changedRecords {
		if _, err := s.frontier.file.WriteAt(records, ids[0]*32); err != nil {
			return row, true, err
		}
	}
	if len(row.Unindexed)+len(row.Short)+len(row.Skipped)+len(row.Resolved) > 0 {
		row.ObservationProof = ""
	}
	if row.ObservationProof != "" {
		for i, n := range names {
			if dirty[n] {
				if row.HoleProofs == nil {
					row.HoleProofs = map[string]string{}
				}
				row.HoleProofs[n] = hex.EncodeToString(after[i][:])
			}
		}
	}
	// Persist proof bytes before the observation which claims their adoption.
	if s.frontier != nil && changedRecords {
		if err := s.frontier.file.Sync(); err != nil {
			return row, true, err
		}
	}
	reasonNames := make([]string, 0, len(reasons))
	for r := range reasons {
		reasonNames = append(reasonNames, r)
	}
	sort.Strings(reasonNames)
	event("sync_legacy_observation", map[string]any{"round": round, "first": names[0], "last": names[len(names)-1], "legacy_observation_adopted": row.LegacyAdopted, "legacy_observation_reverified": row.LegacyReverified, "reasons": reasonNames})
	return row, true, nil
}

// Existing READY recovery checkpoints are already durable restart-gap records.
// Import their bounded windows before legacy replay; READY still owns backfill,
// all-donor parity and the final attestation. The original checkpoint is retained.
func (s *syncer) loadLegacyInvalidations() error {
	s.legacyInvalidations = s.j.find(func(r syncRow) bool { return r.Kind == "verification-invalidated" })
	path := filepath.Join(s.o.state, "ready-recovery.json")
	var recovery readyRecovery
	if err := readReadyObservationJSON(path, &recovery); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	identity, err := syncIdentity(s.o)
	if err != nil {
		return err
	}
	if recovery.Identity != identity {
		return errors.New("recorded restart recovery scope changed")
	}
	windows := recovery.Restarts
	if len(windows) == 0 && recovery.RestartWindow {
		windows = recovery.Windows
	}
	if len(windows) == 0 {
		return nil
	}
	key, err := gapfillKey([]any{"legacy-restart", windows})
	if err != nil {
		return err
	}
	for _, r := range s.legacyInvalidations {
		if r.Identity == key {
			return nil
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	// Filesystem timestamps may lag publication within one clock tick. Treat
	// that final second conservatively; applied event IDs prevent repeat work.
	at := info.ModTime().Truncate(time.Second).Add(time.Second)
	r := syncRow{Kind: "verification-invalidated", Identity: key, Invalidation: &syncInvalidation{Reason: "store_restart_gap", Windows: windows, At: at}}
	if err := s.j.append(r); err != nil {
		return err
	}
	// The event happened when the original checkpoint was published, not when
	// this executable imported it. Older completed observations alone reopen.
	r.Time = info.ModTime().UTC().Format(time.RFC3339Nano)
	s.legacyInvalidations = append(s.legacyInvalidations, r)
	return nil
}
