package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var errSyncNoProof = errors.New("store has no mutation proof endpoint")

type syncWindowProofBackend interface {
	WindowProofs(context.Context, string, []string, int64, int64) ([]string, error)
}

type syncWindowPresenceBackend interface {
	WindowProofsPresence(context.Context, string, []string, int64, int64) ([]string, []bool, error)
}

type syncProofBackend interface {
	Proofs(context.Context, string, []string) ([]string, error)
}

// Catalog line numbers are stable IDs. One 32-byte record per ID holds a
// 128-bit all-holder mutation fence and a 128-bit scope/name integrity seal.
// Zero, missing and torn records are pending. No metric names live in RAM or
// in a second catalog. Changing verification batch boundaries keeps the IDs.
type syncProof [16]byte
type syncFrontier struct {
	file  *os.File
	scope struct {
		Identity string
		Settle   time.Duration
		Now      int64
	}
	key string
}

func (s *syncer) openFrontier(ctx context.Context) error {
	b, ok := s.b.(syncProofBackend)
	if !ok {
		return nil
	}
	source := s.unionSource()
	name, _, err := source.next()
	source.close()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	// Negotiate every holder before adopting a fixed verification generation.
	// A legacy holder keeps the complete current verification path.
	// A legacy resume already negotiated and persisted this scope. Do not
	// issue a proof request for a completed name just to reopen its frontier.
	_, savedScopeErr := os.Stat(filepath.Join(s.o.state, "verification-scope.json"))
	legacyRound := s.adoptingLegacyRound()
	if legacyRound && errors.Is(savedScopeErr, os.ErrNotExist) {
		// The old round ran without a frontier. Preserve that mode until its
		// cursor is consumed, rather than negotiating against an earlier name.
		return nil
	}
	if !legacyRound || savedScopeErr != nil {
		for _, store := range append([]string{s.o.to}, s.o.from...) {
			got, err := b.Proofs(ctx, store, []string{name})
			if errors.Is(err, errSyncNoProof) {
				if _, e := os.Stat(filepath.Join(s.o.state, "verification-scope.json")); errors.Is(e, os.ErrNotExist) {
					return nil
				} else if e != nil {
					return e
				}
				continue
			}
			if err != nil {
				return err
			}
			if len(got) != 1 {
				return errors.New("invalid mutation proof response")
			}
		}
	}
	for _, w := range s.o.windows {
		if w.Until > s.o.now().Unix()-int64(s.o.settle.Seconds()) {
			return nil
		}
	}
	f := &syncFrontier{}
	f.scope.Identity, err = syncIdentity(s.o)
	if err != nil {
		return err
	}
	f.scope.Settle = s.o.settle
	f.scope.Now = s.o.now().Unix()
	path := filepath.Join(s.o.state, "verification-scope.json")
	data, err := os.ReadFile(path)
	if err == nil {
		expected := f.scope
		if err = json.Unmarshal(data, &f.scope); err != nil {
			return err
		}
		if f.scope.Identity != expected.Identity || f.scope.Settle != expected.Settle || f.scope.Now <= 0 || f.scope.Now > expected.Now {
			return errors.New("verification scope changed; use a fresh state directory")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err = atomicStateJSON(path, f.scope); err != nil {
			return err
		}
	} else {
		return err
	}
	data, _ = json.Marshal(f.scope)
	f.key = string(data)
	frontierPath := filepath.Join(s.o.state, "verification-frontier.bin")
	if s.o.readyFrontier {
		if _, e := os.Stat(filepath.Join(s.o.state, "ready-frontier.bin")); e == nil {
			frontierPath = filepath.Join(s.o.state, "ready-frontier.bin")
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	f.file, err = os.OpenFile(frontierPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	s.frontier = f
	return nil
}

func (s *syncer) verificationHorizon() int64 {
	now := s.o.now().Unix()
	if s.frontier != nil {
		now = min(now, s.frontier.scope.Now)
	}
	return now - int64(s.o.settle.Seconds())
}

// proofStamps reads only metadata. The serving-index schema participates too:
// a native metric without its serving-index entry is not a clean read proof.
func (s *syncer) proofStamps(ctx context.Context, names []string) ([]syncProof, error) {
	stamps, _, err := s.proofStampsVersion(ctx, names)
	return stamps, err
}

func (s *syncer) proofStampsVersion(ctx context.Context, names []string, donorOut ...*[]syncProof) ([]syncProof, bool, error) {
	var donors *[]syncProof
	if len(donorOut) > 0 {
		donors = donorOut[0]
	}
	return s.proofStampsMetadata(ctx, names, donors, nil)
}

func (s *syncer) proofStampsMetadata(ctx context.Context, names []string, donorOut *[]syncProof, targetInfo *map[string][]syncArchive) ([]syncProof, bool, error) {
	stores := append([]string{s.o.to}, s.o.from...)
	tokens := make([][]string, len(stores))
	errs := make([]error, len(stores))
	infos := make([]map[string][]syncArchive, len(stores))
	infoErrs := make([]error, len(stores))
	bounded := make([]bool, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		if _, ok := s.b.(syncWindowPresenceBackend); ok {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var absent []bool
				tokens[i], bounded[i], errs[i] = s.windowProofsVersion(ctx, store, names, &absent)
				if errs[i] != nil && !errors.Is(errs[i], errSyncNoProof) {
					return
				}
				requested := make([]string, 0, len(names))
				for j, n := range names {
					// An unknown donor token already makes both seals unknown;
					// its schema is never read by the hashing loop below. Target
					// presence is independently needed by legacy adoption.
					if i > 0 && len(tokens[i]) == len(names) && tokens[i][j] == "" {
						continue
					}
					if len(absent) != len(names) || !absent[j] {
						requested = append(requested, n)
					}
				}
				if len(requested) > 0 {
					infos[i], infoErrs[i] = s.b.Info(ctx, store, requested)
				}
			}()
			continue
		}
		wg.Add(2)
		go func() { defer wg.Done(); infos[i], infoErrs[i] = s.b.Info(ctx, store, names) }()
		go func() { defer wg.Done(); tokens[i], bounded[i], errs[i] = s.windowProofsVersion(ctx, store, names) }()
	}
	wg.Wait()
	for _, err := range infoErrs {
		if err != nil {
			return nil, false, err
		}
	}
	for i, err := range errs {
		if err != nil && !errors.Is(err, errSyncNoProof) {
			return nil, false, err
		}
		if err == nil && len(tokens[i]) != len(names) {
			return nil, false, errors.New("invalid mutation proof response")
		}
	}
	out := make([]syncProof, len(names))
	donors := make([]syncProof, len(names))
	for i, n := range names {
		hash := sha256.New()
		donorHash := sha256.New()
		fmt.Fprintln(donorHash, s.frontier.key, n)
		donorKnown := true
		fmt.Fprintln(hash, s.frontier.key, n)
		known := true
		for store := range stores {
			if errs[store] != nil || tokens[store][i] == "" {
				known = false
				if store > 0 {
					donorKnown = false
				}
				continue
			}
			fmt.Fprintln(hash, stores[store], tokens[store][i])
			schema, _ := json.Marshal(infos[store][n])
			hash.Write(schema)
			if store > 0 {
				fmt.Fprintln(donorHash, stores[store], tokens[store][i])
				donorHash.Write(schema)
			}
			// A fixed window can age into another serving archive without a write.
			// Its selected archive therefore participates in the mutation fence.
			for _, w := range s.o.windows {
				if w.From >= min(w.Until, s.verificationHorizon()) {
					continue
				}
				step := int64(-1)
				for _, a := range infos[store][n] {
					if a.Step*a.Points >= s.o.now().Unix()-w.From+1 {
						step = a.Step
						break
					}
				}
				fmt.Fprintln(hash, w.Name, step)
				if store > 0 {
					fmt.Fprintln(donorHash, w.Name, step)
				}
			}
		}
		if known {
			copy(out[i][:], hash.Sum(nil))
		}
		if donorKnown {
			copy(donors[i][:], donorHash.Sum(nil))
		}
	}
	if donorOut != nil {
		*donorOut = donors
	}
	if targetInfo != nil {
		*targetInfo = infos[0]
	}
	v2 := true
	for _, b := range bounded {
		v2 = v2 && b
	}
	return out, v2, nil
}

func (f *syncFrontier) record(name string, stamp syncProof) [32]byte {
	var record [32]byte
	if stamp == (syncProof{}) {
		return record
	}
	copy(record[:16], stamp[:])
	seal := sha256.Sum256(append([]byte(f.key+"\x00"+name+"\x00"), stamp[:]...))
	copy(record[16:], seal[:16])
	return record
}

// An observation can contain holes and still be completed work. Unlike the
// clean frontier, this seal preserves its result for this exact batch/round.
// Unknown proofs never authorize replay. All holders, schema, archive choice,
// catalog IDs and names participate, including the whole-metric revision guard.
func observationProof(names []string, ids []int64, stamps []syncProof) string {
	if len(names) != len(ids) || len(names) != len(stamps) {
		return ""
	}
	h := sha256.New()
	for i, name := range names {
		if stamps[i] == (syncProof{}) {
			return ""
		}
		fmt.Fprintf(h, "%d\x00%s\x00%x\n", ids[i], name, stamps[i])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *syncer) verifyFrontierBatch(ctx context.Context, round int, names []string, ids []int64, prior syncRow) (syncRow, error) {
	var donorBefore []syncProof
	var targetInfo map[string][]syncArchive
	before, v2, err := s.proofStampsMetadata(ctx, names, &donorBefore, &targetInfo)
	if !v2 {
		donorBefore = nil
	}
	if err != nil {
		return syncRow{}, err
	}
	proof := observationProof(names, ids, before)
	if proof != "" && v2 {
		proof = "v2:" + proof
	}
	if row, adopted, err := s.adoptLegacyObservation(ctx, round, names, ids, prior, before, donorBefore, proof, targetInfo); adopted || err != nil {
		return row, err
	}
	// The first v2 client used an unprefixed seal. Exact matching metadata
	// authenticates those observations too, without forcing another migration.
	matchingProof := prior.ObservationProof == proof || v2 && prior.ObservationProof == strings.TrimPrefix(proof, "v2:")
	if prior.Kind == "verified" && prior.Round == round && prior.Names == len(names) && prior.First == names[0] && prior.Last == names[len(names)-1] && proof != "" && matchingProof {
		prior.ObservationProof = proof
		if v2 && len(prior.Holes) > 0 {
			stamps := make(map[string]string, len(names))
			for i, name := range names {
				stamps[name] = hex.EncodeToString(before[i][:])
			}
			if prior.HoleProofs == nil {
				prior.HoleProofs = map[string]string{}
			}
			for _, hole := range prior.Holes {
				prior.HoleProofs[hole.Name] = stamps[hole.Name]
			}
		}
		prior.Reused = len(names)
		prior.LegacyAdopted, prior.LegacyReverified = 0, 0
		prior.Work = nil            // no backend data operation was performed by this replay
		prior.LegacyRevalidated = 0 // this append records reuse, not a second migration
		return prior, nil
	}
	// Catalog batches are contiguous. Read/write their compact records together
	// rather than issuing one filesystem operation per metric at catalog scale.
	var block []byte
	if span := ids[len(ids)-1] - ids[0] + 1; span > 0 && span <= int64(2*len(ids)) {
		block = make([]byte, span*32)
		if _, err := s.frontier.file.ReadAt(block, ids[0]*32); err != nil && !errors.Is(err, io.EOF) {
			return syncRow{}, err
		}
	}
	sameObservation := prior.Kind == "verified" && prior.Round == round && prior.Names == len(names) && prior.First == names[0] && prior.Last == names[len(names)-1]
	var preservedHoles []syncHole
	preservedProofs := map[string]string{}
	priorHoles := map[string][]syncHole{}
	if sameObservation {
		for _, h := range prior.Holes {
			priorHoles[h.Name] = append(priorHoles[h.Name], h)
		}
	}
	selected := make([]string, 0, len(names))
	positions := make([]int, 0, len(names))
	for i, n := range names {
		if sameObservation && v2 && before[i] != (syncProof{}) && prior.HoleProofs[n] == hex.EncodeToString(before[i][:]) {
			if holes := priorHoles[n]; len(holes) > 0 {
				preservedHoles = append(preservedHoles, holes...)
				preservedProofs[n] = prior.HoleProofs[n]
				continue
			}
		}
		var saved [32]byte
		if block != nil {
			copy(saved[:], block[(ids[i]-ids[0])*32:])
		} else if _, err := s.frontier.file.ReadAt(saved[:], ids[i]*32); err != nil && !errors.Is(err, io.EOF) {
			return syncRow{}, err
		}
		if before[i] != (syncProof{}) && saved == s.frontier.record(n, before[i]) {
			continue
		}
		if saved != ([32]byte{}) {
			var prior syncProof
			copy(prior[:], saved[:16])
			if saved != s.frontier.record(n, prior) {
				// A torn optional proof must never authorize reuse. Keep the existing
				// conservative read path, and make this lost reuse evidence loud.
				event("sync_proof_receipt_invalid", map[string]any{"level": "error", "metric": n, "reason": "corrupt or torn mutation receipt; revalidation required"})
			} else {
				reason := "mutation_proof_changed"
				if before[i] == (syncProof{}) {
					reason = "mutation_proof_unavailable"
				}
				redoReason(ctx, n, reason)
			}
		}
		selected = append(selected, n)
		positions = append(positions, i)
	}
	row := syncRow{Kind: "verified", Round: round, MutationObserved: true}
	if len(selected) > 0 {
		if prior.Kind == "verified" && prior.Round == round && prior.Names == len(names) && prior.First == names[0] && prior.Last == names[len(names)-1] {
			reason := "completed_observation_has_no_mutation_seal"
			if prior.ObservationProof != "" {
				reason = "observation_mutation_proof_changed"
				for _, name := range selected {
					redoReason(ctx, name, reason)
				}
			}
			event("sync_verification_reopened", map[string]any{"round": round, "reason": reason, "names": len(selected), "first": selected[0], "last": selected[len(selected)-1], "windows": s.o.windows, "full_history": !s.windowOnly()})
		}
		row, err = s.verifyBatch(ctx, round, selected)
		if err != nil {
			return row, err
		}
		row.MutationObserved = true
		after, afterV2, err := s.proofStampsVersion(ctx, selected)
		if err != nil {
			return row, err
		}
		if v2 != afterV2 {
			proof = ""
		}
		dirty := map[string]bool{}
		excluded := map[string]bool{}
		for _, h := range row.Holes {
			dirty[h.Name] = true
		}
		for _, n := range row.Unindexed {
			dirty[n] = true
			excluded[n] = true
		}
		for _, k := range row.Skipped {
			dirty[k.Name] = true
			excluded[k.Name] = true
		}
		// Eligibility/absence exclusions may depend on policy outside the native
		// metric config. Re-resolve them instead of persisting a clean data proof.
		for _, k := range row.Resolved {
			dirty[k.Name] = true
			excluded[k.Name] = true
		}
		for _, n := range row.Short {
			dirty[n] = true
			excluded[n] = true
		}
		for j, i := range positions {
			stamp := syncProof{}
			if before[i] != (syncProof{}) && before[i] != after[j] {
				row.Skipped = append(row.Skipped, syncSkip{names[i], "store changed during verification"})
				dirty[names[i]] = true
				excluded[names[i]] = true
			}
			if !dirty[names[i]] && before[i] == after[j] {
				stamp = after[j]
			}
			// The whole-batch seal requires every mutation fence to stay stable.
			// Individual known hole observations below survive unknown neighbors.
			if before[i] != after[j] {
				proof = ""
			}
			if v2 && afterV2 && !excluded[names[i]] && before[i] != (syncProof{}) && before[i] == after[j] {
				if dirty[names[i]] {
					if row.HoleProofs == nil {
						row.HoleProofs = map[string]string{}
					}
					row.HoleProofs[names[i]] = hex.EncodeToString(after[j][:])
				}
				if sameObservation && !strings.HasPrefix(prior.ObservationProof, "v2:") {
					row.LegacyRevalidated++
				}
			}
			record := s.frontier.record(names[i], stamp)
			if block != nil {
				copy(block[(ids[i]-ids[0])*32:], record[:])
			} else if _, err = s.frontier.file.WriteAt(record[:], ids[i]*32); err != nil {
				return row, err
			}
		}
		if block != nil {
			if _, err = s.frontier.file.WriteAt(block, ids[0]*32); err != nil {
				return row, err
			}
		}
	}
	if len(row.Unindexed) == 0 && len(row.Skipped) == 0 && len(row.Resolved) == 0 && len(row.Short) == 0 {
		row.ObservationProof = proof
	}
	if row.LegacyRevalidated > 0 {
		event("sync_legacy_revalidated", map[string]any{"round": round, "names": row.LegacyRevalidated, "first": names[0], "last": names[len(names)-1], "proof": "v2", "windows": s.o.windows, "full_history": !s.windowOnly()})
	}
	row.Holes = append(row.Holes, preservedHoles...)
	if len(preservedProofs) > 0 {
		if row.HoleProofs == nil {
			row.HoleProofs = map[string]string{}
		}
		for name, stamp := range preservedProofs {
			row.HoleProofs[name] = stamp
		}
	}
	row.First, row.Last, row.Names = names[0], names[len(names)-1], len(names)
	row.Reused = len(names) - len(selected)
	return row, nil
}

// A later requested parity period cannot silently expand a frozen history
// generation. Such a tail is still unproven even when a parity sample is clean.
func frontierReadyBounds(o syncOptions, r syncReadyOptions, report *readyReport) error {
	data, err := os.ReadFile(filepath.Join(o.state, "verification-scope.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f syncFrontier
	if err = json.Unmarshal(data, &f.scope); err != nil {
		return err
	}
	horizon := f.scope.Now - int64(f.scope.Settle.Seconds())
	report.HistoryVerifiedUntil = horizon
	if r.scope != "windows" {
		for _, w := range r.period {
			if w.Until > horizon {
				report.Reasons = append(report.Reasons, fmt.Sprintf("parity period %s extends past history verification horizon %d; a new history scope is required", w.Name, horizon))
			}
		}
	}
	return nil
}

// Freeze each requested range at the verified horizon. Older endpoints retain
// the whole-revision fence; a clean v2 lineage can retain historical proofs.
func (s *syncer) windowProofs(ctx context.Context, host string, names []string) ([]string, error) {
	proofs, _, err := s.windowProofsVersion(ctx, host, names)
	return proofs, err
}

func (s *syncer) windowProofsVersion(ctx context.Context, host string, names []string, absentOut ...*[]bool) ([]string, bool, error) {
	b, ok := s.b.(syncWindowProofBackend)
	if !ok {
		proofs, err := s.b.(syncProofBackend).Proofs(ctx, host, names)
		return proofs, false, err
	}
	hash := make([]hash.Hash, len(names))
	known := make([]bool, len(names))
	absent := make([]bool, len(names))
	seenPresence := make([]bool, len(names))
	for i := range hash {
		known[i] = true
		hash[i] = sha256.New()
	}
	windows := append(syncRanges(nil), s.o.windows...)
	if !s.windowOnly() || s.o.sampleEvery > 0 {
		windows = append(windows, syncRange{Name: "history", From: 1, Until: s.verificationHorizon()})
	}
	for wi, w := range windows {
		until := min(w.Until, s.verificationHorizon())
		if until <= w.From {
			continue
		}
		var requested []string
		var positions []int
		for i, n := range names {
			if wi >= len(s.o.windows) && !s.sampled(n) {
				continue
			}
			requested = append(requested, n)
			positions = append(positions, i)
		}
		if len(requested) == 0 {
			continue
		}
		var proofs []string
		var missing []bool
		var err error
		if presence, ok := s.b.(syncWindowPresenceBackend); ok {
			proofs, missing, err = presence.WindowProofsPresence(ctx, host, requested, w.From, until)
		} else {
			proofs, err = b.WindowProofs(ctx, host, requested, w.From, until)
		}
		if errors.Is(err, errSyncNoProof) {
			proofs, err := s.b.(syncProofBackend).Proofs(ctx, host, names)
			return proofs, false, err
		}
		if err != nil {
			return nil, false, err
		}
		if len(proofs) != len(requested) {
			return nil, false, errors.New("invalid window mutation proof response")
		}
		for j, p := range proofs {
			i := positions[j]
			isAbsent := len(missing) == len(requested) && missing[j] && p != ""
			if !seenPresence[i] {
				absent[i] = isAbsent
				seenPresence[i] = true
			} else {
				absent[i] = absent[i] && isAbsent
			}
			if p == "" {
				known[i] = false
				continue
			}
			fmt.Fprintln(hash[i], w.Name, w.From, until, p)
		}
	}
	if len(absentOut) > 0 {
		*absentOut[0] = absent
	}
	out := make([]string, len(names))
	for i := range out {
		if known[i] {
			out[i] = fmt.Sprintf("%x", hash[i].Sum(nil))
		}
	}
	return out, true, nil
}
