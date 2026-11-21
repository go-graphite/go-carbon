package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

const syncWindowSelectionReason = "source window selection unresolved: missing metadata or pending snapshot"
const syncWindowSelectionStall = 2 * time.Minute

type syncWindowSelectionKey struct {
	group  int
	source string
}

func (s *syncer) resolvedWindowSelections() map[syncWindowSelectionKey]map[string]bool {
	out := map[syncWindowSelectionKey]map[string]bool{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-resolved" }) {
		k := syncWindowSelectionKey{r.Round, r.Source}
		if out[k] == nil {
			out[k] = map[string]bool{}
		}
		for _, n := range r.Selected {
			out[k][n] = true
		}
	}
	// Selection uncertainty can outlive a successful import: old selections
	// may overlap copied names, or a process may stop between copied and
	// window-resolved. Reconcile those receipts before probing or copying again.
	// Keep only pending membership here, never a second full name catalog.
	pending := map[syncWindowSelectionKey]map[string]bool{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-selected" && len(r.Skipped) > 0 }) {
		k := syncWindowSelectionKey{r.Round, r.Source}
		if pending[k] == nil {
			pending[k] = map[string]bool{}
		}
		for _, skip := range r.Skipped {
			if !out[k][skip.Name] {
				pending[k][skip.Name] = true
			}
		}
	}
	for _, r := range s.j.find(func(r syncRow) bool {
		return r.Kind == "copied" && !r.Forbidden && r.Copied > 0 &&
			(r.Phase == "window-missing" || strings.HasPrefix(r.Phase, "window-retry-"))
	}) {
		k := syncWindowSelectionKey{r.Round, r.Source}
		for name := range pending[k] {
			at := sort.SearchStrings(r.Selected, name)
			if !(at < len(r.Selected) && r.Selected[at] == name) &&
				!(r.Names == 1 && r.First == name && r.Last == name) {
				continue
			}
			failed := slices.Contains(r.Failed, name) || slices.Contains(r.Short, name)
			for _, item := range r.Resolved {
				failed = failed || item.Name == name
			}
			if failed {
				continue
			}
			if out[k] == nil {
				out[k] = map[string]bool{}
			}
			out[k][name] = true
			delete(pending[k], name)
		}
	}
	return out
}

// A skipped selection is unknown source coverage, rather than an immutable copy
// intent. Reselect only these names on fresh pins. Original selections and their
// native range identities remain unchanged. Resolutions are committed only after
// complete window selection and, where necessary, a successful window copy.
// waitWindowSelections keeps transient selection uncertainty out of the point
// repair loop. Each pass reopens source pins and reuses durable resolutions;
// cancellation preserves the unresolved frontier for a later invocation.
func (s *syncer) waitWindowSelections(ctx context.Context) error {
	if err := s.savedWindowSelectionFailure(); err != nil {
		return err
	}
	lastPending := -1
	lastProgress := s.o.now()
	for delay := time.Second; ; delay = min(30*time.Second, delay*2) {
		if err := s.retryWindowSelections(ctx); err != nil {
			return err
		}
		pending := 0
		for _, r := range s.sum.residual {
			if r.Reason == syncWindowSelectionReason {
				pending++
			}
		}
		if pending == 0 {
			return nil
		}
		if s.windowRetryCopyError != nil {
			return s.windowSelectionFailure(s.sum.residual, true)
		}
		if s.o.noCopy {
			return fmt.Errorf("%w: %d pending window selections (--no-copy)", ErrSyncResidual, pending)
		}
		if lastPending < 0 || pending < lastPending {
			lastProgress = s.o.now()
		}
		lastPending = pending
		if s.o.now().Sub(lastProgress) >= syncWindowSelectionStall {
			var frontier []string
			for _, r := range s.sum.residual {
				if r.Reason == syncWindowSelectionReason {
					frontier = append(frontier, r.Source+" "+r.Name)
				}
			}
			sort.Strings(frontier)
			event("sync_window_selection_stalled", map[string]any{"pending_selections": pending, "frontier": frontier, "reason": syncWindowSelectionReason})
			return s.windowSelectionFailure(s.sum.residual, true)
		}
		event("sync_window_selection_wait", map[string]any{"pending_selections": pending, "retry_seconds": delay.Seconds()})
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
}

// Copy failures are provisional until verification has checked every donor.
// A stalled selection must not exclude unrelated, previously failed copies
// from that verification (their boundary receipts may already be satisfied).
func (s *syncer) quarantineWindowSelections() error {
	var rows []syncResidual
	for _, r := range s.sum.residual {
		if r.Reason == syncWindowSelectionReason {
			rows = append(rows, r)
		}
	}
	return s.o.issues.record(rows, "window retry", false)
}

func (s *syncer) retryWindowSelections(ctx context.Context) error {
	s.windowRetryCopyError = nil
	if !s.windowOnly() {
		return nil
	}
	rows := s.j.find(func(r syncRow) bool { return r.Kind == "window-selected" && len(r.Skipped) > 0 })
	if len(rows) == 0 {
		return nil
	}
	if b, ok := s.b.(interface{ beginWindowCopy() func() }); ok {
		defer b.beginWindowCopy()()
	}
	resolved := s.resolvedWindowSelections()
	sources := map[string]int{}
	for i, source := range s.o.from {
		sources[source] = i
	}
	// Rows of a catalog group can overlap across holders. Keep their original
	// order (and therefore target-wins donor priority); different catalog groups
	// are disjoint and can safely run together.
	groups := map[int][]syncRow{}
	var order []int
	total, durable := 0, 0
	visited := map[syncWindowSelectionKey]bool{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-retry-visited" }) {
		visited[syncWindowSelectionKey{r.Round, r.Source}] = true
	}
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		total += len(r.Skipped)
		if visited[k] {
			durable += len(r.Skipped)
		} else {
			for _, skip := range r.Skipped {
				if resolved[k][skip.Name] {
					durable++
				}
			}
		}
		if _, ok := groups[r.Round]; !ok {
			order = append(order, r.Round)
		}
		groups[r.Round] = append(groups[r.Round], r)
	}
	floor := int64(durable)
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-retry-floor" }) {
		floor = max(floor, int64(r.Names))
	}
	// Resume unfinished groups before revisiting earlier uncertainty. A legacy
	// attempted counter is only a scheduling hint; it never retires a name.
	var fresh, revisit []int
	prefix := int64(0)
	for _, group := range order {
		unfinished := false
		for _, row := range groups[group] {
			prefix += int64(len(row.Skipped))
			k := syncWindowSelectionKey{row.Round, row.Source}
			if !visited[k] {
				for _, skip := range row.Skipped {
					unfinished = unfinished || !resolved[k][skip.Name]
				}
			}
		}
		if unfinished && (len(visited) > 0 || prefix > floor) {
			fresh = append(fresh, group)
		} else {
			revisit = append(revisit, group)
		}
	}
	fresh = append(fresh, revisit...)
	order = fresh
	if err := s.beginPhase("window retry", 0, total); err != nil {
		return err
	}
	s.prog.mu.Lock()
	s.prog.durableFloor = max(s.prog.durableFloor, min(int64(total), floor))
	s.prog.mu.Unlock()
	s.prog.done.Store(int64(durable))
	s.prog.replayed.Store(int64(durable))
	if !s.legacyCopyReceipts {
		s.prog.planVerification(int64(s.verifyScopeNames()))
	}
	workers := 1
	if _, ok := s.b.(interface {
		WindowPresenceRanges(context.Context, string, []string, syncRanges) (map[string][]syncDigest, error)
	}); ok {
		workers = max(1, s.o.streams/max(1, len(s.o.from)))
	}
	type result struct {
		child *syncer
		rows  []syncRow
		stats syncWindowRetryStats
		err   error
	}
	results := make(chan result, workers)
	active := 0
	attempted, remaining, recovered := 0, 0, 0
	var firstErr error
	collect := func() {
		r := <-results
		active--
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		s.sum.Copied += r.child.sum.Copied
		if r.child.windowRetryCopyError != nil {
			s.windowRetryCopyError = r.child.windowRetryCopyError
		}
		for source, forbidden := range r.child.forbid {
			if forbidden {
				s.forbid[source] = true
			}
		}
		attempted += r.stats.attempted
		remaining += r.stats.remaining
		recovered += r.stats.recovered
		// Only durable per-row receipts move the cursor. A failed/in-flight row
		// is retried; an already resolved name never reaches a backend again.
		for _, row := range r.rows {
			k := syncWindowSelectionKey{row.Round, row.Source}
			if !visited[k] && r.stats.visited[k] {
				visited[k] = true
				durable += len(row.Skipped)
				for _, skip := range row.Skipped {
					if resolved[k][skip.Name] {
						durable--
					}
				}
			}
		}
		s.prog.advance(int64(durable))
	}
	for _, group := range order {
		if active == workers {
			collect()
		}
		if firstErr != nil || ctx.Err() != nil {
			break
		}
		child := &syncer{o: s.o, b: s.b, j: s.j, redo: s.redo, legacyCopyReceipts: s.legacyCopyReceipts, forbid: map[string]bool{}}
		for source, forbidden := range s.forbid {
			child.forbid[source] = forbidden
		}
		groupRows := groups[group]
		groupResolved := map[syncWindowSelectionKey]map[string]bool{}
		for _, row := range groupRows {
			k := syncWindowSelectionKey{row.Round, row.Source}
			groupResolved[k] = map[string]bool{}
			for name, done := range resolved[k] {
				groupResolved[k][name] = done
			}
		}
		active++
		go func() {
			stats, err := child.retryWindowRows(ctx, groupRows, groupResolved, sources)
			results <- result{child, groupRows, stats, err}
		}()
	}
	for active > 0 {
		collect()
	}
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resolved = s.resolvedWindowSelections()
	// Rebuild selection uncertainty from the durable original selections and
	// resolutions, including sources that are now forbidden. Copy and boundary
	// failures have their own residuals and remain untouched.
	kept := s.sum.residual[:0]
	for _, r := range s.sum.residual {
		if r.Reason != syncWindowSelectionReason {
			kept = append(kept, r)
		}
	}
	s.sum.residual = kept
	pendingSeen := map[syncKey]bool{}
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		for _, skip := range r.Skipped {
			key := syncKey{skip.Name, r.Source}
			if !resolved[k][skip.Name] && !s.o.issues.policyExcluded(skip.Name) && !pendingSeen[key] {
				pendingSeen[key] = true
				s.addResidual(syncHole{Name: skip.Name, Source: r.Source}, syncWindowSelectionReason)
			}
		}
	}
	event("sync_window_retry_done", map[string]any{"attempted": attempted, "resolved": recovered, "remaining": remaining})
	return nil
}

type syncWindowRetryStats struct {
	attempted, remaining, recovered int
	visited                         map[syncWindowSelectionKey]bool
}

func (s *syncer) retryWindowRows(ctx context.Context, rows []syncRow, resolved map[syncWindowSelectionKey]map[string]bool, sources map[string]int) (syncWindowRetryStats, error) {
	stats := syncWindowRetryStats{visited: map[syncWindowSelectionKey]bool{}}
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		var names []string
		seen := map[string]bool{}
		for _, skip := range r.Skipped {
			if !resolved[k][skip.Name] && !s.o.issues.excluded(skip.Name) && !seen[skip.Name] {
				seen[skip.Name] = true
				names = append(names, skip.Name)
			}
		}
		if len(names) == 0 {
			continue
		}
		stats.attempted += len(names)
		src, ok := sources[r.Source]
		if !ok {
			return stats, fmt.Errorf("window retry source is absent from original options")
		}
		if s.forbid[r.Source] {
			stats.remaining += len(names)
			continue
		}
		relevant, unknown := map[string]bool{}, map[string]bool{}
		selectionFailed := false
		selectNames := func(subset []string) error {
			windows := s.copyWindows()
			if _, ok := s.b.(interface {
				WindowPresenceRanges(context.Context, string, []string, syncRanges) (map[string][]syncDigest, error)
			}); ok {
				windows = syncRanges{{}} // one envelope request, filtered against every exact window
			}
			for _, w := range windows {
				var digests map[string][]syncDigest
				var err error
				if b, ok := s.b.(interface {
					WindowPresenceRanges(context.Context, string, []string, syncRanges) (map[string][]syncDigest, error)
				}); ok {
					digests, err = b.WindowPresenceRanges(ctx, r.Source, subset, s.copyWindows())
				} else {
					digests, err = s.windowPresence(ctx, r.Source, subset, w)
				}
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					event("sync_window_retry_failed", map[string]any{"level": "warn", "source": r.Source, "group": r.Round, "names": len(names), "stage": "selection", "error": err.Error(), "retained": true})
					return err
				}
				for _, n := range subset {
					var archives []syncArchive
					var count int64
					if len(digests[n]) == 0 {
						unknown[n] = true
					}
					for _, d := range digests[n] {
						if d.Hash == syncWindowUnresolved {
							unknown[n] = true
						}
						archives = append(archives, syncArchive{Step: d.Step, Points: d.Points})
						count += d.Count
						if d.Hash == "window-incomplete" {
							count++
						}
					}
					if count > 0 && (s.o.includeShort || !syncShort(archives)) {
						relevant[n] = true
					}
				}
			}
			return nil
		}
		if err := selectNames(names); err != nil {
			if ctx.Err() != nil {
				return stats, ctx.Err()
			}
			selectionFailed = true
		}
		var refresh []string
		for _, n := range names {
			if unknown[n] {
				refresh = append(refresh, n)
			}
		}
		if b, ok := s.b.(interface {
			prepareWindowSelection(context.Context, string, []string) error
		}); ok && !s.o.noCopy && !selectionFailed && len(refresh) > 0 {
			if err := b.prepareWindowSelection(ctx, r.Source, refresh); err != nil {
				return stats, err
			}
			for _, n := range refresh {
				delete(unknown, n)
				delete(relevant, n)
			}
			if err := selectNames(refresh); err != nil {
				if ctx.Err() != nil {
					return stats, ctx.Err()
				}
				selectionFailed = true
			}
		}
		if selectionFailed {
			stats.remaining += len(names)
			continue
		}
		// Missing window metadata needs a fresh absence proof. A source-local
		// proof resolves only this source's selection; peer selections and final
		// all-donor parity remain independent.
		var uncertain []string
		for _, n := range names {
			if unknown[n] {
				uncertain = append(uncertain, n)
			}
		}
		if len(uncertain) > 0 {
			// A native metadata hit disproves global absence even when this
			// window is empty. Avoid whole-history probes for pending names.
			end := s.o.now().Unix() - int64(s.o.settle.Seconds())
			got, err := s.digest(ctx, r.Source, uncertain, syncRange{From: max(int64(1), end-86400), Until: end})
			if err != nil {
				return stats, err
			}
			candidates := uncertain
			uncertain = nil
			for _, name := range candidates {
				if _, found := got[name]; !found {
					uncertain = append(uncertain, name)
				}
			}
		}

		var sourceProof []syncSkip
		if len(uncertain) > 0 {
			absent, err := s.windowSourceAbsent(ctx, r.Source, uncertain)
			if err != nil {
				return stats, err
			}
			candidates := uncertain
			uncertain = nil
			for _, name := range candidates {
				if absent[name] {
					delete(unknown, name)
					delete(relevant, name)
					sourceProof = append(sourceProof, syncSkip{Name: name, Reason: "source_absent_no_pending"})
				} else {
					uncertain = append(uncertain, name)
				}
			}
		}

		if len(uncertain) > 0 {
			var proof syncRow
			err := s.resolveAbsent(ctx, &proof, uncertain, map[string][]syncArchive{})
			for _, n := range uncertain {
				s.masks.Delete(n)
			}
			if err != nil {
				return stats, err
			}
			for _, item := range proof.Resolved {
				if item.Reason == "absent_all_sources" {
					delete(unknown, item.Name)
					delete(relevant, item.Name)
				}
			}
		}
		var selected, done []string
		for _, n := range names {
			if unknown[n] {
				continue
			}
			if relevant[n] {
				selected = append(selected, n)
			} else {
				done = append(done, n)
			}
		}
		copied := 0
		if len(selected) > 0 && !s.o.noCopy {
			// A different resolved subset gets its own native journal. A crash before
			// the resolution row safely replays the same intent, with target wins.
			data, _ := json.Marshal(selected)
			hash := sha256.Sum256(data)
			phase := "window-retry-" + hex.EncodeToString(hash[:16])
			result, err := s.copyFrom(ctx, phase, r.Round, src, selected)
			if err != nil {
				if ctx.Err() != nil {
					return stats, ctx.Err()
				}
				event("sync_window_retry_failed", map[string]any{"level": "warn", "source": r.Source, "group": r.Round, "names": len(selected), "stage": "copy", "error": err.Error(), "retained": true})
			}
			excluded := map[string]bool{}
			for _, n := range result.ExpectedAbsent {
				excluded[n] = true
			}
			if err != nil || result.Forbidden || len(result.Failed) > 0 {
				metric := selected[0]
				if len(result.Failed) > 0 {
					metric = result.Failed[0]
				}
				s.windowRetryCopyError = fmt.Errorf("%w: window selection repair rejected for %s", ErrSyncResidual, metric)
				event("sync_window_retry_failed", map[string]any{"level": "ERROR", "metric": metric, "source": r.Source, "stage": "copy", "error": fmt.Sprint(err), "failed": len(result.Failed), "forbidden": result.Forbidden})
			}
			failed := map[string]bool{}
			for _, n := range result.Failed {
				failed[n] = true
			}
			if err == nil && !result.Forbidden {
				for _, n := range selected {
					if !failed[n] {
						done = append(done, n)
						if !excluded[n] {
							copied++
						}
					}
				}
			}
		}
		if len(done) > 0 {
			if err := s.j.append(syncRow{Kind: "window-resolved", Round: r.Round, Source: r.Source, Selected: done, Copied: copied, Resolved: sourceProof}); err != nil {
				return stats, err
			}
			if resolved[k] == nil {
				resolved[k] = map[string]bool{}
			}
			for _, n := range done {
				resolved[k][n] = true
			}
			s.sum.Copied += copied
		}
		stats.recovered += len(done)
		stats.remaining += len(names) - len(done)
		if err := s.j.append(syncRow{Kind: "window-retry-visited", Round: r.Round, Source: r.Source, Names: len(r.Skipped)}); err != nil {
			return stats, err
		}
		stats.visited[k] = true
		event("sync_window_retry", map[string]any{"source": r.Source, "group": r.Round, "names": len(names), "resolved": len(done), "copied": copied, "remaining": len(names) - len(done), "attempted_total": stats.attempted, "resolved_total": stats.recovered, "remaining_total": stats.remaining})
		if err := ctx.Err(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// windowSourceAbsent proves that the original source has neither persisted
// metadata in any archive nor live write activity. It cannot waive a peer's
// selection, a target mismatch, or final verification. Never substitute a
// serving-archive digest when full-history sums are unsupported.
func (s *syncer) windowSourceAbsent(ctx context.Context, source string, names []string) (map[string]bool, error) {
	out := map[string]bool{}
	evidence, ok := s.b.(expirationEvidenceBackend)
	if !ok {
		return out, nil
	}
	sums, ok := s.b.(interface {
		SumDigest(context.Context, string, []string, int64, int64, int64) (map[string][]syncDigest, error)
	})
	if !ok {
		return out, nil
	}
	before, err := evidence.ExpirationEvidence(ctx, source, names, false)
	if err != nil {
		return nil, err
	}
	var candidates []string
	for _, name := range names {
		if e, found := before[name]; found && !e.Pending && e.Latest == 0 {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		return out, nil
	}
	got, err := sums.SumDigest(ctx, source, candidates, 0, 0, 0)
	if errors.Is(err, errSyncNoDigest) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	after, err := evidence.ExpirationEvidence(ctx, source, candidates, false)
	if err != nil {
		return nil, err
	}
	for _, name := range candidates {
		_, found := got[name]
		e, known := after[name]
		if !found && known && !e.Pending && e.Latest == 0 {
			out[name] = true
		}
	}
	return out, nil
}

// The old retry counter measured attempted names, including unresolved ones.
// Preserve that display floor during adoption without turning it into coverage
// evidence or skipping any unknown name. The journal binds it to this sync scope.
func (s *syncer) preserveWindowRetryProgress() error {
	data, err := os.ReadFile(filepath.Join(s.o.state, "progress.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var p progressSnapshot
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if p.Phase != "window retry" || p.Done <= 0 || p.Done > p.Total {
		return nil
	}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-retry-floor" }) {
		if int64(r.Names) >= p.Done {
			return nil
		}
	}
	return s.j.append(syncRow{Kind: "window-retry-floor", Names: int(p.Done)})
}
