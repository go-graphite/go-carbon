package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

const syncWindowUnresolved = "window-unresolved"

type syncWindowBackend interface {
	CopyWindow(context.Context, string, string, []string, string, syncRanges) (syncCopyResult, error)
}

func (s *syncer) windowOnly() bool { return len(s.o.windows) > 0 && !s.o.fullHistory }

func (s *syncer) copyWindows() syncRanges {
	return s.copyWindowsAt(s.settledHorizon())
}

// settledHorizon is the newest timestamp whose points the replicas have stopped writing.
func (s *syncer) settledHorizon() int64 { return s.o.now().Unix() - int64(s.o.settle.Seconds()) }

// settledWindowsCompatible reports whether windows saved by an earlier run of
// this state are the requested windows clipped at an older settled horizon.
// The requested windows decide the scope; the horizon only grows while a
// state runs, so a resume after the windows settled keeps the saved work and
// copies or verifies the newly settled tail with the current windows.
func (s *syncer) settledWindowsCompatible(saved syncRanges) bool {
	current := s.copyWindows()
	if slices.Equal(saved, current) {
		return true
	}
	if len(saved) == 0 {
		return false
	}
	horizon := saved[len(saved)-1].Until
	return horizon <= s.settledHorizon() && slices.Equal(saved, s.copyWindowsAt(horizon))
}

func (s *syncer) copyWindowsAt(horizon int64) syncRanges {
	var out syncRanges
	for _, w := range s.o.windows {
		w.Until = min(w.Until, horizon)
		if w.From < w.Until {
			out = append(out, w)
		}
	}
	// Copy the union once. Verification still checks each requested window.
	// Nested/overlapping windows otherwise export and transfer the same history
	// repeatedly, which serializes on native source/target admission.
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	var merged syncRanges
	for _, w := range out {
		if len(merged) > 0 && w.From <= merged[len(merged)-1].Until {
			last := &merged[len(merged)-1]
			last.Until = max(last.Until, w.Until)
			continue
		}
		merged = append(merged, w)
	}
	return merged
}

// compatibleWindowPolicy accepts a policy saved before the windows settled:
// the same batch size, and the requested windows clipped at an older horizon.
func (s *syncer) compatibleWindowPolicy(identity string) bool {
	var saved []json.RawMessage
	var batch int
	var windows syncRanges
	if json.Unmarshal([]byte(identity), &saved) != nil || len(saved) != 2 || json.Unmarshal(saved[0], &batch) != nil || json.Unmarshal(saved[1], &windows) != nil {
		return false
	}
	return batch == s.o.batch && s.settledWindowsCompatible(windows)
}

// Scan the existing catalog in bounded batches. Window exports establish actual
// coverage in every archive; retention metadata alone cannot prove relevance.
// Copy each batch immediately, rather than waiting for the complete catalog.
// This separate phase leaves old full-history copy/precheck rows reusable.
func (s *syncer) copyMissingWindow(ctx context.Context) error {
	f, scan, err := openLines(s.missingPath())
	if err != nil {
		return err
	}
	defer f.Close()
	policy, _ := json.Marshal([]any{s.o.batch, s.copyWindows()})
	policies := s.j.find(func(r syncRow) bool { return r.Kind == "window-policy" })
	if len(policies) > 0 && policies[0].Identity != string(policy) && !s.compatibleWindowPolicy(policies[0].Identity) {
		return fmt.Errorf("window copy batch size or settled windows changed; use the original options or a fresh state directory")
	}
	if len(policies) == 0 {
		if err := s.j.append(syncRow{Kind: "window-policy", Identity: string(policy)}); err != nil {
			return err
		}
	}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "window-resolved" }) {
		s.sum.Copied += r.Copied
	}
	if b, ok := s.b.(interface{ beginWindowCopy() func() }); ok {
		defer b.beginWindowCopy()()
	}
	s.phase("copy")
	if err := s.beginPhase("window missing", 0, s.sum.Missing); err != nil {
		return err
	}
	if !s.legacyCopyReceipts {
		s.prog.planVerification(int64(s.verifyScopeNames()))
	}
	selections, completed := map[syncWindowSelectionKey]syncRow{}, map[syncWindowSelectionKey]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool {
		return r.Kind == "window-selected" || r.Kind == "copied" && r.Phase == "window-missing"
	}) {
		k := syncWindowSelectionKey{r.Round, r.Source}
		if r.Kind == "window-selected" {
			selections[k] = r
		} else {
			completed[k] = r
		}
	}
	group, scanned, active := 0, 0, 0
	workers := 1
	if _, ok := s.b.(interface {
		WindowPresenceRanges(context.Context, string, []string, syncRanges) (map[string][]syncDigest, error)
	}); ok {
		// Each group can select all donors concurrently. Keep the total number
		// of requests within the stream budget, including selection requests.
		workers = max(1, s.o.streams/max(1, len(s.o.from)))
	}
	type result struct {
		group, names int
		replayed     bool
		child        *syncer
		started      time.Time
		err          error
	}
	results := make(chan result, workers)
	// Selections and issued pulls share cancellation. Native intents are durable
	// before dispatch; receipts resolve a lost acknowledgement on resume.
	var firstErr error
	collect := func() {
		r := <-results
		active--
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		for n := range r.child.copied {
			s.copied[n] = true
		}
		for n := range r.child.failedCp {
			s.failedCp[n] = true
		}
		for source := range r.child.forbid {
			s.forbid[source] = true
		}
		s.sum.Copied += r.child.sum.Copied
		s.sum.CopyFailed += r.child.sum.CopyFailed
		s.sum.residual = append(s.sum.residual, r.child.sum.residual...)
		if r.err == nil {
			if r.replayed {
				s.prog.replayed.Add(int64(r.names))
			}
			scanned += r.names
			s.prog.done.Store(int64(scanned))
			event("sync_window_missing", map[string]any{"group": r.group, "names": r.names, "scanned": scanned, "copied": s.sum.Copied, "seconds": time.Since(r.started).Seconds(), "groups_in_flight": active, "group_workers": workers})
		}
	}
	// Every exit joins issued work before the shared pins or journal close.
	defer func() {
		for active > 0 {
			collect()
		}
	}()
	for {
		if active == workers {
			collect()
		}
		if firstErr != nil {
			return firstErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var names []string
		var masks []uint64
		for len(names) < s.o.batch && scan.Scan() {
			n, mask, onTarget, err := parseUnionLine(scan.Text())
			if err != nil {
				return err
			}
			if !onTarget && mask != 0 {
				names = append(names, n)
				masks = append(masks, mask)
			}
		}
		if err := scan.Err(); err != nil {
			return err
		}
		if len(names) == 0 {
			break
		}
		group++
		child := &syncer{o: s.o, b: s.b, j: s.j, copied: map[string]bool{}, failedCp: map[string]bool{}, forbid: map[string]bool{}}
		for source, forbidden := range s.forbid {
			child.forbid[source] = forbidden
		}
		replayed := s.windowGroupReplayed(group, masks, selections, completed)
		active++
		go func(group int) {
			started := time.Now()
			err := child.copyWindowGroup(ctx, group, names, masks, selections, completed)
			results <- result{group, len(names), replayed, child, started, err}
		}(group)
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
	return s.endPhase("copy", scanned)
}

// Replayed groups advance the durable cursor, but did no work in this run.
func (s *syncer) windowGroupReplayed(group int, masks []uint64, selections, completed map[syncWindowSelectionKey]syncRow) bool {
	for src, source := range s.o.from {
		if s.forbid[source] {
			continue
		}
		present := false
		for _, mask := range masks {
			present = present || mask&(1<<uint(src)) != 0
		}
		if !present {
			continue
		}
		k := syncWindowSelectionKey{group, source}
		if completed[k].SelectionComplete {
			continue
		}
		if r := selections[k]; !r.SelectionComplete || len(r.Selected) > 0 {
			return false
		}
	}
	return true
}

// A catalog group contains disjoint names. Its private apply state is merged by
// the driver; the shared journal serializes durable rows. Donor order within a
// group stays serial, so existing target values and the first donor still win.
func (s *syncer) copyWindowGroup(ctx context.Context, group int, names []string, masks []uint64, selections, completed map[syncWindowSelectionKey]syncRow) error {
	selectionStarted := time.Now()
	subsets := make([][]string, len(s.o.from))
	for src, source := range s.o.from {
		if s.forbid[source] {
			continue
		}
		for i, n := range names {
			skip, e := s.excludePolicy(n, "window missing")
			if e != nil {
				return e
			}
			if skip {
				continue
			}
			if masks[i]&(1<<uint(src)) != 0 {
				if err := chunkstore.ValidateMetricName(n); err == nil {
					subsets[src] = append(subsets[src], n)
				} else if !s.failedCp[n] {
					s.applyCopy([]string{n}, nil, 0, []string{n}, false)
				}
			}
		}
	}
	type selectionResult struct {
		digests map[string][]syncDigest
		err     error
	}
	results := make([]selectionResult, len(s.o.from))
	var workers sync.WaitGroup
	// Only the live combined selector promises concurrent source access.
	// State, journals and copies stay in original source order.
	live, parallel := s.b.(interface {
		WindowPresenceRanges(context.Context, string, []string, syncRanges) (map[string][]syncDigest, error)
	})
	if parallel {
		slots := make(chan struct{}, max(1, s.o.streams))
		for src, source := range s.o.from {
			k := syncWindowSelectionKey{group, source}
			if len(subsets[src]) == 0 || completed[k].SelectionComplete || selections[k].SelectionComplete {
				continue
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case slots <- struct{}{}:
					defer func() { <-slots }()
				case <-ctx.Done():
					results[src].err = ctx.Err()
					return
				}
				results[src].digests, results[src].err = live.WindowPresenceRanges(ctx, source, subsets[src], s.copyWindows())
			}()
		}
		workers.Wait()
	}
	event("sync_window_group_selection", map[string]any{"group": group, "seconds": time.Since(selectionStarted).Seconds()})
	for src, source := range s.o.from {
		subset := subsets[src]
		if len(subset) == 0 {
			continue
		}
		// Reuse completed selections on resume: native range identities include
		// the precise names and must never depend on a later source snapshot.
		var selected []string
		var skipped []syncSkip
		k := syncWindowSelectionKey{group, source}
		if r, ok := selections[k]; ok && (r.First != names[0] || r.Last != names[len(names)-1] || r.Names != len(names)) {
			return fmt.Errorf("window selection differs from catalog batch %d", group)
		}
		if r, ok := completed[k]; ok && r.SelectionComplete && !r.Forbidden {
			s.applyCopy(r.Selected, nil, r.Copied, r.Failed, r.Forbidden)
			continue
		}
		if r, ok := selections[k]; ok && r.SelectionComplete {
			selected = r.Selected
		} else {
			relevant := map[string]bool{}
			unresolved := map[string]bool{}
			selectionWindows := s.copyWindows()
			if parallel {
				selectionWindows = selectionWindows[:min(1, len(selectionWindows))]
			}
			for _, w := range selectionWindows {
				var digests map[string][]syncDigest
				var err error
				if parallel {
					digests, err = results[src].digests, results[src].err
				} else {
					digests, err = s.windowPresence(ctx, source, subset, w)
				}
				if err != nil {
					return fmt.Errorf("window missing selection on %s: %w", source, err)
				}
				for _, n := range subset {
					var archives []syncArchive
					count := int64(0)
					for _, d := range digests[n] {
						if d.Hash == syncWindowUnresolved {
							unresolved[n] = true
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
			for _, n := range subset {
				if unresolved[n] {
					reason := syncWindowSelectionReason
					skipped = append(skipped, syncSkip{n, reason})
					s.addResidual(syncHole{Name: n, Source: source}, reason)
					event("sync_window_selection_skipped", map[string]any{"level": "warn", "source": source, "metric": n, "reason": reason})
				} else if relevant[n] {
					selected = append(selected, n)
				}
			}
			if err := s.j.append(syncRow{Kind: "window-selected", Round: group, Source: source, First: names[0], Last: names[len(names)-1], Names: len(names), Selected: selected, Skipped: skipped, SelectionComplete: true}); err != nil {
				return err
			}
		}
		if len(selected) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		copyStarted := time.Now()
		res, err := s.copyFrom(ctx, "window-missing", group, src, selected)
		event("sync_window_group_copy", map[string]any{"group": group, "source": source, "names": len(selected), "seconds": time.Since(copyStarted).Seconds()})
		if err != nil {
			return err
		}
		s.applyCopy(selected, nil, res.Copied, res.Failed, res.Forbidden)
	}
	return nil
}

// Reuse the existing native window exporter and target-wins combiner. Durable
// native intents/receipts are kept beside the ordinary sync journal; no metric
// fallback is permitted because that API would silently import full history.
// A rejected native batch must not strand unrelated names. Keep the original
// intent journal intact and give each deterministic child its own window-bound
// receipt identity. Even if a rejected parent applied points before failing,
// native combine preserves existing target values when its children retry.
func (b *syncLiveBackend) splitRejectedWindow(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges, err error) (syncCopyResult, error) {
	var result syncCopyResult
	if err == nil || ctx.Err() != nil {
		return result, err
	}
	var api *apiError
	if !errors.As(err, &api) || (api.Status != http.StatusRequestEntityTooLarge && api.Status != http.StatusConflict) {
		return result, err
	}
	return b.splitWindowRange(ctx, source, target, names, journalPath, windows, err.Error(), false)
}

func (b *syncLiveBackend) splitWindowRange(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges, reason string, planned bool) (syncCopyResult, error) {
	if len(names) <= 1 {
		event("sync_window_copy_unresolved", map[string]any{"level": "error", "source": source, "target": target, "names": names, "reason": reason, "retained": true})
		return syncCopyResult{Failed: append([]string(nil), names...)}, nil
	}
	kind := "sync_window_copy_split"
	if planned {
		kind = "sync_window_copy_plan"
	}
	event(kind, map[string]any{"source": source, "target": target, "names": len(names), "reason": reason})
	mid := len(names) / 2
	identity := sha256.Sum256([]byte(journalPath))
	base := filepath.Join(filepath.Dir(journalPath), "window-split-"+hex.EncodeToString(identity[:16]))
	left, err := b.CopyWindow(ctx, source, target, names[:mid], base+".left", windows)
	if err != nil && !restartCopyError(err) {
		return left, err
	}
	right, rightErr := b.CopyWindow(ctx, source, target, names[mid:], base+".right", windows)
	if rightErr != nil && (err == nil || !restartCopyError(rightErr)) {
		err = rightErr
	}
	return syncCopyResult{Copied: left.Copied + right.Copied, Failed: append(left.Failed, right.Failed...), Forbidden: left.Forbidden || right.Forbidden}, err
}

func (b *syncLiveBackend) CopyWindow(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges) (syncCopyResult, error) {
	if err := b.health.wait(ctx); err != nil {
		return syncCopyResult{}, err
	}
	var result syncCopyResult
	err := b.wait(ctx, "CopyWindow", source, func() error {
		var err error
		result, err = b.copyWindowOnce(ctx, source, target, names, journalPath, windows)
		if ctx.Err() == nil && restartCopyError(err) {
			b.expireWindowPins()
		}
		return err
	})
	return result, err
}

func (b *syncLiveBackend) copyWindowOnce(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges) (syncCopyResult, error) {
	if b.o.noCopy {
		return syncCopyResult{Failed: append([]string(nil), names...)}, nil
	}
	for _, host := range []string{source, target} {
		var cap struct{ NativeTimeWindow, NativeExactCombine, NativeDeferredCombine bool }
		if _, err := b.m.requestOnce(ctx, host, "/v1/native/capabilities", nil, &cap); err != nil {
			var api *apiError
			if errors.As(err, &api) && api.Status == http.StatusForbidden {
				return syncCopyResult{Forbidden: true}, nil
			}
			return syncCopyResult{}, err
		}
		if !cap.NativeTimeWindow || host == target && (!cap.NativeExactCombine || !cap.NativeDeferredCombine) {
			return syncCopyResult{}, fmt.Errorf("window-only sync requires native window combine on %s", host)
		}
	}
	failed := map[string]bool{}
	var failedMu sync.Mutex
	markFailed := func(n string) { failedMu.Lock(); failed[n] = true; failedMu.Unlock() }
	type windowCopy struct {
		w               syncRange
		id, journalPath string
		measure         bool
		m               *mover
	}
	copies := make([]windowCopy, 0, len(windows))
	defer func() {
		for _, copy := range copies {
			copy.m.journal.close()
		}
	}()
	for wi, w := range windows {
		data, _ := json.Marshal([]any{source, target, names, w.From, w.Until})
		hash := sha256.Sum256(data)
		id := hex.EncodeToString(hash[:])
		path := fmt.Sprintf("%s.window-%d-%s", journalPath, wi, id[:16])
		pointPath := path + ".window-points.json"
		measure, err := prepareWindowRangeCounts(pointPath, windowPointCount{Source: source, Window: w})
		if err != nil {
			return syncCopyResult{}, err
		}
		j, err := openJournal(path, id)
		if err != nil {
			return syncCopyResult{}, err
		}
		m := &mover{options: options{tokenEnv: b.o.tokenEnv, transferIdle: b.o.copyIdle, bulkCombine: true, nativeWindow: &chunkstore.NativeWindow{From: w.From, Until: w.Until}}, client: b.client, journal: j, planData: &manifest{PlanID: id}}
		if m.transferIdle <= 0 {
			m.transferIdle = defaultTransferIdle
		}
		copies = append(copies, windowCopy{w, id, path, measure, m})
	}
	progress, _ := ctx.Value(syncWindowProgressKey{}).(func(string, int, bool))
	// Each worker completes all windows of one disjoint name range. This
	// exposes fully processed names without waiting for a whole donor/window.
	err := runSyncWindowRanges(ctx, len(names), b.o.copyStreams, func(ctx context.Context, start, end int) error {
		fresh := false
		var retry error
		for _, copy := range copies {
			err := func() error {
				w, id, m, j := copy.w, copy.id, copy.m, copy.m.journal
				t := task{Stream: id, Source: source, Target: target, Start: int64(start), End: int64(end)}
				m.stateMu.Lock()
				if saved, ok := j.tasks[taskKey(t)]; ok {
					t = saved
				}
				m.stateMu.Unlock()
				pointPath := fmt.Sprintf("%s.range-%d-%d.window-points.json", copy.journalPath, start, end)
				if err := chunkstore.ValidateNativeNames(names[start:end]); err != nil && (t.Native == nil || !t.Native.complete()) {
					// Preserve the old binary split identities and any receipts already
					// written by failure-driven splitting. Plan from known name bytes,
					// before measuring or exporting a range the receiver cannot accept.
					if copy.measure {
						if _, err := prepareWindowRangeCounts(pointPath, windowPointCount{Source: source, Window: w}); err != nil {
							return err
						}
					}
					childCtx := context.WithValue(ctx, syncWindowProgressKey{}, func(_ string, n int, replayed bool) {
						if n > 0 && !replayed {
							fresh = true
						}
					})
					path := fmt.Sprintf("%s.range-%d-%d", copy.journalPath, start, end)
					res, err := b.splitWindowRange(childCtx, source, target, names[start:end], path, syncRanges{w}, err.Error(), true)
					if err != nil {
						return err
					}
					if res.Forbidden {
						return errors.New("window split source or target became forbidden")
					}
					for _, n := range res.Failed {
						markFailed(n)
					}
					return nil
				}
				c, release, err := b.windowController(ctx, m, source)
				if err != nil {
					return err
				}
				defer func() { release() }()
				var measurement windowPointCount
				if copy.measure {
					measurement, err = b.beginWindowRangeCount(ctx, target, names[start:end], w, pointPath, t.Native != nil, source)
					if err != nil {
						return err
					}
				}
				rangeFailed := false
				finishCount := func() error {
					if !copy.measure || measurement.After != nil {
						return nil
					}
					if rangeFailed {
						measurement.Before = nil
					}
					return b.finishWindowRangeCount(ctx, target, names[start:end], w, pointPath, measurement)
				}
				if t.Native == nil || !t.Native.complete() {
					err := b.executeWindowRange(ctx, c, t, names[start:end])
					if err != nil {
						// A recursive split must release its borrowed source
						// before pin rotation can acquire the exclusive lock.
						release()
						release = func() {}
						childCtx := context.WithValue(ctx, syncWindowProgressKey{}, func(_ string, n int, replayed bool) {
							if n > 0 && !replayed {
								fresh = true
							}
						})
						path := fmt.Sprintf("%s.range-%d-%d", copy.journalPath, start, end)
						res, err := b.splitRejectedWindow(childCtx, source, target, names[start:end], path, syncRanges{w}, err)
						if err != nil {
							return err
						}
						if res.Forbidden {
							return errors.New("window split source or target became forbidden")
						}
						for _, n := range res.Failed {
							markFailed(n)
						}
						rangeFailed = true // children own their own counts; parent is uncertain
						return finishCount()
					}
					fresh = true
					m.stateMu.Lock()
					t = j.tasks[taskKey(t)]
					m.stateMu.Unlock()
					if err := b.o.rate.wait(ctx, t.Bytes); err != nil {
						return err
					}
				}
				skips := slices.Clone(t.Native.Receipt.WindowSkipped)
				for _, attempt := range t.Native.Combines {
					if attempt.Receipt != nil {
						skips = append(skips, attempt.Receipt.WindowSkipped...)
					}
				}
				for _, skip := range skips {
					if skip.Timestamp >= w.From && skip.Timestamp < w.Until && skip.Ordinal >= 0 && skip.Ordinal < end-start {
						name := names[start+skip.Ordinal]
						repaired, err := b.repairWindowBoundary(ctx, c, source, target, name, skip)
						if err != nil {
							return err
						}
						if repaired {
							fresh = true
							continue
						}
						rangeFailed = true
						markFailed(name)
						event("sync_window_boundary_skip", map[string]any{"level": "warn", "metric": names[start+skip.Ordinal], "timestamp": skip.Timestamp, "reason": skip.Reason})
					}
				}
				for i, n := range names[start:end] {
					if !t.Native.done(i) {
						rangeFailed = true
						markFailed(n)
					}
				}
				return finishCount()
			}()
			if err != nil {
				if ctx.Err() == nil && restartCopyError(err) {
					retry = err
					continue // other windows retain independent receipts
				}
				return err
			}
		}
		if retry != nil {
			return retry
		}
		if progress != nil {
			progress(fmt.Sprintf("%s:%d:%d", journalPath, start, end), end-start, !fresh)
		}
		return nil
	})
	if err != nil {
		return syncCopyResult{}, err
	}
	return syncCopyResult{Copied: len(names) - len(failed), Failed: sortedKeys(failed)}, nil
}

// WindowPresence uses the already deployed window exporter rather than the
// serving-archive fetch/digest APIs: coarse archives are independently authoritative.
func (s *syncer) windowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	if b, ok := s.b.(interface {
		WindowPresence(context.Context, string, []string, syncRange) (map[string][]syncDigest, error)
	}); ok {
		return b.WindowPresence(ctx, source, names, w)
	}
	return s.digest(ctx, source, names, w)
}

func (b *syncLiveBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	return b.WindowPresenceRanges(ctx, source, names, syncRanges{w})
}

// WindowPresenceRanges selects the union with one envelope export. Points in
// gaps are ignored. Precise copy windows still own boundary skips and receipts;
// an envelope may conservatively select a trailing coarse bucket for that copy.
func (b *syncLiveBackend) WindowPresenceRanges(ctx context.Context, source string, names []string, windows syncRanges) (map[string][]syncDigest, error) {
	if err := b.health.wait(ctx); err != nil {
		return nil, err
	}
	var result map[string][]syncDigest
	err := b.wait(ctx, "WindowPresenceRanges", source, func() error {
		var err error
		result, err = b.windowPresenceRangesOnce(ctx, source, names, windows)
		if ctx.Err() == nil && restartCopyError(err) {
			b.expireWindowPins()
		}
		return err
	})
	return result, err
}

type windowSelectionCapacity struct {
	load func() (chan struct{}, error)
}

func (b *syncLiveBackend) windowPresenceRangesOnce(ctx context.Context, source string, names []string, windows syncRanges) (map[string][]syncDigest, error) {
	if len(windows) == 0 {
		return map[string][]syncDigest{}, nil
	}
	defer b.recordTime("window_selection", source)()
	// Match the donor's export capacity, leaving one slot for copy pulls.
	// Older donors omit the capability and retain serial selection. Learn once
	// per donor so disjoint groups share a bound without retaining queued pins.
	v, _ := b.windowSelectionSlots.LoadOrStore(source, &windowSelectionCapacity{load: sync.OnceValues(func() (chan struct{}, error) {
		var capability struct{ NativeExportConcurrency int }
		if _, err := b.m.requestOnce(ctx, source, "/v1/native/capabilities", nil, &capability); err != nil {
			return nil, err
		}
		limit := max(1, min(8, capability.NativeExportConcurrency)-1)
		event("sync_window_selection_capacity", map[string]any{"source": source, "exports": capability.NativeExportConcurrency, "selections": limit})
		return make(chan struct{}, limit), nil
	})})
	slot, err := v.(*windowSelectionCapacity).load()
	if err != nil {
		// OnceValues retains errors too. A restart must not poison this
		// donor forever; remove only the generation we just observed.
		b.windowSelectionSlots.CompareAndDelete(source, v)
		return nil, err
	}
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	w := windows[0]
	for _, next := range windows[1:] {
		w.From = min(w.From, next.From)
		w.Until = max(w.Until, next.Until)
	}
	contains := func(ts int64) bool {
		for _, exact := range windows {
			if ts >= exact.From && ts < exact.Until {
				return true
			}
		}
		return false
	}

	c, release, err := b.windowController(ctx, b.m, source)
	if err != nil {
		return nil, err
	}
	defer release()
	id, err := c.session(ctx, source)
	if err != nil {
		return nil, err
	}
	out := map[string][]syncDigest{}
	tooLarge := errors.New("window selection request too large")
	conflict := errors.New("window selection conflict")
	var readSubset func([]string) error
	readSubset = func(subset []string) error {
		if err := chunkstore.ValidateNativeNames(subset); err != nil && len(subset) > 1 {
			mid := len(subset) / 2
			if err := readSubset(subset[:mid]); err != nil {
				return err
			}
			return readSubset(subset[mid:])
		}
		body, _ := json.Marshal(struct {
			Session string
			Names   []string
			Window  *chunkstore.NativeWindow
		}{id, subset, &chunkstore.NativeWindow{From: w.From, Until: w.Until}})
		attempt := map[string][]syncDigest{}
		err := func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, source+"/v1/native/export", bytes.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			if token := os.Getenv(b.o.tokenEnv); token != "" {
				req.Header.Set("X-Buckyd-Authorization", token)
			}
			requestStarted := time.Now()
			response, err := b.client.Do(req)
			headersSeconds := time.Since(requestStarted).Seconds()
			defer func() {
				event("sync_window_export_time", map[string]any{"source": source, "names": len(subset), "headers_seconds": headersSeconds, "seconds": time.Since(requestStarted).Seconds()})
			}()
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode == http.StatusRequestEntityTooLarge {
				return tooLarge
			}
			if response.StatusCode != http.StatusOK {
				message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
				if response.StatusCode == http.StatusConflict {
					return fmt.Errorf("%w: %s", conflict, message)
				}
				return &apiError{Status: response.StatusCode, Message: string(message)}
			}
			stream, err := chunkstore.NewTransferStream(response.Body)
			if err != nil {
				return err
			}
			if stream.Window == nil || stream.Window.From != w.From || stream.Window.Until != w.Until || stream.Count != len(subset) {
				return fmt.Errorf("window export scope mismatch")
			}
			expected := make(map[string]bool, len(subset))
			for _, n := range subset {
				expected[n] = true
			}
			for i := 0; i < stream.Count; i++ {
				record, err := stream.Next()
				if err != nil {
					return err
				}
				if !expected[record.Config.Name] {
					return fmt.Errorf("window export name mismatch")
				}
				delete(expected, record.Config.Name)
				if len(record.Config.Retentions) == 0 {
					// An omitted record is unknown coverage, never empty history.
					// Preserve a per-name marker while consuming the rest of the
					// stream, including its checksum, so healthy names proceed.
					attempt[record.Config.Name] = []syncDigest{{Hash: syncWindowUnresolved}}
					continue
				}
				if record.NativeSkip != nil {
					return fmt.Errorf("window export skipped %s: %s", record.Config.Name, record.NativeSkip.Reason)
				}
				snap, err := chunkstore.TransferSnapshot(ctx, record, &whisperio.ContinuationCodec{}, time.Now)
				if err != nil {
					return err
				}
				for ai, a := range snap.Archives {
					count := int64(0)
					for _, point := range a.Points {
						if contains(point.Timestamp) {
							count++
						}
					}
					hash := ""
					// Include omitted in-window boundary buckets in selection, so the copy
					// reports them as residuals instead of silently skipping the metric.
					for _, skip := range record.WindowSkipped {
						if skip.Archive == ai && contains(skip.Timestamp) {
							hash = "window-incomplete"
						}
					}
					attempt[record.Config.Name] = append(attempt[record.Config.Name], syncDigest{Step: int64(a.Retention.Step), Points: int64(a.Retention.Count), Count: count, Hash: hash})
				}
			}
			_, _, err = stream.Finish()
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, response.Body)
			return err
		}()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		truncated := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if errors.Is(err, tooLarge) || errors.Is(err, conflict) || truncated {
			if len(subset) <= 1 {
				if truncated || errors.Is(err, conflict) {
					// A failed stream cannot prove empty history. Isolate the
					// name as unresolved so other metrics keep moving.
					event("sync_window_export_unresolved", map[string]any{"level": "warn", "source": source, "metric": subset[0], "reason": err.Error()})
					out[subset[0]] = []syncDigest{{Hash: syncWindowUnresolved}}
					return nil
				}
				return fmt.Errorf("window export HTTP 413 for a single name")
			}
			// A truncated stream identifies complete record boundaries, but
			// its results are untrusted until the checksum arrives. Re-export
			// that prefix as one request rather than decoding it at every level
			// of a binary split. Both partitions must still validate completely.
			if truncated && len(attempt) > 0 && len(attempt) < len(subset) {
				var complete, remainder []string
				for _, name := range subset {
					if _, ok := attempt[name]; ok {
						complete = append(complete, name)
					} else {
						remainder = append(remainder, name)
					}
				}
				event("sync_window_export_split", map[string]any{"source": source, "complete_records": len(complete), "remaining_names": len(remainder), "checksum_validated": false})
				if err := readSubset(complete); err != nil {
					return err
				}
				return readSubset(remainder)
			}
			// Split only the transport request. The source pin, logical catalog
			// group, window and durable copy identities remain unchanged.
			mid := len(subset) / 2
			if err := readSubset(subset[:mid]); err != nil {
				return err
			}
			return readSubset(subset[mid:])
		}
		if err == nil {
			// Publish only after the complete stream and checksum validate.
			for name, digests := range attempt {
				out[name] = digests
			}
		}
		return err
	}
	for start := 0; start < len(names); start += chunkstore.NativeMaxRangeNames {
		if err := readSubset(names[start:min(start+chunkstore.NativeMaxRangeNames, len(names))]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Reuse source pins across groups. Rotation waits for borrowed controllers, so
// concurrent donor selections cannot close each other's snapshots. The bounded
// lifetime amortizes a global pending-name snapshot without retaining it for the
// complete catalog scan; revision and pending fences remain authoritative.
const syncWindowPinAge = 15 * time.Minute

func (b *syncLiveBackend) beginWindowCopy() func() {
	b.windowMu.Lock()
	// Selection can prove absence and must retain its pending/revision fence.
	// Repair only fills target holes from committed history; it uses a distinct
	// pin so queued current writes cannot starve a closed historical window.
	b.windowSources = &nativeController{mover: b.m, sessions: map[string]*nativeRemoteSession{}}
	b.windowCopySources = &nativeController{mover: b.m, committedWindow: true, sessions: map[string]*nativeRemoteSession{}}
	b.windowPinned = time.Now()
	b.windowMu.Unlock()
	return func() {
		b.windowMu.Lock()
		defer b.windowMu.Unlock()
		b.windowSources.release()
		b.windowSources = nil
		b.windowCopySources.release()
		b.windowCopySources = nil
	}
}

func (b *syncLiveBackend) windowController(ctx context.Context, m *mover, source string) (*nativeController, func(), error) {
	committed := m.nativeWindow != nil && m.nativeWindow.Until <= time.Now().Unix()
	c := &nativeController{mover: m, committedWindow: committed, sessions: map[string]*nativeRemoteSession{}}
	b.windowMu.RLock()
	if b.windowSources == nil {
		b.windowMu.RUnlock()
		return c, c.release, nil
	}
	if b.windowRestarted.Load() || time.Since(b.windowPinned) >= syncWindowPinAge {
		b.windowMu.RUnlock()
		b.windowMu.Lock()
		if b.windowSources != nil && (b.windowRestarted.Swap(false) || time.Since(b.windowPinned) >= syncWindowPinAge) {
			b.windowSources.release()
			b.windowCopySources.release()
			b.windowPinned = time.Now()
		}
		b.windowMu.Unlock()
		b.windowMu.RLock()
	}
	shared := b.windowSources
	if committed {
		shared = b.windowCopySources
	}
	if shared == nil {
		b.windowMu.RUnlock()
		return c, c.release, nil
	}
	done := b.recordTime("window_pin", source)
	_, err := shared.session(ctx, source)
	done()
	if err != nil {
		b.windowMu.RUnlock()
		return nil, nil, err
	}
	shared.mu.Lock()
	c.sessions[source] = shared.sessions[source]
	shared.mu.Unlock()
	return c, b.windowMu.RUnlock, nil
}

// A nested digest can fail while its caller borrows a source pin. Mark it
// stale without taking the exclusive lock; the next borrower rotates it once
// all current users finish.
func (b *syncLiveBackend) expireWindowPins() {
	b.windowRestarted.Store(true)
}
