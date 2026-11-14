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
	var out syncRanges
	horizon := s.o.now().Unix() - int64(s.o.settle.Seconds())
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
	if len(policies) > 0 && policies[0].Identity != string(policy) {
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
	s.prog.begin("window missing", s.sum.Missing)
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
	// Read-only selections remain cancellable. Issued pulls drain separately
	// because cancelling a pull can lose the server's durable acknowledgement.
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
	s.endPhase("copy", scanned)
	return nil
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
		if r, ok := completed[k]; ok && r.SelectionComplete {
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
		res, err := s.copyFrom(context.WithoutCancel(ctx), "window-missing", group, src, selected)
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
func (b *syncLiveBackend) CopyWindow(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges) (syncCopyResult, error) {
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
	for wi, w := range windows {
		beforeStarted := time.Now()
		before, beforeErr := b.Digest(ctx, target, names, w.From, w.Until)
		event("sync_window_digest", map[string]any{"stage": "before", "source": source, "names": len(names), "seconds": time.Since(beforeStarted).Seconds()})
		data, _ := json.Marshal([]any{source, target, names, w.From, w.Until})
		hash := sha256.Sum256(data)
		id := hex.EncodeToString(hash[:])
		pointPath := fmt.Sprintf("%s.window-%d-%s.window-points.json", journalPath, wi, id[:16])
		measurement := windowPointCount{Source: source, Window: w}
		if beforeErr == nil {
			n := countWindowPoints(before)
			measurement.Before = &n
		}
		measurement, err := readWindowPointCount(pointPath, measurement)
		if err != nil {
			return syncCopyResult{}, err
		}
		j, err := openJournal(fmt.Sprintf("%s.window-%d-%s", journalPath, wi, id[:16]), id)
		if err != nil {
			return syncCopyResult{}, err
		}
		m := &mover{options: options{tokenEnv: b.o.tokenEnv, transferIdle: b.o.copyIdle, bulkCombine: true, nativeWindow: &chunkstore.NativeWindow{From: w.From, Until: w.Until}}, client: b.client, journal: j, planData: &manifest{PlanID: id}}
		if m.transferIdle <= 0 {
			m.transferIdle = defaultTransferIdle
		}
		c, release, err := b.windowController(ctx, m, source)
		if err != nil {
			j.close()
			return syncCopyResult{}, err
		}
		err = func() error {
			defer release()
			defer j.close()
			for start := 0; start < len(names); start += chunkstore.NativeMaxRangeNames {
				end := min(start+chunkstore.NativeMaxRangeNames, len(names))
				t := task{Stream: id, Source: source, Target: target, Start: int64(start), End: int64(end)}
				if saved, ok := j.tasks[taskKey(t)]; ok {
					t = saved
				}
				if t.Native == nil || !t.Native.complete() {
					if err := c.executeRange(ctx, t, names[start:end]); err != nil {
						return err
					}
					t = j.tasks[taskKey(t)]
					if err := b.o.rate.wait(ctx, t.Bytes); err != nil {
						return err
					}
				}
				// Native exports deliberately omit incomplete trailing coarse
				// buckets. Never count such an in-window omission as success.
				for _, skip := range t.Native.Receipt.WindowSkipped {
					if skip.Timestamp >= w.From && skip.Timestamp < w.Until && skip.Ordinal >= 0 && skip.Ordinal < end-start {
						failed[names[start+skip.Ordinal]] = true
						event("sync_window_boundary_skip", map[string]any{"level": "warn", "metric": names[start+skip.Ordinal], "timestamp": skip.Timestamp, "reason": skip.Reason})
					}
				}
				for i, n := range names[start:end] {
					if t.Native == nil || !t.Native.done(i) {
						failed[n] = true
					}
				}
			}
			return nil
		}()
		if err != nil {
			return syncCopyResult{}, err
		}
		afterStarted := time.Now()
		after, afterErr := b.Digest(ctx, target, names, w.From, w.Until)
		if measurement.After == nil && afterErr == nil {
			if len(failed) > 0 {
				measurement.Before = nil
			}
			n := countWindowPoints(after)
			measurement.After = &n
			if err := atomicStateJSON(pointPath, measurement); err != nil {
				return syncCopyResult{}, err
			}
		}
		event("sync_window_digest", map[string]any{"stage": "after", "source": source, "names": len(names), "seconds": time.Since(afterStarted).Seconds()})
		count := func(d map[string][]syncDigest) int64 {
			var n int64
			for _, archives := range d {
				for _, a := range archives {
					n += a.Count
				}
			}
			return n
		}
		if beforeErr == nil && afterErr == nil {
			event("sync_window_points", map[string]any{"scope": "serving_archive", "source": source, "names": len(names), "window": w.Name, "before": count(before), "after": count(after), "added": count(after) - count(before)})
		}
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
	if len(windows) == 0 {
		return map[string][]syncDigest{}, nil
	}
	defer b.recordTime("window_selection", source)()
	// One selection per donor leaves at most one selection ahead of a copy
	// pull on older servers with serial export admission. Disjoint groups and
	// different donors still overlap; queued groups retain no source pin.
	v, _ := b.windowSelectionSlots.LoadOrStore(source, make(chan struct{}, 1))
	slot := v.(chan struct{})
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
				return fmt.Errorf("window export HTTP %d: %s", response.StatusCode, message)
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
	b.windowSources = &nativeController{mover: b.m, sessions: map[string]*nativeRemoteSession{}}
	b.windowPinned = time.Now()
	b.windowMu.Unlock()
	return func() {
		b.windowMu.Lock()
		defer b.windowMu.Unlock()
		b.windowSources.release()
		b.windowSources = nil
	}
}

func (b *syncLiveBackend) windowController(ctx context.Context, m *mover, source string) (*nativeController, func(), error) {
	c := &nativeController{mover: m, sessions: map[string]*nativeRemoteSession{}}
	b.windowMu.RLock()
	if b.windowSources == nil {
		b.windowMu.RUnlock()
		return c, c.release, nil
	}
	if time.Since(b.windowPinned) >= syncWindowPinAge {
		b.windowMu.RUnlock()
		b.windowMu.Lock()
		if b.windowSources != nil && time.Since(b.windowPinned) >= syncWindowPinAge {
			b.windowSources.release()
			b.windowPinned = time.Now()
		}
		b.windowMu.Unlock()
		b.windowMu.RLock()
	}
	shared := b.windowSources
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
