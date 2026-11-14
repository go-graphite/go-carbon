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
	return out
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
	if err := s.retryWindowSelections(ctx); err != nil {
		return err
	}
	resolved := s.resolvedWindowSelections()
	if b, ok := s.b.(interface{ beginWindowCopy() func() }); ok {
		defer b.beginWindowCopy()()
	}
	s.phase("copy")
	s.prog.begin("window missing", s.sum.Missing)
	type key struct {
		group  int
		source string
	}
	selections, completed := map[key]syncRow{}, map[key]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool {
		return r.Kind == "window-selected" || r.Kind == "copied" && r.Phase == "window-missing"
	}) {
		k := key{r.Round, r.Source}
		if r.Kind == "window-selected" {
			selections[k] = r
		} else {
			completed[k] = r
		}
	}
	group, scanned := 0, 0
	for {
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
		started := time.Now()
		for src, source := range s.o.from {
			if s.forbid[source] {
				continue
			}
			var subset []string
			for i, n := range names {
				if masks[i]&(1<<uint(src)) != 0 {
					if err := chunkstore.ValidateMetricName(n); err == nil {
						subset = append(subset, n)
					} else if !s.failedCp[n] {
						s.applyCopy([]string{n}, nil, 0, []string{n}, false)
					}
				}
			}
			if len(subset) == 0 {
				continue
			}
			// Reuse completed selections on resume: native range identities include
			// the precise names and must never depend on a later source snapshot.
			var selected []string
			var skipped []syncSkip
			k := key{group, source}
			if r, ok := selections[k]; ok && (r.First != names[0] || r.Last != names[len(names)-1] || r.Names != len(names)) {
				return fmt.Errorf("window selection differs from catalog batch %d", group)
			}
			if r, ok := selections[k]; ok && r.SelectionComplete {
				for _, skip := range r.Skipped {
					if !resolved[syncWindowSelectionKey{group, source}][skip.Name] {
						s.addResidual(syncHole{Name: skip.Name, Source: source}, skip.Reason)
					}
				}
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
				for _, w := range s.copyWindows() {
					digests, err := s.windowPresence(ctx, source, subset, w)
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
			res, err := s.copyFrom(ctx, "window-missing", group, src, selected)
			if err != nil {
				return err
			}
			s.applyCopy(selected, nil, res.Copied, res.Failed, res.Forbidden)
		}
		scanned += len(names)
		s.prog.done.Store(int64(scanned))
		event("sync_window_missing", map[string]any{"group": group, "names": len(names), "scanned": scanned, "copied": s.sum.Copied, "seconds": time.Since(started).Seconds(), "names_per_second": float64(len(names)) / time.Since(started).Seconds()})
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.endPhase("copy", scanned)
	return nil
}

// Reuse the existing native window exporter and target-wins combiner. Durable
// native intents/receipts are kept beside the ordinary sync journal; no metric
// fallback is permitted because that API would silently import full history.
func (b *syncLiveBackend) CopyWindow(ctx context.Context, source, target string, names []string, journalPath string, windows syncRanges) (syncCopyResult, error) {
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
		before, beforeErr := b.Digest(ctx, target, names, w.From, w.Until)
		data, _ := json.Marshal([]any{source, target, names, w.From, w.Until})
		hash := sha256.Sum256(data)
		id := hex.EncodeToString(hash[:])
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
		after, afterErr := b.Digest(ctx, target, names, w.From, w.Until)
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
	for start := 0; start < len(names); start += chunkstore.NativeMaxRangeNames {
		subset := names[start:min(start+chunkstore.NativeMaxRangeNames, len(names))]
		body, _ := json.Marshal(struct {
			Session string
			Names   []string
			Window  *chunkstore.NativeWindow
		}{id, subset, &chunkstore.NativeWindow{From: w.From, Until: w.Until}})
		err := func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, source+"/v1/native/export", bytes.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			if token := os.Getenv(b.o.tokenEnv); token != "" {
				req.Header.Set("X-Buckyd-Authorization", token)
			}
			response, err := b.client.Do(req)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return fmt.Errorf("window export HTTP %d", response.StatusCode)
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
					out[record.Config.Name] = []syncDigest{{Hash: syncWindowUnresolved}}
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
					count := int64(len(a.Points))
					hash := ""
					// Include omitted in-window boundary buckets in selection, so the copy
					// reports them as residuals instead of silently skipping the metric.
					for _, skip := range record.WindowSkipped {
						if skip.Archive == ai && skip.Timestamp >= w.From && skip.Timestamp < w.Until {
							hash = "window-incomplete"
						}
					}
					out[record.Config.Name] = append(out[record.Config.Name], syncDigest{Step: int64(a.Retention.Step), Points: int64(a.Retention.Count), Count: count, Hash: hash})
				}
			}
			_, _, err = stream.Finish()
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, response.Body)
			return err
		}()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// The missing-window loop is serial. Keep its source pins across journal batches
// so capturing the global pending-name set is amortized, without changing batch
// identities or native range limits. Bound snapshot age to limit retained files
// and conservative revision exclusions. Existing pending/revision fences remain
// authoritative; no omitted name becomes proof of empty history.
func (b *syncLiveBackend) beginWindowCopy() func() {
	b.windowSources = &nativeController{mover: b.m, sessions: map[string]*nativeRemoteSession{}}
	b.windowPinned = time.Now()
	return func() {
		b.windowSources.release()
		b.windowSources = nil
	}
}

func (b *syncLiveBackend) windowController(ctx context.Context, m *mover, source string) (*nativeController, func(), error) {
	c := &nativeController{mover: m, sessions: map[string]*nativeRemoteSession{}}
	if b.windowSources == nil {
		return c, c.release, nil
	}
	if time.Since(b.windowPinned) >= 5*time.Minute {
		b.windowSources.release()
		b.windowPinned = time.Now()
	}
	_, err := b.windowSources.session(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	// Keep the complete session handle, including any readiness state.
	b.windowSources.mu.Lock()
	c.sessions[source] = b.windowSources.sessions[source]
	b.windowSources.mu.Unlock()
	return c, func() {}, nil
}
