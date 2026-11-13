package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

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

// Scan the existing catalog in bounded batches. Digests establish actual source
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
			k := key{group, source}
			if r, ok := selections[k]; ok && (r.First != names[0] || r.Last != names[len(names)-1] || r.Names != len(names)) {
				return fmt.Errorf("window selection differs from catalog batch %d", group)
			}
			if r, ok := completed[k]; ok {
				s.applyCopy(r.Selected, nil, r.Copied, r.Failed, r.Forbidden)
				continue
			}
			if r, ok := selections[k]; ok {
				selected = r.Selected
			} else {
				relevant := map[string]bool{}
				for _, w := range s.copyWindows() {
					digests, err := s.digest(ctx, source, subset, w)
					if err != nil {
						return fmt.Errorf("window missing digest on %s: %w (window-only sync requires source digests)", source, err)
					}
					for _, n := range subset {
						var archives []syncArchive
						count := int64(0)
						for _, d := range digests[n] {
							archives = append(archives, syncArchive{Step: d.Step, Points: d.Points})
							count += d.Count
						}
						if count > 0 && (s.o.includeShort || !syncShort(archives)) {
							relevant[n] = true
						}
					}
				}
				for _, n := range subset {
					if relevant[n] {
						selected = append(selected, n)
					}
				}
				if err := s.j.append(syncRow{Kind: "window-selected", Round: group, Source: source, First: names[0], Last: names[len(names)-1], Names: len(names), Selected: selected}); err != nil {
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
		data, _ := json.Marshal([]any{source, target, names, w.From, w.Until})
		hash := sha256.Sum256(data)
		id := hex.EncodeToString(hash[:])
		j, err := openJournal(fmt.Sprintf("%s.window-%d", journalPath, wi), id)
		if err != nil {
			return syncCopyResult{}, err
		}
		m := &mover{options: options{tokenEnv: b.o.tokenEnv, transferIdle: b.o.copyIdle, bulkCombine: true, nativeWindow: &chunkstore.NativeWindow{From: w.From, Until: w.Until}}, client: b.client, journal: j, planData: &manifest{PlanID: id}}
		if m.transferIdle <= 0 {
			m.transferIdle = defaultTransferIdle
		}
		c := &nativeController{mover: m, sessions: map[string]*nativeRemoteSession{}}
		err = func() error {
			defer c.release()
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
	}
	return syncCopyResult{Copied: len(names) - len(failed), Failed: sortedKeys(failed)}, nil
}
