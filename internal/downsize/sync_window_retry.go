package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const syncWindowSelectionReason = "source window selection unresolved: missing metadata or pending snapshot"

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
	return out
}

// A skipped selection is unknown source coverage, rather than an immutable copy
// intent. Reselect only these names on fresh pins. Original selections and their
// native range identities remain unchanged. Resolutions are committed only after
// complete window selection and, where necessary, a successful window copy.
func (s *syncer) retryWindowSelections(ctx context.Context) error {
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
	total := 0
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		for _, skip := range r.Skipped {
			if !resolved[k][skip.Name] {
				total++
			}
		}
	}
	s.prog.begin("window retry", total)
	attempted, remaining, recovered := 0, 0, 0
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		var names []string
		for _, skip := range r.Skipped {
			if !resolved[k][skip.Name] {
				names = append(names, skip.Name)
			}
		}
		if len(names) == 0 {
			continue
		}
		attempted += len(names)
		src, ok := sources[r.Source]
		if !ok {
			return fmt.Errorf("window retry source is absent from original options")
		}
		if s.forbid[r.Source] {
			remaining += len(names)
			continue
		}
		relevant, unknown := map[string]bool{}, map[string]bool{}
		selectionFailed := false
		for _, w := range s.copyWindows() {
			digests, err := s.windowPresence(ctx, r.Source, names, w)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				event("sync_window_retry_failed", map[string]any{"level": "warn", "source": r.Source, "group": r.Round, "names": len(names), "stage": "selection", "error": err.Error(), "retained": true})
				selectionFailed = true
				break
			}
			for _, n := range names {
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
		if selectionFailed {
			remaining += len(names)
			s.prog.done.Store(int64(attempted))
			continue
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
		if len(selected) > 0 {
			// A different resolved subset gets its own native journal. A crash before
			// the resolution row safely replays the same intent, with target wins.
			data, _ := json.Marshal(selected)
			hash := sha256.Sum256(data)
			phase := "window-retry-" + hex.EncodeToString(hash[:16])
			result, err := s.copyFrom(ctx, phase, r.Round, src, selected)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				event("sync_window_retry_failed", map[string]any{"level": "warn", "source": r.Source, "group": r.Round, "names": len(selected), "stage": "copy", "error": err.Error(), "retained": true})
			}
			failed := map[string]bool{}
			for _, n := range result.Failed {
				failed[n] = true
			}
			if err == nil && !result.Forbidden {
				for _, n := range selected {
					if !failed[n] {
						done = append(done, n)
						copied++
					}
				}
			}
		}
		if len(done) > 0 {
			if err := s.j.append(syncRow{Kind: "window-resolved", Round: r.Round, Source: r.Source, Selected: done, Copied: copied}); err != nil {
				return err
			}
			if resolved[k] == nil {
				resolved[k] = map[string]bool{}
			}
			for _, n := range done {
				resolved[k][n] = true
			}
			s.sum.Copied += copied
		}
		s.prog.done.Store(int64(attempted))
		recovered += len(done)
		remaining += len(names) - len(done)
		event("sync_window_retry", map[string]any{"source": r.Source, "group": r.Round, "names": len(names), "resolved": len(done), "copied": copied, "remaining": len(names) - len(done), "attempted_total": attempted, "resolved_total": recovered, "remaining_total": remaining})
		if err := ctx.Err(); err != nil {
			return err
		}
	}
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
	for _, r := range rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		for _, skip := range r.Skipped {
			if !resolved[k][skip.Name] {
				s.addResidual(syncHole{Name: skip.Name, Source: r.Source}, syncWindowSelectionReason)
			}
		}
	}
	event("sync_window_retry_done", map[string]any{"attempted": attempted, "resolved": recovered, "remaining": remaining})
	return nil
}
