package downsize

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Older clients quarantined provisional copy failures when an unrelated window
// selection stalled. Give only that legacy class an independent, resumable
// full-history repair. Never reuse the main job's excluded verification rows as
// evidence that these names are covered. Other quarantine classes stay held.
func repairLegacyWindowQuarantine(ctx context.Context, o syncOptions, b syncBackend) error {
	if o.issues == nil || o.noCopy || o.noVerify || !o.fullHistory {
		return nil
	}
	q := o.issues
	q.mu.Lock()
	var names []string
	for n, issue := range q.quarantined {
		if issue.Phase == "window retry" && issue.Reason == "copy_failed" {
			names = append(names, n)
		}
	}
	q.mu.Unlock()
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	data := []byte(strings.Join(names, "\n") + "\n")
	digest := sha256.Sum256(data)
	state := filepath.Join(o.state, fmt.Sprintf("quarantine-repair-%x", digest[:16]))
	if err := os.MkdirAll(state, 0755); err != nil {
		return err
	}
	path := filepath.Join(state, "names.txt")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	repair := o
	repair.state, repair.namesFile, repair.namesHash = state, path, fmt.Sprintf("%x", digest)
	repair.issues = nil
	repair.sampleEvery, repair.maxRounds = 1, 3
	repair.restartWindow = false
	// This pass cannot certify READY. It repairs and verifies only these names;
	// the original scope, journals and readiness gates still run afterward.
	event("sync_quarantine_repair", map[string]any{"names": len(names), "state": state})
	err := runSync(ctx, repair, b)
	if err != nil && !errors.Is(err, ErrSyncQuarantined) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	checked, err := openSyncIssues(state)
	if err != nil {
		return err
	}
	// Persist the exact legacy entries before removing any. A failed/cancelled
	// repair leaves the original exclusions untouched, and is safe to resume.
	q.mu.Lock()
	defer q.mu.Unlock()
	var before []syncIssue
	for _, n := range names {
		before = append(before, q.quarantined[n])
	}
	if err := atomicStateJSON(filepath.Join(state, "original-quarantine.json"), before); err != nil {
		return err
	}
	next := make([]syncIssue, 0, len(q.quarantined))
	resolved := make(map[string]bool, len(names))
	for _, n := range names {
		resolved[n] = true
	}
	for n, issue := range q.quarantined {
		if resolved[n] {
			failure, failed := checked.quarantined[n]
			if !failed {
				failure, failed = checked.policy[n]
			}
			if !failed {
				continue
			}
			issue.Reason = "fresh quarantine repair: " + failure.Reason
			issue.LastSeen = time.Now()
			resolved[n] = false
		}
		next = append(next, issue)
	}
	sort.Slice(next, func(i, j int) bool { return next[i].Name < next[j].Name })
	if err := atomicStateJSON(filepath.Join(q.state, "quarantine.json"), next); err != nil {
		return err
	}
	q.quarantined = make(map[string]syncIssue, len(next))
	for _, issue := range next {
		q.quarantined[issue.Name] = issue
	}
	count := 0
	for _, ok := range resolved {
		if ok {
			count++
		}
	}
	event("sync_quarantine_repaired", map[string]any{"resolved": count, "remaining": len(names) - count, "state": state})
	return nil
}
