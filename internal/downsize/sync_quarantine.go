package downsize

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// ErrSyncQuarantined distinguishes completed work with named exceptions from
// failed/incomplete work. It never authorizes decommissioning a source.
var ErrSyncQuarantined = fmt.Errorf("%w: READY_EXCEPT; see quarantine.json", ErrSyncResidual)

type syncIssue struct {
	Name      string    `json:"name"`
	Reason    string    `json:"reason"`
	Phase     string    `json:"phase"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	// Released is journal-only: a later observation resolved the name, so a
	// resume or checkpoint drops it. Snapshots never contain released rows.
	Released bool `json:"released,omitempty"`
}
type syncIssues struct {
	mu                  sync.Mutex
	state               string
	quarantined, policy map[string]syncIssue
	dirty               map[string]bool
	failure             error
}

func openSyncIssues(state string) (*syncIssues, error) {
	q := &syncIssues{state: state, quarantined: map[string]syncIssue{}, policy: map[string]syncIssue{}, dirty: map[string]bool{}}
	for file, dst := range map[string]map[string]syncIssue{"quarantine.json": q.quarantined, "policy-skipped.json": q.policy} {
		data, err := os.ReadFile(filepath.Join(state, file))
		if errors.Is(err, os.ErrNotExist) {
			data, err = []byte("[]"), nil
		}
		if err != nil {
			return nil, err
		}
		var rows []syncIssue
		if err = json.Unmarshal(data, &rows); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		for _, r := range rows {
			if r.Name == "" || r.Reason == "" || r.Phase == "" || r.FirstSeen.IsZero() || r.LastSeen.IsZero() {
				return nil, fmt.Errorf("invalid issue in %s", file)
			}
			dst[r.Name] = r
		}
		if err := q.replay(file, dst); err != nil {
			return nil, err
		}
	}
	return q, nil
}
func (q *syncIssues) counts() (int64, int64) {
	if q == nil {
		return 0, 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.quarantined)), int64(len(q.policy))
}
func (q *syncIssues) excluded(name string) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, a := q.quarantined[name]
	_, b := q.policy[name]
	return a || b
}

// retryableSelection reports whether an unresolved source window selection may
// be selected again. Selection quarantine is retried on resume (the source
// snapshot may have landed); policy and other quarantine reasons are not.
func (q *syncIssues) retryableSelection(name string) bool {
	if q == nil {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.policy[name]; ok {
		return false
	}
	v, ok := q.quarantined[name]
	return !ok || v.Reason == syncWindowSelectionReason
}

// names returns up to limit quarantined names in name order, for reports.
func (q *syncIssues) names(limit int) []string {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	out := make([]string, 0, len(q.quarantined))
	for n := range q.quarantined {
		out = append(out, n)
	}
	q.mu.Unlock()
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Policy exclusions are explicit eligibility decisions, not unknown coverage.
func (q *syncIssues) policyExcluded(name string) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, excluded := q.policy[name]
	return excluded
}
func (q *syncIssues) record(rows []syncResidual, phase string, policy bool) error {
	if len(rows) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failure != nil {
		return q.failure
	}
	dst, file := q.quarantined, "quarantine.json"
	if policy {
		dst, file = q.policy, "policy-skipped.json"
	}
	now := time.Now()
	updates := make(map[string]syncIssue, len(rows))
	var added []syncIssue
	for _, r := range rows {
		v, exists := updates[r.Name]
		if !exists {
			v, exists = dst[r.Name]
		}
		if !exists {
			v = syncIssue{Name: r.Name, Reason: r.Reason, Phase: phase, FirstSeen: now}
			added = append(added, v)
		}
		v.LastSeen = now
		updates[r.Name] = v
	}
	delta := make([]syncIssue, 0, len(updates))
	for _, v := range updates {
		delta = append(delta, v)
	}
	if err := q.appendLocked(file, delta); err != nil {
		return err
	}
	for name, v := range updates {
		dst[name] = v
	}
	for _, r := range added {
		event("sync_name_quarantined", map[string]any{"level": "ERROR", "metric": r.Name, "reason": r.Reason, "phase": r.Phase, "policy_skipped": policy, "path": filepath.Join(q.state, file)})
	}
	return nil
}

// appendLocked durably appends one delta before any phase cursor may
// acknowledge it. Only the delta is written, independent of ledger size.
// Caller holds q.mu.
func (q *syncIssues) appendLocked(file string, delta []syncIssue) error {
	data, err := json.Marshal(delta)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(q.state, file+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, werr := f.Write(append(data, '\n'))
	err = errors.Join(werr, f.Sync(), f.Close())
	if err == nil && !q.dirty[file] {
		err = syncDirectory(q.state)
	}
	if err != nil {
		// Do not append behind a possibly torn record during this invocation.
		q.failure = err
		return err
	}
	q.dirty[file] = true
	return nil
}

// releaseSelections drops selection quarantine for names whose source window
// selection has since resolved. Other quarantine reasons are kept.
func (q *syncIssues) releaseSelections(names []string) error {
	if q == nil || len(names) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failure != nil {
		return q.failure
	}
	now := time.Now()
	var delta []syncIssue
	for _, n := range names {
		v, ok := q.quarantined[n]
		if !ok || v.Reason != syncWindowSelectionReason {
			continue
		}
		v.LastSeen, v.Released = now, true
		delta = append(delta, v)
	}
	if len(delta) == 0 {
		return nil
	}
	if err := q.appendLocked("quarantine.json", delta); err != nil {
		return err
	}
	for _, v := range delta {
		delete(q.quarantined, v.Name)
		event("sync_name_released", map[string]any{"metric": v.Name, "reason": v.Reason, "phase": v.Phase, "path": filepath.Join(q.state, "quarantine.json")})
	}
	return nil
}

// replay accepts only an incomplete final record as an interrupted append.
// Complete malformed records are corruption and must fail closed.
func (q *syncIssues) replay(file string, dst map[string]syncIssue) error {
	f, err := os.OpenFile(filepath.Join(q.state, file+".jsonl"), os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				if err := f.Truncate(offset); err != nil {
					return err
				}
				return f.Sync()
			}
			return nil
		}
		if err != nil {
			return err
		}
		var rows []syncIssue
		if err := json.Unmarshal(bytes.TrimSpace(line), &rows); err != nil {
			return fmt.Errorf("%s journal: %w", file, err)
		}
		for _, v := range rows {
			if v.Name == "" || v.Reason == "" || v.Phase == "" || v.FirstSeen.IsZero() || v.LastSeen.IsZero() {
				return fmt.Errorf("invalid issue in %s journal", file)
			}
			if prev, ok := dst[v.Name]; !ok || !v.LastSeen.Before(prev.LastSeen) {
				if v.Released {
					delete(dst, v.Name)
					continue
				}
				dst[v.Name] = v
			}
		}
		q.dirty[file] = true
		offset += int64(len(line))
	}
}

// checkpoint materializes the existing sorted JSON array format at phase end
// and on exit. Rename+directory sync precedes journal removal: either old
// snapshot+journal or new snapshot (with an optional duplicate journal) recovers.
func (q *syncIssues) checkpoint() error {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for file, dst := range map[string]map[string]syncIssue{"quarantine.json": q.quarantined, "policy-skipped.json": q.policy} {
		if !q.dirty[file] {
			continue
		}
		all := make([]syncIssue, 0, len(dst))
		for _, v := range dst {
			all = append(all, v)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
		if err := atomicStateJSON(filepath.Join(q.state, file), all); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(q.state, file+".jsonl")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirectory(q.state); err != nil {
			return err
		}
		delete(q.dirty, file)
	}
	return nil
}

func (s *syncer) excludePolicy(name, phase string) (bool, error) {
	if s.o.issues == nil {
		return false, nil
	}
	// A selection retry repairs its own quarantined name; any other phase
	// keeps skipping it until that retry resolves and releases the name.
	if s.o.issues.excluded(name) && !(strings.HasPrefix(phase, "window-retry-") && s.o.issues.retryableSelection(name)) {
		return true, nil
	}
	if len(name) <= chunkstore.MaxMetricNameBytes {
		return false, nil
	}
	err := s.o.issues.record([]syncResidual{{syncHole: syncHole{Name: name}, Reason: "name exceeds store policy limit of 4096 bytes"}}, phase, true)
	return true, err
}
func (s *syncer) quarantineResiduals(phase string) error {
	if s.o.issues == nil {
		return nil
	}
	var rows []syncResidual
	for _, r := range s.sum.residual {
		skip, err := s.excludePolicy(r.Name, phase)
		if err != nil {
			return err
		}
		if !skip {
			rows = append(rows, r)
		}
	}
	return s.o.issues.record(rows, phase, false)
}
