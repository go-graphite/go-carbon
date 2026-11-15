package downsize

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
}
type syncIssues struct {
	mu                  sync.Mutex
	state               string
	quarantined, policy map[string]syncIssue
}

func openSyncIssues(state string) (*syncIssues, error) {
	q := &syncIssues{state: state, quarantined: map[string]syncIssue{}, policy: map[string]syncIssue{}}
	for file, dst := range map[string]map[string]syncIssue{"quarantine.json": q.quarantined, "policy-skipped.json": q.policy} {
		data, err := os.ReadFile(filepath.Join(state, file))
		if errors.Is(err, os.ErrNotExist) {
			continue
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
func (q *syncIssues) record(rows []syncResidual, phase string, policy bool) error {
	if len(rows) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	dst, file := q.quarantined, "quarantine.json"
	if policy {
		dst, file = q.policy, "policy-skipped.json"
	}
	now := time.Now()
	var added []syncIssue
	for _, r := range rows {
		v, exists := dst[r.Name]
		if !exists {
			v = syncIssue{Name: r.Name, Reason: r.Reason, Phase: phase, FirstSeen: now}
			added = append(added, v)
		}
		v.LastSeen = now
		dst[r.Name] = v
	}
	all := make([]syncIssue, 0, len(dst))
	for _, r := range dst {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	if err := atomicStateJSON(filepath.Join(q.state, file), all); err != nil {
		return err
	}
	for _, r := range added {
		event("sync_name_quarantined", map[string]any{"level": "ERROR", "metric": r.Name, "reason": r.Reason, "phase": r.Phase, "policy_skipped": policy, "path": filepath.Join(q.state, file)})
	}
	return nil
}
func (s *syncer) excludePolicy(name, phase string) (bool, error) {
	if s.o.issues == nil {
		return false, nil
	}
	if s.o.issues.excluded(name) {
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
