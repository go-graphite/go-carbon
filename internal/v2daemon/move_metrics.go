package v2daemon

import "os"

// MoveStatuses reads atomic receipts without waiting for a copier or owner.
func (a *MoveAdmin) MoveStatuses() []MoveStatus {
	entries, _ := os.ReadDir(a.root)
	var statuses []MoveStatus
	for _, entry := range entries {
		if m, e := a.load(entry.Name()); e == nil {
			statuses = append(statuses, m.status())
		}
	}
	return statuses
}
func (a *MoveAdmin) MoveRegressions() uint64 { return a.rejected.Load() }

// MovePhaseNumber is shared by Graphite and Prometheus collectors.
func MovePhaseNumber(phase string) float64 { return float64(moveRank(phase)) }
