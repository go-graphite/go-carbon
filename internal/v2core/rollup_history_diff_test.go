package v2core

import (
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Seals may change physical layout; resolved timestamps and value bits must
// retain the original logical history across every epoch and restart cut.
func TestRollupHistoryDifferential(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	closedHistoryLogicalDifferential(t)
}

// Legacy parts without record kinds must resolve to the same logical history.
// The original logical hashes are also verified on the unmodified base with
// LegacyKinds enabled; no format-specific golden is needed.
func TestRollupHistoryDifferentialLegacyKinds(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	// Parallel subtests outlive this function: reset in a cleanup.
	v2part.LegacyKinds.Store(true)
	t.Cleanup(func() { v2part.LegacyKinds.Store(false) })
	closedHistoryLogicalDifferential(t)
}
