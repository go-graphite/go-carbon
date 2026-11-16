package v2core

import (
	"time"

	"github.com/cockroachdb/pebble"
)

// Sample reports the offline engine's counters and process measurements. The
// caller supplies its start time and offered-point count; owner counters may
// advance independently. This is diagnostic telemetry, not a replay frontier.
func Sample(e *Engine, stage string, start time.Time, offered uint64) map[string]any {
	return coreSample(e, stage, start, offered)
}

// Read decodes numeric raw rows from a caller-owned immutable snapshot. It does
// not interpret the portable parts format or perform production render rollups.
func Read(snapshot *pebble.Snapshot, id uint64) ([]Row, error) { return coreRead(snapshot, id) }

// Shard returns the numeric prototype's routing owner; n must be positive. This
// is not the portable placement-group/routing-vshard placement function.
func Shard(id uint64, n int) int { return coreShard(id, n) }
