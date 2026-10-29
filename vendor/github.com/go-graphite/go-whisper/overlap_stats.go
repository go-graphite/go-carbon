package whisper

import "sync/atomic"

// OverlapKind names the rule that sent a compressed batch down the full
// classic replay and rewrite path (see compressedBatchOverlapKind).
type OverlapKind uint8

const (
	// OverlapNone: the batch took the incremental compressed path.
	OverlapNone OverlapKind = iota
	// OverlapBatchSlot: points of the batch destined for different archives
	// alias one circular slot of a coarse archive.
	OverlapBatchSlot
	// OverlapBufferedRollup: a coarse buffer holds a replacement aggregate
	// behind that archive's encoded watermark.
	OverlapBufferedRollup
	// OverlapSidecarSlot: the out-of-order sidecar holds a different
	// timestamp in the circular slot of a batch interval.
	OverlapSidecarSlot
	// OverlapFuture: the batch holds a point newer than now.
	OverlapFuture
	// OverlapStoredSlot: a batch interval aliases the slot of a newer stored
	// point, a full retention or more ahead of it.
	OverlapStoredSlot

	overlapKinds
)

var overlapKindNames = [overlapKinds]string{"none", "batch_slot", "buffered_rollup", "sidecar_slot", "future", "stored_slot"}

func (k OverlapKind) String() string {
	if k < overlapKinds {
		return overlapKindNames[k]
	}
	return "unknown"
}

// OverlapKinds lists every kind that takes the replay path, for exporters
// that pre-register one series per kind.
func OverlapKinds() []OverlapKind {
	kinds := make([]OverlapKind, 0, overlapKinds-1)
	for k := OverlapNone + 1; k < overlapKinds; k++ {
		kinds = append(kinds, k)
	}
	return kinds
}

var overlapUpdates [overlapKinds]atomic.Uint64

// OverlapUpdates returns the process-wide, monotonic number of compressed
// updates that took the replay path for kind.
func OverlapUpdates(kind OverlapKind) uint64 {
	if kind >= overlapKinds {
		return 0
	}
	return overlapUpdates[kind].Load()
}
