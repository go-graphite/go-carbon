package whisper

import (
	"fmt"
	"sync/atomic"
)

// RewriteBlockFallback names how a rewrite read a compressed block that does
// not decode as an ordinary one: its bytes could not be produced, or it holds
// points off its archive's grid.
type RewriteBlockFallback uint8

const (
	// RewriteBlockExact: the file could not produce the block's bytes, and
	// typed storage served its exact points instead.
	RewriteBlockExact RewriteBlockFallback = iota
	// RewriteBlockAligned: the block held points off its archive's grid,
	// which the rewrite stored on it (see alignBlockPoints).
	RewriteBlockAligned

	rewriteBlockFallbacks
)

var rewriteBlockFallbackNames = [rewriteBlockFallbacks]string{"exact", "aligned"}

func (k RewriteBlockFallback) String() string {
	if k < rewriteBlockFallbacks {
		return rewriteBlockFallbackNames[k]
	}
	return "unknown"
}

// RewriteBlockFallbacks lists every fallback, for exporters that pre-register
// one series each.
func RewriteBlockFallbacks() []RewriteBlockFallback {
	kinds := make([]RewriteBlockFallback, rewriteBlockFallbacks)
	for k := range kinds {
		kinds[k] = RewriteBlockFallback(k)
	}
	return kinds
}

var rewriteBlockFallbackCounts [rewriteBlockFallbacks]atomic.Uint64

// RewriteBlockFallbackCount returns the process-wide, monotonic number of
// blocks rewrites read through the given fallback.
func RewriteBlockFallbackCount(kind RewriteBlockFallback) uint64 {
	if kind >= rewriteBlockFallbacks {
		return 0
	}
	return rewriteBlockFallbackCounts[kind].Load()
}

// rewriteFallbackPoints appends to dst the exact points of archive index's
// block, which buildRewrite could not read as bytes (readErr), from typed
// storage. Without typed storage, or when it refuses the block too, it
// returns the read error, with the typed reader's reason.
func (whisper *Whisper) rewriteFallbackPoints(index int, archive *archiveInfo, block blockRange, dst []dataPoint, readErr error) ([]dataPoint, error) {
	failed := fmt.Errorf("archives[%d].blocks[%d].file.read: %s", index, block.index, readErr)
	reader, ok := whisper.file.(StorageBlockReader)
	if !ok {
		return nil, failed
	}
	points, err := reader.ReadStorageBlock(archive.continuationBlock(index, block))
	if err != nil {
		if err.Error() == readErr.Error() {
			return nil, failed // the stored points themselves are refused
		}
		return nil, fmt.Errorf("%w; exact read: %s", failed, err)
	}
	for _, p := range points {
		dst = append(dst, dataPoint{interval: p.Time, value: p.Value})
	}
	rewriteBlockFallbackCounts[RewriteBlockExact].Add(1)
	return dst, nil
}

// alignBlockPoints moves the points of a block off its archive's grid onto it,
// as a fetch reads them: each to the start of its interval. points are
// strictly increasing and are changed in place. It reports whether any point
// moved.
//
// Such points come only from converted sources: corrupt CWhisper blocks, such
// as one whose raw first point is off the grid, which moves every delta-coded
// point after it. A rewrite keeping them would let a stored point shadow a
// merged late point of the same interval, which reads did not show before.
//
// Of several points in one interval an on-grid one wins. Writers store only
// on-grid points, so it is a later write than the converted points next to it:
// a late point an in-place patch stored into the block before patches declined
// such blocks, which reads then hid behind the off-grid point. Without one, the
// latest wins, which is what a fetch serves.
func alignBlockPoints(points []dataPoint, step int) ([]dataPoint, bool) {
	moved := false
	for _, p := range points {
		if mod(p.interval, step) != 0 {
			moved = true
			break
		}
	}
	if !moved {
		return points, false
	}
	kept, onGrid := points[:0], false
	for _, p := range points {
		offset := mod(p.interval, step)
		p.interval -= offset
		if n := len(kept); n > 0 && kept[n-1].interval == p.interval {
			// An interval's on-grid point is its first, so kept holds it.
			if !onGrid {
				kept[n-1] = p
			}
			continue
		}
		kept, onGrid = append(kept, p), offset == 0
	}
	return kept, true
}
