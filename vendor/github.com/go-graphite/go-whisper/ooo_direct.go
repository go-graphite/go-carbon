package whisper

import (
	"sort"
	"sync/atomic"
	"time"
)

// Direct late writes.
//
// The sidecar exists because a compressed block is an append-only bit stream:
// a point behind the encoded watermark cannot be inserted without re-encoding
// the block, which for a real file means rewriting it. Storage that keeps each
// block's decoded points addressable (StorageBlockPatcher) re-encodes a block
// from any position cheaply, so with Options.OutOfOrderDirect a late point is
// stored where classic whisper stores it: in its slot, replacing what the slot
// held, with the coarse windows it changes recomputed and replaced, as classic
// propagation would. That is the merge MergeOutOfOrder performs for a sidecar
// holding exactly these points, applied in place within the same update, so
// reads need no sidecar overlay and no later merge or replay is due.

// DirectLateResult names how an update's late base points reached storage:
// directly into their blocks, or diverted to the sidecar and why.
type DirectLateResult uint8

const (
	// DirectWritten: stored in the blocks covering their intervals, with the
	// coarse windows they change recomputed in place.
	DirectWritten DirectLateResult = iota
	// DirectUnsupported: the storage cannot patch blocks, the file uses Mix
	// aggregation or an older compressed format, or its blocks overlap.
	DirectUnsupported
	// DirectUnfolded: the file already has a sidecar that could not be folded
	// in place first; a direct write next to it would lose to its older values.
	DirectUnfolded
	// DirectOverflow: the current block would overflow, rotating the ring.
	DirectOverflow
	// DirectTail: a point or a recomputed window lies past its archive's
	// encoded range and is neither held by a buffer nor still to be flushed
	// from the finer archive's buffer.
	DirectTail
	// DirectCoarse: a point a coarse archive rejected that is not a base
	// correction (no direct path; the sidecar keeps its precedence).
	DirectCoarse

	directResults
)

var directResultNames = [directResults]string{"direct", "sidecar_unsupported", "sidecar_unfolded", "sidecar_overflow", "sidecar_tail", "sidecar_coarse"}

func (k DirectLateResult) String() string {
	if k < directResults {
		return directResultNames[k]
	}
	return "unknown"
}

// DirectLateResults lists every result, for exporters that pre-register one
// series each.
func DirectLateResults() []DirectLateResult {
	kinds := make([]DirectLateResult, directResults)
	for k := range kinds {
		kinds[k] = DirectLateResult(k)
	}
	return kinds
}

var directLateUpdates, directLatePoints [directResults]atomic.Uint64
var directSidecarFolds [inPlaceResults]atomic.Uint64

// DirectLateWrites returns the process-wide, monotonic number of updates
// whose late base points took each path (an update can count under several).
func DirectLateWrites(kind DirectLateResult) uint64 {
	if kind >= directResults {
		return 0
	}
	return directLateUpdates[kind].Load()
}

// DirectLatePoints is DirectLateWrites in points.
func DirectLatePoints(kind DirectLateResult) uint64 {
	if kind >= directResults {
		return 0
	}
	return directLatePoints[kind].Load()
}

// DirectSidecarFolds returns the process-wide, monotonic number of existing
// sidecars an update with direct late writes tried to fold first, by result.
func DirectSidecarFolds(kind InPlaceResult) uint64 {
	if kind >= inPlaceResults {
		return 0
	}
	return directSidecarFolds[kind].Load()
}

func countDirect(kind DirectLateResult, points int) {
	if points == 0 {
		return
	}
	directLateUpdates[kind].Add(1)
	directLatePoints[kind].Add(uint64(points))
}

// directLateWrites reports whether late points are to be written into their
// blocks rather than diverted.
func (whisper *Whisper) directLateWrites() bool {
	return whisper.oooEnabled() && whisper.opts.OutOfOrderDirect
}

// foldSidecarForDirect folds an existing sidecar in place before an update
// with direct late writes, so the update's late points do not land next to
// older sidecar values that reads would let win. It reports whether the file
// is now free of a sidecar.
func (whisper *Whisper) foldSidecarForDirect() (bool, error) {
	if whisper.oooPath == "" {
		if err := whisper.detectOOO(); err != nil {
			return false, err
		}
		if whisper.oooPath == "" {
			return true, nil
		}
	}
	if whisper.oooBroken {
		directSidecarFolds[InPlaceUnsupported].Add(1)
		return false, nil
	}
	folded, err := whisper.foldOutOfOrderInPlace(whisper.now(), &directSidecarFolds)
	if err != nil {
		return false, err
	}
	if !folded && whisper.oooPath != "" && whisper.atomicStorage() {
		if err := whisper.MergeOutOfOrder(); err != nil {
			return false, err
		}
	}
	return folded || whisper.oooPath == "", nil
}

// FoldOutOfOrderInPlaceAt folds the file's sidecar into its blocks in place
// as an update with direct late writes does first (foldSidecarForDirect),
// reading the sidecar as of now, and reports the decision and whether the
// file is now free of a sidecar. It counts nothing; the caller does.
//
// Folding applies whenever the storage can place the merge in the existing
// blocks, whatever the sidecar's size or age. Otherwise (a result other than
// InPlaced: storage that cannot patch blocks, an unusable sidecar, a merge
// that would extend the file, overflow a block or append past an archive's
// encoded range) nothing changes and the sidecar stays. When err is set after
// an InPlaced decision, applying it failed part way and the file must be
// discarded.
func (whisper *Whisper) FoldOutOfOrderInPlaceAt(now time.Time) (InPlaceResult, bool, error) {
	if whisper.oooPath == "" {
		if err := whisper.detectOOO(); err != nil {
			return InPlaceUnsupported, false, err
		}
		if whisper.oooPath == "" {
			return InPlaced, true, nil
		}
	}
	if whisper.oooBroken {
		return InPlaceUnsupported, false, nil
	}
	result, _, folded, err := whisper.foldOutOfOrderInPlaceResult(now)
	if err != nil {
		return result, false, err
	}
	return result, folded || whisper.oooPath == "", nil
}

// writeLateInPlace stores the late base points the encoder rejected in the
// blocks covering their intervals and recomputes, in place, the coarse windows
// they change. It returns the points left for the sidecar. sidecarFree is
// whether the file has no sidecar a direct write would have to be ordered
// against.
//
// Classic whisper writes a late point into its slot, replacing the slot's
// value, then propagates: each coarse window it falls into is re-aggregated
// from the finer archive's slots, subject to xFilesFactor, and written, one
// level after another. recomputeArchiveAggregates computes exactly those
// windows from the union of the file's points and the late ones, and
// planInPlace stores the late points and the recomputed windows as replacing
// values in the existing blocks, as MergeOutOfOrder's rewrite would for a
// sidecar holding just these points. A recomputed window whose value is still
// buffered replaces it in the buffer; one whose inputs are still buffered in
// the finer archive is left to that buffer's flush, which aggregates the
// corrected values when the window completes, as it does for any window.
func (whisper *Whisper) writeLateInPlace(dropped []oooPoint, sidecarFree bool, rollups [][]dataPoint) ([]oooPoint, error) {
	if len(dropped) == 0 || !whisper.directLateWrites() {
		return dropped, nil
	}
	base := whisper.archives[0]
	var late []dataPoint
	var rest []oooPoint
	coarse := 0
	for _, d := range dropped {
		if d.retention == base.MaxRetention() {
			late = append(late, d.point)
		} else {
			rest = append(rest, d)
			coarse++
		}
	}
	countDirect(DirectCoarse, coarse)
	if len(late) == 0 {
		return dropped, nil
	}
	divert := func(kind DirectLateResult) ([]oooPoint, error) {
		countDirect(kind, len(late))
		return dropped, nil
	}
	if !sidecarFree {
		return divert(DirectUnfolded)
	}
	patcher, reader, support := whisper.inPlaceSupport()
	if support != InPlaced {
		return divert(DirectUnsupported)
	}

	// One value per interval, the batch's last, ascending.
	last := make(map[int]int, len(late))
	for i, p := range late {
		last[p.interval] = i
	}
	unique := late[:0]
	for i, p := range late {
		if p.interval > 0 && last[p.interval] == i {
			unique = append(unique, p)
		}
	}
	late = unique
	sort.Slice(late, func(a, b int) bool { return late[a].interval < late[b].interval })

	// A late point newer than the encoded watermark but older than the buffer
	// (a window that never reached the buffer) would have to start a window
	// past the blocks; it keeps the sidecar path.
	_, until := base.getRange()
	keep := late[:0]
	var tail []oooPoint
	for _, p := range late {
		if until == 0 || p.interval > until {
			tail = append(tail, oooPoint{retention: base.MaxRetention(), point: p})
			continue
		}
		keep = append(keep, p)
	}
	late = keep
	countDirect(DirectTail, len(tail))
	rest = append(rest, tail...)
	if len(late) == 0 {
		return rest, nil
	}

	extras := make([][]dataPoint, len(whisper.archives))
	extras[0] = late
	recomputed, err := whisper.recomputeArchiveAggregates(extras, nil)
	if err != nil {
		return nil, err
	}
	pending := make([][]extraPoint, len(whisper.archives))
	blocks := make([][]extraPoint, len(whisper.archives))
	for i, archive := range whisper.archives {
		pending[i] = combineExtras(extras[i], recomputed[i], i == 0)
		if i == 0 {
			blocks[i] = pending[i]
			continue
		}
		_, encoded := archive.getRange()
		for _, p := range pending[i] {
			switch {
			case encoded != 0 && p.interval <= encoded:
				blocks[i] = append(blocks[i], p)
			case archive.buffers(p.interval):
				// applyInPlace replaces the buffered value.
			case whisper.archives[i-1].bufferedWindow(p.interval):
				// The finer buffer's flush aggregates the corrected window.
			default:
				countDirect(DirectTail, len(late))
				return dropped, nil
			}
		}
	}

	plan, result, err := whisper.planInPlace(reader, blocks)
	if err != nil {
		return nil, err
	}
	switch {
	case result == InPlaceOverflow:
		return divert(DirectOverflow)
	case result != InPlaced:
		return divert(DirectUnsupported)
	}
	for _, appends := range plan.appends {
		if len(appends) > 0 {
			return divert(DirectTail)
		}
	}
	if err := whisper.applyInPlace(patcher, plan, pending, "compact"); err != nil {
		return nil, err
	}
	// The update's buffer flushes queued rollups aggregated from the stored
	// window before these points were written; a window recomputed here
	// includes them and supersedes that rollup.
	for j := 1; j < len(rollups) && j < len(recomputed); j++ {
		rollups[j] = withoutIntervals(rollups[j], recomputed[j])
	}
	countDirect(DirectWritten, len(late))
	return rest, nil
}

// withoutIntervals returns points without those at an interval of drop.
func withoutIntervals(points, drop []dataPoint) []dataPoint {
	if len(points) == 0 || len(drop) == 0 {
		return points
	}
	skip := make(map[int]bool, len(drop))
	for _, p := range drop {
		skip[p.interval] = true
	}
	kept := points[:0]
	for _, p := range points {
		if !skip[p.interval] {
			kept = append(kept, p)
		}
	}
	return kept
}

// buffers reports whether archive's buffer holds interval.
func (archive *archiveInfo) buffers(interval int) bool {
	for offset := 0; offset+PointSize <= len(archive.buffer); offset += PointSize {
		if unpackInt(archive.buffer[offset:]) == interval {
			return true
		}
	}
	return false
}

// bufferedWindow reports whether archive's buffer holds points of the next
// archive's window starting at start, which its flush will aggregate.
func (archive *archiveInfo) bufferedWindow(start int) bool {
	if archive.next == nil || !archive.hasBuffer() {
		return false
	}
	for offset := 0; offset+PointSize <= len(archive.buffer); offset += PointSize {
		if interval := unpackInt(archive.buffer[offset:]); interval != 0 && archive.AggregateInterval(interval) == start {
			return true
		}
	}
	return false
}

// hasPoints reports whether any list holds a point.
func hasPoints(lists [][]dataPoint) bool {
	for _, l := range lists {
		if len(l) > 0 {
			return true
		}
	}
	return false
}
