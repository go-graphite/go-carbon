package whisper

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// InPlaceResult names how a correction that used to rewrite the whole
// compressed file reached storage: in place, or by rewrite and why.
type InPlaceResult uint8

const (
	// InPlaced: the affected blocks were patched, and later points appended,
	// under the same file.
	InPlaced InPlaceResult = iota
	// InPlaceUnsupported: the storage cannot patch blocks, the file uses Mix
	// aggregation or an older compressed format, or its blocks overlap.
	InPlaceUnsupported
	// InPlaceExtend: the file also outgrew its estimated point size, so the
	// rewrite extends it.
	InPlaceExtend
	// InPlaceOverflow: a patched block no longer fits its capacity.
	InPlaceOverflow
	// InPlaceLateBatch: the colliding batch reaches the newest stored base
	// interval or behind it, which only the replay orders as classic whisper
	// does, so the replay folds the sidecar as before.
	InPlaceLateBatch
	// InPlaceTail: folding the sidecar would encode values past an archive's
	// encoded range, such as a partial coarse aggregate of its sparse data,
	// which the replay recomputes from the batch and an append would not.
	InPlaceTail

	inPlaceResults
)

var inPlaceResultNames = [inPlaceResults]string{"in_place", "rewrite_unsupported", "rewrite_extend", "rewrite_overflow", "rewrite_late_batch", "rewrite_tail"}

func (k InPlaceResult) String() string {
	if k < inPlaceResults {
		return inPlaceResultNames[k]
	}
	return "unknown"
}

// InPlaceResults lists every result, for exporters that pre-register one
// series each.
func InPlaceResults() []InPlaceResult {
	kinds := make([]InPlaceResult, inPlaceResults)
	for k := range kinds {
		kinds[k] = InPlaceResult(k)
	}
	return kinds
}

var rollupCorrections, sidecarSlotMerges, sidecarFolds [inPlaceResults]atomic.Uint64

// RollupCorrections returns the process-wide, monotonic number of buffered
// rollup corrections (see rewriteBufferedRollups) by result.
func RollupCorrections(kind InPlaceResult) uint64 {
	if kind >= inPlaceResults {
		return 0
	}
	return rollupCorrections[kind].Load()
}

// SidecarSlotMerges returns the process-wide, monotonic number of sidecar
// slot collisions (OverlapSidecarSlot) by how their sidecar was folded: in
// place, after which the batch is checked again, or not, in which case the
// batch replays as before.
func SidecarSlotMerges(kind InPlaceResult) uint64 {
	if kind >= inPlaceResults {
		return 0
	}
	return sidecarSlotMerges[kind].Load()
}

// SidecarFolds returns the process-wide, monotonic number of due sidecars
// FoldOutOfOrderInPlace handled, by result: folded in place, or left for the
// ordinary merge rules and why.
func SidecarFolds(kind InPlaceResult) uint64 {
	if kind >= inPlaceResults {
		return 0
	}
	return sidecarFolds[kind].Load()
}

// FoldOutOfOrderInPlace folds the sidecar into the main file in place when
// options make it due, exactly as MergeOutOfOrderWithOptions would merge it,
// and reports whether it did. It never rewrites the file: when the storage
// cannot patch blocks, the sidecar is not due, or the fold cannot be placed in
// the existing blocks, nothing changes and the caller's ordinary merge rules
// (size threshold, rate limit, rewrite) still apply.
//
// A small sidecar is otherwise only merged once it outgrows the size
// threshold, so a few late points stay until an ordinary batch maps onto their
// slots a retention later and the collision replays the whole file. Folding it
// once due, at the cost of the blocks it touches, removes both the collision
// and the sidecar merge every fetch pays meanwhile.
func (whisper *Whisper) FoldOutOfOrderInPlace(options OutOfOrderMergeOptions) (bool, error) {
	if options.MinPoints < 0 || options.MaxPointAge < 0 || options.RetentionMargin < 0 {
		return false, errors.New("out-of-order merge limits must not be negative")
	}
	patcher, reader, result := whisper.inPlaceSupport()
	if result != InPlaced || whisper.oooPath == "" {
		return false, nil
	}
	merge, err := whisper.prepareOutOfOrderMerge(&options, whisper.now(), nil)
	if err != nil || merge == nil {
		return false, err
	}
	if whisper.extensionNeeded() {
		sidecarFolds[InPlaceExtend].Add(1)
		return false, nil
	}
	plan, result, err := whisper.planInPlace(reader, merge.pending)
	if err != nil {
		return false, err
	}
	if result == InPlaced {
		for _, appends := range plan.appends {
			if len(appends) > 0 {
				result = InPlaceTail
				break
			}
		}
	}
	sidecarFolds[result].Add(1)
	if result != InPlaced {
		return false, nil
	}
	if err := whisper.applyInPlace(patcher, plan, merge.pending, merge.op); err != nil {
		return true, err
	}
	whisper.finishOutOfOrderMerge(merge.sidecarPath)
	return true, nil
}

// inPlaceBlock is one block an in-place merge changes.
type inPlaceBlock struct {
	archive int
	block   blockRange        // as stored
	points  []TimeSeriesPoint // the block's points after the merge
	// positions lists the points whose value changed when the timestamps did
	// not; otherwise from is the first position that differs from storage.
	positions []int
	from      int
	state     blockInfo // encoder state after points
	crc       uint32
}

// inPlacePlan is a rewrite's merge, computed without changing anything.
type inPlacePlan struct {
	blocks  []inPlaceBlock
	appends [][]dataPoint
}

// planInPlace computes the changes a rewrite merging pending into the file
// makes to each archive's stored points, when they can be made in place.
//
// buildRewrite streams each archive's blocks in start order into a fresh file
// and merges pending into that stream: extras older than a block are written
// before it, extras within it are merged by mergeExtraWithBuffer (a replace
// extra wins at a stored interval, any other extra fills only a missing one),
// and extras past the last block are appended. With blocks that do not
// overlap, that stream is strictly increasing, so the encoder discards
// nothing and the rewritten archive holds exactly the merged points, in
// blocks it packs anew.
//
// The plan stores the same points in the existing blocks: each changed block
// gets its merged points, extras before a block joining it, and extras past
// the last block are appended. A block is re-encoded from its first point,
// which reproduces the encoder state an incremental writer reaches with those
// points. When they no longer fit, the points that do not carry into the next
// block in time order, as the rewrite's stream would continue there, until a
// block takes them; past the last block they are appended. Only block
// boundaries differ from the rewrite's, as they do between any two rewrites of
// different histories. If the current block would overflow, which would
// rotate the ring, the plan is left to the rewrite.
func (whisper *Whisper) planInPlace(reader StorageBlockReader, pending [][]extraPoint) (*inPlacePlan, InPlaceResult, error) {
	plan := &inPlacePlan{appends: make([][]dataPoint, len(whisper.archives))}
	for i, archive := range whisper.archives {
		extras := pending[i]
		if len(extras) == 0 {
			continue
		}
		for n := 1; n < len(extras); n++ {
			if extras[n].interval <= extras[n-1].interval {
				return nil, InPlaceUnsupported, nil
			}
		}
		if extras[0].interval <= 0 {
			// An empty-slot replacement only affects the buffer in a rewrite.
			return nil, InPlaceUnsupported, nil
		}
		sorted := archive.getSortedBlockRanges()
		for n := range sorted {
			if sorted[n].start == 0 {
				sorted = sorted[:n]
				break
			}
		}
		for n := 1; n < len(sorted); n++ {
			if sorted[n].start <= sorted[n-1].end {
				return nil, InPlaceUnsupported, nil
			}
		}
		if len(sorted) > 0 && archive.blockRanges[archive.cblock.index].start != 0 && sorted[len(sorted)-1].index != archive.cblock.index {
			return nil, InPlaceUnsupported, nil
		}
		next := 0
		var carry []TimeSeriesPoint
		for _, block := range sorted {
			if next == len(extras) && len(carry) == 0 {
				break
			}
			mid := next
			for mid < len(extras) && extras[mid].interval < block.start {
				mid++
			}
			end := mid
			for end < len(extras) && extras[end].interval <= block.end {
				end++
			}
			before, within := extras[next:mid], extras[mid:end]
			next = end
			if len(carry) == 0 && len(before) == 0 && len(within) == 0 {
				continue
			}
			change, left, result, err := whisper.planBlock(reader, i, archive, block, carry, before, within)
			if err != nil || result != InPlaced {
				return nil, result, err
			}
			if change != nil {
				plan.blocks = append(plan.blocks, *change)
			}
			carry = left
		}
		for _, p := range carry {
			plan.appends[i] = append(plan.appends[i], dataPoint{interval: p.Time, value: p.Value})
		}
		plan.appends[i] = append(plan.appends[i], dataPointsOf(extras[next:])...)
	}
	return plan, InPlaced, nil
}

type inPlaceScratch struct {
	stored []TimeSeriesPoint
	points []dataPoint
}

var inPlaceBuffers = sync.Pool{New: func() any { return new(inPlaceScratch) }}

// planBlock merges carry and before (all older than block) and within (all
// inside it) into the block's stored points. It returns the change, nil when
// nothing changes, and the points that no longer fit, to carry on.
func (whisper *Whisper) planBlock(reader StorageBlockReader, index int, archive *archiveInfo, block blockRange, carry []TimeSeriesPoint, before, within []extraPoint) (*inPlaceBlock, []TimeSeriesPoint, InPlaceResult, error) {
	scratch := inPlaceBuffers.Get().(*inPlaceScratch)
	defer func() {
		// Keep at most 1 MiB per slice; unusually large blocks remain unpooled.
		if cap(scratch.stored) > 1<<16 {
			scratch.stored = nil
		}
		if cap(scratch.points) > 1<<16 {
			scratch.points = nil
		}
		inPlaceBuffers.Put(scratch)
	}()
	var stored []TimeSeriesPoint
	var err error
	if input, ok := reader.(interface {
		ReadStorageBlockInto(ContinuationBlock, []TimeSeriesPoint) ([]TimeSeriesPoint, error)
	}); ok {
		stored, err = input.ReadStorageBlockInto(archive.continuationBlock(index, block), scratch.stored[:0])
		if err == nil {
			scratch.stored = stored
		}
	} else {
		stored, err = reader.ReadStorageBlock(archive.continuationBlock(index, block))
	}
	if err != nil {
		return nil, nil, 0, err
	}
	for _, p := range stored {
		if p.Time%archive.secondsPerPoint != 0 {
			// Points off the grid (a converted corrupt block): the rewrite
			// stores them as reads serve them (alignBlockPoints), which a
			// patch keeping the stored points would not.
			return nil, nil, InPlaceUnsupported, nil
		}
	}
	merged := make([]TimeSeriesPoint, 0, len(carry)+len(before)+len(stored)+len(within))
	merged = append(merged, carry...)
	for _, p := range before {
		merged = append(merged, TimeSeriesPoint{Time: p.interval, Value: p.value})
	}
	s, w := 0, 0
	for s < len(stored) && w < len(within) {
		switch {
		case stored[s].Time < within[w].interval:
			merged = append(merged, stored[s])
			s++
		case stored[s].Time > within[w].interval:
			merged = append(merged, TimeSeriesPoint{Time: within[w].interval, Value: within[w].value})
			w++
		case within[w].replace:
			merged = append(merged, TimeSeriesPoint{Time: within[w].interval, Value: within[w].value})
			s++
			w++
		default:
			merged = append(merged, stored[s])
			s++
			w++
		}
	}
	merged = append(merged, stored[s:]...)
	for _, p := range within[w:] {
		merged = append(merged, TimeSeriesPoint{Time: p.interval, Value: p.value})
	}
	if samePoints(merged, stored) {
		return nil, nil, InPlaced, nil
	}

	if cap(scratch.points) < len(merged) {
		scratch.points = make([]dataPoint, len(merged))
	}
	points := scratch.points[:len(merged)]
	for n, p := range merged {
		points[n] = dataPoint{interval: p.Time, value: p.Value}
	}
	state, crc, fit := archive.reencodeBlock(block.index, points)
	if fit == 0 || fit < len(merged) && block.index == archive.cblock.index {
		return nil, nil, InPlaceOverflow, nil
	}
	kept, left := merged[:fit], merged[fit:]
	change := &inPlaceBlock{archive: index, block: block, points: kept, from: -1, state: state, crc: crc}
	if len(kept) == len(stored) {
		for n := range kept {
			if kept[n].Time != stored[n].Time {
				change.positions = nil
				break
			}
			if math.Float64bits(kept[n].Value) != math.Float64bits(stored[n].Value) {
				change.positions = append(change.positions, n)
			}
		}
	}
	if change.positions == nil {
		change.from = len(kept)
		if len(stored) < change.from {
			change.from = len(stored)
		}
		for n := 0; n < change.from; n++ {
			if kept[n].Time != stored[n].Time || math.Float64bits(kept[n].Value) != math.Float64bits(stored[n].Value) {
				change.from = n
				break
			}
		}
	}
	if change.positions == nil && change.from == len(kept) && len(kept) == len(stored) {
		// The block keeps its points; only the excess carries on.
		change = nil
	}
	return change, left, InPlaced, nil
}

// samePoints reports whether a and b hold the same points, values compared
// bitwise.
func samePoints(a, b []TimeSeriesPoint) bool {
	if len(a) != len(b) {
		return false
	}
	for n := range a {
		if a[n].Time != b[n].Time || math.Float64bits(a[n].Value) != math.Float64bits(b[n].Value) {
			return false
		}
	}
	return true
}

// inPlaceSupport reports whether the file's storage can take planInPlace's
// changes, and otherwise why not.
func (whisper *Whisper) inPlaceSupport() (StorageBlockPatcher, StorageBlockReader, InPlaceResult) {
	patcher, _ := whisper.file.(StorageBlockPatcher)
	reader, _ := whisper.file.(StorageBlockReader)
	if patcher == nil || reader == nil || !patcher.CanReplaceStoragePoints() ||
		!whisper.compressed || whisper.aggregationMethod == Mix || whisper.compVersion != 1 {
		return nil, nil, InPlaceUnsupported
	}
	return patcher, reader, InPlaced
}

// applyInPlace makes the plan's changes: the buffers get the rewrite's
// precedence for op, before any watermark moves, then the blocks are patched,
// the appends written and the header stored. On atomic storage they commit
// with the rest of the update; an error fails the update.
func (whisper *Whisper) applyInPlace(patcher StorageBlockPatcher, plan *inPlacePlan, pending [][]extraPoint, op string) error {
	for i, archive := range whisper.archives {
		applyExtraToBuffer(archive.buffer, pending[i], op, archive)
	}
	for _, change := range plan.blocks {
		archive := whisper.archives[change.archive]
		index := change.block.index
		if change.positions != nil {
			if err := patcher.ReplaceStoragePoints(archive.continuationBlock(change.archive, change.block), change.positions, change.points); err != nil {
				return err
			}
		} else {
			block := blockRange{index: index, start: change.points[0].Time, end: change.points[len(change.points)-1].Time, count: len(change.points) - 1}
			if err := patcher.RewriteStorageBlock(archive.continuationBlock(change.archive, block), change.from, change.points); err != nil {
				return err
			}
			archive.blockRanges[index] = block
		}
		archive.blockRanges[index].crc32 = change.crc
		if index == archive.cblock.index {
			archive.cblock = change.state
		}
	}
	for i, archive := range whisper.archives {
		if len(plan.appends[i]) == 0 {
			continue
		}
		if _, err := archive.appendToBlockAndRotate(plan.appends[i]); err != nil {
			return err
		}
	}
	return whisper.WriteHeaderCompressed()
}

// patchRollupsInPlace applies rewriteBufferedRollups' "rollup" rewrite in
// place when planInPlace can, and reports whether it did. The rewrite's
// replacements are all marked replace, so stored intervals only change value.
func (whisper *Whisper) patchRollupsInPlace(recomputed [][]dataPoint, extend bool) (bool, error) {
	patcher, reader, result := whisper.inPlaceSupport()
	if result == InPlaced && extend {
		result = InPlaceExtend
	}
	var plan *inPlacePlan
	pending := make([][]extraPoint, len(recomputed))
	for i := range recomputed {
		pending[i] = markExtras(recomputed[i], true)
	}
	if result == InPlaced {
		var err error
		if plan, result, err = whisper.planInPlace(reader, pending); err != nil {
			return false, err
		}
	}
	rollupCorrections[result].Add(1)
	if result != InPlaced {
		return false, nil
	}
	return true, whisper.applyInPlace(patcher, plan, pending, "rollup")
}

// mergeSidecarForBatchInPlace handles a sidecar slot collision of points by
// folding the sidecar in place (mergeOutOfOrderInPlace), when the batch then
// takes a path the replay is equivalent to, and reports whether it did.
//
// The replay folds the sidecar first, then writes the batch into a classic
// copy of the merged file and rewrites the file from that copy. Once the
// sidecar is folded within the stored range, a batch strictly newer than
// every stored base interval only appends: the incremental path stores the
// same points and propagates the same windows as classic whisper, as for any
// ordinary write, and the caller still replays it for any other overlap. The
// result equals the replay's, except where the replay's rewrite encodes an
// unfinished coarse window whose completion is then corrected to a value
// classic whisper does not store; there it keeps classic's. A batch reaching
// the newest stored base interval or behind it would divert points to a new
// sidecar, whose reads only approximate classic ordering, and a fold
// appending past an archive's encoded range would encode such an unfinished
// window itself; both replay as before.
func (whisper *Whisper) mergeSidecarForBatchInPlace(points []*TimeSeriesPoint) (bool, error) {
	base := whisper.archives[0]
	_, latest := base.getRange()
	for offset := 0; offset < len(base.buffer); offset += PointSize {
		if interval := unpackInt(base.buffer[offset:]); interval > latest {
			latest = interval
		}
	}
	for _, p := range points {
		if p.Time-mod(p.Time, base.secondsPerPoint) <= latest {
			sidecarSlotMerges[InPlaceLateBatch].Add(1)
			return false, nil
		}
	}
	return whisper.mergeOutOfOrderInPlace(whisper.now())
}

// mergeOutOfOrderInPlace folds the sidecar into the main file as the merge
// rewrite of updateCompressedOverlappingBatch would (prepareOutOfOrderMerge
// with no options, a "compact" rewrite, then removing the sidecar), but in
// place, and reports whether it did. Nothing changes when it does not.
func (whisper *Whisper) mergeOutOfOrderInPlace(now time.Time) (bool, error) {
	return whisper.foldOutOfOrderInPlace(now, &sidecarSlotMerges)
}

// foldOutOfOrderInPlace is mergeOutOfOrderInPlace counting its decision, if
// it reaches one, in counts.
func (whisper *Whisper) foldOutOfOrderInPlace(now time.Time, counts *[inPlaceResults]atomic.Uint64) (bool, error) {
	result, decided, folded, err := whisper.foldOutOfOrderInPlaceResult(now)
	if decided {
		counts[result].Add(1)
	}
	return folded, err
}

// foldOutOfOrderInPlaceResult is foldOutOfOrderInPlace without counting. It
// returns the decision and whether it reached one (not when reading the
// sidecar fails or there is nothing to merge), and whether it changed the
// file: only an InPlaced decision does, and when applying it fails, err says
// so and the file must be discarded.
func (whisper *Whisper) foldOutOfOrderInPlaceResult(now time.Time) (result InPlaceResult, decided, folded bool, err error) {
	patcher, reader, result := whisper.inPlaceSupport()
	if result == InPlaced && whisper.extensionNeeded() {
		result = InPlaceExtend
	}
	if result != InPlaced {
		return result, true, false, nil
	}
	merge, err := whisper.prepareOutOfOrderMerge(nil, now, nil)
	if err != nil || merge == nil {
		return result, false, false, err
	}
	plan, result, err := whisper.planInPlace(reader, merge.pending)
	if err != nil {
		return result, false, false, err
	}
	if result == InPlaced {
		for _, appends := range plan.appends {
			if len(appends) > 0 {
				result = InPlaceTail
				break
			}
		}
	}
	if result != InPlaced {
		return result, true, false, nil
	}
	if err := whisper.applyInPlace(patcher, plan, merge.pending, merge.op); err != nil {
		return result, true, true, err
	}
	whisper.finishOutOfOrderMerge(merge.sidecarPath)
	return result, true, true, nil
}

// extensionNeeded is computeExtendedRetentions' decision without its side
// effects.
func (whisper *Whisper) extensionNeeded() bool {
	for _, arc := range whisper.archives {
		totalPoints, totalBlocks := arc.blockStatsBeforeCurrent()
		if totalPoints > 0 && float32(totalBlocks*arc.blockSize)/float32(totalPoints) > arc.avgCompressedPointSize {
			return true
		}
	}
	return false
}

// continuationBlock describes block of the archive at index for typed block
// storage, as appendToBlockAndRotate does.
func (archive *archiveInfo) continuationBlock(index int, block blockRange) ContinuationBlock {
	return ContinuationBlock{Archive: index, Index: block.index, Offset: archive.blockOffset(block.index),
		Size: archive.blockSize, Step: archive.secondsPerPoint, First: block.start, Last: block.end, Count: block.count + 1}
}

// reencodeBlock encodes points from the start of block index, as appending
// them to that empty block would, and returns how many fit, the encoder state
// after them and the checksum CheckIntegrity expects: over the written bytes
// for the current block and over the whole block otherwise.
func (archive *archiveInfo) reencodeBlock(index int, points []dataPoint) (state blockInfo, crc uint32, fit int) {
	scratch := archiveInfo{Retention: archive.Retention, offset: archive.offset, blockSize: archive.blockSize,
		blockRanges: make([]blockRange, len(archive.blockRanges)),
		cblock:      blockInfo{index: index, lastByteOffset: archive.blockOffset(index), lastByteBitPos: 7}}
	buf := make([]byte, archive.blockSize+2*MaxCompressedPointSize+2*endOfBlockSize)
	_, left, rotate := scratch.AppendPointsToBlock(buf, points)
	fit = len(points) - len(left)
	if fit == 0 || scratch.cblock.count+1 != fit {
		return blockInfo{}, 0, 0
	}
	if index == archive.cblock.index && !rotate {
		return scratch.cblock, scratch.cblock.crc32, fit
	}
	if rotate {
		// AppendPointsToBlock checksummed the full block when it filled it.
		return scratch.cblock, scratch.cblock.crc32, fit
	}
	return scratch.cblock, crc32(buf[:archive.blockSize], 0), fit
}
