package whisper

import (
	"fmt"
	"math"
	"sort"
)

// A replay can encode an unfinished coarse window. Once finer writes complete
// it, its replacement sits in the coarse buffer behind the encoded watermark.
// Preserve that replacement with a tail correction, rather than replaying all
// retained archives and encoding another unfinished window on every cycle.
// Late, future and sidecar writes, and a batch mixing late and fresh points,
// still take the classic circular write ordering of a full replay.
func (whisper *Whisper) materializeBufferedContinuation(points []*TimeSeriesPoint, now int) error {
	if !whisper.oooEnabled() || whisper.oooPath != "" || whisper.oooBroken || len(points) == 0 || points[0].Time > now {
		return nil
	}
	var corrections [][]dataPoint
	for i, archive := range whisper.archives[1:] {
		for offset := 0; offset < len(archive.buffer); offset += PointSize {
			point := unpackDataPoint(archive.buffer[offset:])
			if !archive.bufferedBehindWatermark(point.interval) {
				continue
			}
			if corrections == nil {
				corrections = make([][]dataPoint, len(whisper.archives))
			}
			corrections[i+1] = append(corrections[i+1], point)
		}
	}
	if corrections == nil {
		return nil
	}
	base := whisper.archives[0]
	_, latest := base.getRange()
	for offset := 0; offset < len(base.buffer); offset += PointSize {
		if interval := unpackInt(base.buffer[offset:]); interval > latest {
			latest = interval
		}
	}
	oldest := points[len(points)-1].Time
	if now-oldest > base.MaxRetention() || base.Interval(oldest) <= latest {
		return nil
	}
	for i := range corrections {
		sort.Slice(corrections[i], func(a, b int) bool { return corrections[i][a].interval < corrections[i][b].interval })
	}
	recomputed, err := whisper.recomputeArchiveAggregates(make([][]dataPoint, len(whisper.archives)), corrections)
	if err != nil {
		return err
	}
	rets, _, _ := whisper.computeExtendedRetentions()
	return whisper.rewrite(rets, "rollup", func(i int) []extraPoint { return markExtras(recomputed[i], true) })
}

// bufferedBehindWatermark reports whether a buffered interval is already
// covered by this archive's encoded blocks. A replay or compaction can encode an
// unfinished window; its finished replacement then lands behind the watermark,
// where a flush would demote it to a gap-filling sidecar value. Detection in
// compressedBatchOverlaps and remediation in materializeBufferedContinuation
// must agree on this rule.
func (archive *archiveInfo) bufferedBehindWatermark(interval int) bool {
	return interval != 0 && interval <= archive.cblock.pn1.interval
}

// Materialize buffered rollups under the old policy before changing it. Reading
// those buffers under the new aggregation would retroactively change history.
func (whisper *Whisper) materializeCompressedRollups() error {
	extras := make([][]extraPoint, len(whisper.archives))
	rets := make([]*Retention, len(whisper.archives))
	for i, archive := range whisper.archives {
		points, err := whisper.fetchCompressed(1, int64(maxInt), archive)
		if err != nil {
			return err
		}
		values := make(map[int]dataPoint, len(points))
		for _, point := range points {
			values[point.interval] = point
		}
		for _, point := range values {
			extras[i] = append(extras[i], extraPoint{dataPoint: point, replace: true})
		}
		sort.Slice(extras[i], func(a, b int) bool { return extras[i][a].interval < extras[i][b].interval })
		ret := archive.Retention
		rets[i] = &ret
	}
	return whisper.rewrite(rets, "batch", func(i int) []extraPoint { return extras[i] })
}

// compressedBatchOverlaps reports circular-slot collisions whose write order
// cannot be preserved by deferred propagation or sidecar timestamp precedence.
//
// skipcq: GO-R1005
func (whisper *Whisper) compressedBatchOverlaps(points []*TimeSeriesPoint, now int) (bool, error) {
	if !whisper.compressed || whisper.aggregationMethod == Mix {
		return false, nil
	}
	var sidecar *Whisper
	if whisper.oooPath != "" {
		var err error
		sidecar, err = whisper.oooSidecar(false)
		if err != nil {
			return false, err
		}
	}
	var finer []*TimeSeriesPoint
	remaining := points
	latest := 0
	future := len(points) > 0 && points[0].Time > now
	for index, archive := range whisper.archives {
		current, rest := extractPoints(remaining, now, archive.MaxRetention())
		remaining = rest
		slots := make(map[int]int, len(finer))
		for _, point := range finer {
			interval := point.Time - mod(point.Time, archive.secondsPerPoint)
			slots[mod(interval/archive.secondsPerPoint, archive.numberOfPoints)] = interval
		}
		for _, point := range current {
			interval := point.Time - mod(point.Time, archive.secondsPerPoint)
			if previous, ok := slots[mod(interval/archive.secondsPerPoint, archive.numberOfPoints)]; ok && previous != interval {
				return true, nil
			}
		}
		finer = append(finer, current...)
		// Include finer buffers in the bound: their virtual aggregates may be
		// newer than this archive's encoded watermark.
		_, end := archive.getRange()
		if end > latest {
			latest = end
		}
		for offset := 0; offset < len(archive.buffer); offset += PointSize {
			interval := unpackInt(archive.buffer[offset:])
			// A rewritten partial window may acquire a newer aggregate in
			// its buffer. Preserve that replacement before flushing would
			// demote it to a gap-filling coarse sidecar value.
			if index > 0 && archive.bufferedBehindWatermark(interval) {
				return true, nil
			}
			if interval > latest {
				latest = interval
			}
		}
		if len(finer) == 0 || (!whisper.oooEnabled() && sidecar == nil && !future) {
			continue
		}
		intervals := make(map[int]int, len(finer))
		oldest := maxInt
		for _, point := range finer {
			interval := point.Time - mod(point.Time, archive.secondsPerPoint)
			intervals[mod(interval/archive.secondsPerPoint, archive.numberOfPoints)] = interval
			if interval < oldest {
				oldest = interval
			}
		}
		if sidecar != nil {
			if collides, err := whisper.sidecarSlotsCollide(sidecar, index, intervals, now); err != nil || collides {
				return collides, err
			}
		}
		if future || (whisper.oooEnabled() && latest >= oldest+archive.MaxRetention()) {
			from := oldest + archive.MaxRetention()
			if future {
				from = 1
			}
			stored, err := whisper.fetchCompressed(int64(from), int64(maxInt), archive)
			if err != nil {
				return false, err
			}
			for _, point := range stored {
				slot := mod(point.interval/archive.secondsPerPoint, archive.numberOfPoints)
				if interval, ok := intervals[slot]; ok && point.interval != interval && (future || point.interval > interval) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// sidecarSlotsCollide reports whether the sidecar archive at index holds a
// different timestamp in the slot of any of intervals, keyed by slot.
func (whisper *Whisper) sidecarSlotsCollide(sidecar *Whisper, index int, intervals map[int]int, now int) (bool, error) {
	sa := sidecar.archives[index]
	base := sidecar.getBaseInterval(sa)
	if base == 0 {
		return false, nil
	}
	batch := make([]int, 0, len(intervals))
	for _, interval := range intervals {
		batch = append(batch, interval)
	}
	stored, err := sidecar.readSlotIntervals(sa, base, batch)
	if err != nil {
		return false, err
	}
	retention := whisper.archives[index].MaxRetention()
	for i, interval := range batch {
		previous := stored[i]
		if previous == 0 || previous == interval {
			continue
		}
		// With no coarse archives, an expired sidecar alias has no remaining
		// observable value to materialize. Multi-archive files must preserve
		// its retained rollups first.
		if len(whisper.archives) == 1 && previous < now-retention && previous < interval {
			continue
		}
		return true, nil
	}
	return false, nil
}

// maxSlotRangeRead bounds the span readSlotIntervals reads with one call.
const maxSlotRangeRead = 64 << 10

// readSlotIntervals returns the timestamp stored in the slot of each interval
// of an uncompressed archive. A batch maps to a narrow range of slots, so read
// that range at once instead of making one system call per point.
func (whisper *Whisper) readSlotIntervals(archive *archiveInfo, base int, intervals []int) ([]int, error) {
	offsets := make([]int64, len(intervals))
	lo, hi := int64(math.MaxInt64), int64(math.MinInt64)
	for i, interval := range intervals {
		offsets[i] = archive.PointOffset(base, interval)
		if offsets[i] < lo {
			lo = offsets[i]
		}
		if offsets[i] > hi {
			hi = offsets[i]
		}
	}
	stored := make([]int, len(intervals))
	if span := hi - lo + PointSize; len(intervals) > 1 && span <= maxSlotRangeRead {
		buf := make([]byte, span)
		if err := whisper.fileReadAt(buf, lo); err != nil {
			return nil, err
		}
		for i, offset := range offsets {
			stored[i] = unpackInt(buf[offset-lo:])
		}
		return stored, nil
	}
	var raw [PointSize]byte
	for i, offset := range offsets {
		if err := whisper.fileReadAt(raw[:], offset); err != nil {
			return nil, err
		}
		stored[i] = unpackInt(raw[:])
	}
	return stored, nil
}

// updateCompressedOverlappingBatch uses the classic circular write/propagation
// order for an exceptional batch that overlaps slots across resolutions or
// between the compressed file and its sidecar. The sidecar has no sequence
// number to order different timestamps occupying the same circular slot. The
// scratch file is in memory; only the finished compressed replacement is
// published, under the caller's existing path lock. Ordinary writes keep the
// incremental compressed path.
func (whisper *Whisper) updateCompressedOverlappingBatch(points []*TimeSeriesPoint) error {
	if err := whisper.MergeOutOfOrder(); err != nil {
		return err
	}
	name := auxiliaryPath(whisper.file.Name(), ".batch")
	replay, err := CreateWithOptions(name, NewRetentionsNoPointer(whisper.Retentions()), whisper.aggregationMethod, whisper.xFilesFactor, &Options{InMemory: true, Sparse: true})
	if err != nil {
		return fmt.Errorf("create overlapping batch scratch: %w", err)
	}
	defer func() {
		_ = replay.Close()
		releaseMemFile(name)
	}()
	for i, archive := range whisper.archives {
		stored, err := whisper.fetchCompressed(1, int64(maxInt), archive)
		if err != nil {
			return err
		}
		// fetchCompressed has already aligned points and resolved circular-slot
		// aliases. Seed the fresh classic ring directly, retaining last-value
		// precedence for duplicate timestamps without another full-size copy.
		replay.seedReplayArchive(i, stored)
	}
	// Restore the caller order that UpdateMany's reverse/stable sort expects.
	input := append([]*TimeSeriesPoint(nil), points...)
	reversePoints(input)
	if err := replay.UpdateMany(input); err != nil {
		return err
	}
	extras := make([][]extraPoint, len(whisper.archives))
	for i := range extras {
		extras[i] = replay.replayArchivePoints(i)
	}
	rets := make([]*Retention, len(whisper.archives))
	for i, archive := range whisper.archives {
		ret := archive.Retention
		rets[i] = &ret
	}
	return whisper.rewrite(rets, "batch", func(i int) []extraPoint { return extras[i] })
}

// seedReplayArchive is restricted to a newly created in-memory classic file and
// points returned by fetchCompressed: intervals are aligned and different
// timestamps never own the same circular slot.
func (whisper *Whisper) seedReplayArchive(index int, points []dataPoint) {
	if len(points) == 0 {
		return
	}
	archive := whisper.archives[index]
	data := whisper.file.(*memFile).data
	base := points[0].interval
	for _, point := range points {
		offset := archive.PointOffset(base, point.interval)
		packInt(data[offset:], point.interval, 0)
		packFloat64(data[offset:], point.value, IntSize)
	}
}

type replayPointsByInterval []extraPoint

func (p replayPointsByInterval) Len() int           { return len(p) }
func (p replayPointsByInterval) Less(i, j int) bool { return p[i].interval < p[j].interval }
func (p replayPointsByInterval) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (whisper *Whisper) replayArchivePoints(index int) []extraPoint {
	archive := whisper.archives[index]
	data := whisper.file.(*memFile).data[archive.Offset():archive.End()]
	count := 0
	first := 0
	oldest := maxInt
	for offset := 0; offset < len(data); offset += PointSize {
		if interval := unpackInt(data[offset:]); interval > 0 {
			count++
			if interval < oldest {
				oldest = interval
				first = offset
			}
		}
	}
	points := make([]extraPoint, 0, count)
	ordered := true
	// A classic ring normally contains a rotated, ascending time window. Start
	// at its oldest occupied slot to read it in order without a full sort.
	// Sparse rings can retain older generations in unwritten slots, so verify
	// the order rather than assuming every snapshot fits one retention window.
	for _, part := range [][]byte{data[first:], data[:first]} {
		for offset := 0; offset < len(part); offset += PointSize {
			point := unpackDataPoint(part[offset:])
			if point.interval > 0 {
				if len(points) > 0 && points[len(points)-1].interval > point.interval {
					ordered = false
				}
				points = append(points, extraPoint{dataPoint: point, replace: true})
			}
		}
	}
	if !ordered {
		// Typed swaps avoid sort.Slice's reflection overhead for extraPoint.
		sort.Sort(replayPointsByInterval(points))
	}
	return points
}
