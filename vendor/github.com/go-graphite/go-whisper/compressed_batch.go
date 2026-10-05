package whisper

import (
	"fmt"
	"sort"
)

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
			if index > 0 && interval != 0 && interval <= archive.cblock.pn1.interval {
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
			sa := sidecar.archives[index]
			base := sidecar.getBaseInterval(sa)
			if base != 0 {
				var raw [PointSize]byte
				for _, interval := range intervals {
					if err := sidecar.fileReadAt(raw[:], sa.PointOffset(base, interval)); err != nil {
						return false, err
					}
					previous := unpackInt(raw[:])
					if previous != 0 && previous != interval {
						// With no coarse archives, an expired sidecar alias has
						// no remaining observable value to materialize. Multi-
						// archive files must preserve its retained rollups first.
						if len(whisper.archives) == 1 && previous < now-archive.MaxRetention() && previous < interval {
							continue
						}
						return true, nil
					}
				}
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

func (whisper *Whisper) replayArchivePoints(index int) []extraPoint {
	archive := whisper.archives[index]
	data := whisper.file.(*memFile).data[archive.Offset():archive.End()]
	count := 0
	for offset := 0; offset < len(data); offset += PointSize {
		if unpackInt(data[offset:]) > 0 {
			count++
		}
	}
	points := make([]extraPoint, 0, count)
	for offset := 0; offset < len(data); offset += PointSize {
		point := unpackDataPoint(data[offset:])
		if point.interval > 0 {
			points = append(points, extraPoint{dataPoint: point, replace: true})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].interval < points[j].interval })
	return points
}
