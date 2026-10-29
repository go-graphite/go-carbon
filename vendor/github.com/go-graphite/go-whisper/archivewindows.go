package whisper

import (
	"fmt"
	"math"
)

// ArchiveWindow identifies aligned slots in a specific archive. These helpers
// expose the existing Fetch view for a bounded rollback mirror; they do not
// change file encoding, aggregation or the ordinary writer/read path.
type ArchiveWindow struct{ Archive, From, Until int }

// RewriteRevision changes when this open writer replaces its main file. A
// mirror can detect implicit OOO folding inside UpdateMany without extra stat
// calls. It is local to this writer handle, not a persistent file version.
func (w *Whisper) RewriteRevision() uint64 { return w.rewriteRevision }

// MergeOutOfOrderWithArchiveWindows has the same merge policy as
// MergeOutOfOrderWithOptions and reports sidecar and buffered-rollup slots the
// merge may change. The caller also accounts for retired main-file blocks.
// Read the windows after a successful merge while retaining the writer lock.
// The ordinary file writer need not pay for this mirror bookkeeping.
func (w *Whisper) MergeOutOfOrderWithArchiveWindows(options OutOfOrderMergeOptions) (bool, []ArchiveWindow, error) {
	if options.MinPoints < 0 || options.MaxPointAge < 0 || options.RetentionMargin < 0 {
		return false, nil, fmt.Errorf("out-of-order merge limits must not be negative")
	}
	windows := w.BufferedRollupWindows()
	sidecar, err := w.oooSidecar(false)
	if err != nil {
		return false, nil, err
	}
	if sidecar != nil {
		for archive := range sidecar.archives {
			// Include expired records: compaction removes their old visible
			// values too. Scan populated extents without materializing the
			// sparse retention's empty slots.
			points, err := readArchivePointsInRange(sidecar, archive, 1, maxInt)
			if err != nil {
				return false, nil, err
			}
			for _, point := range points {
				for lower := archive; lower < len(w.archives); lower++ {
					step := w.archives[lower].secondsPerPoint
					from := point.interval - point.interval%step
					windows = append(windows, ArchiveWindow{lower, from, from + step})
				}
			}
		}
	}
	merged, err := w.MergeOutOfOrderWithOptions(options)
	if err != nil || !merged {
		return merged, nil, err
	}
	windows = append(windows, w.BufferedRollupWindows()...)
	return true, windows, nil
}

// StoredArchiveBounds reads the compressed main-file block/buffer bounds from
// the open writer's metadata. Retiring a physical block can remove old samples
// whose logical circular slots were not overwritten by a sparse new batch.
// Sidecars and virtual rollups are resolved by FetchArchiveWindow, not by these
// conservative main-file bounds. Classic fixed-slot files need no such repair.
func (w *Whisper) StoredArchiveBounds() []ArchiveWindow {
	if !w.compressed {
		return nil
	}
	result := make([]ArchiveWindow, 0, len(w.archives))
	for i, archive := range w.archives {
		first, last := 0, 0
		include := func(timestamp int) {
			if timestamp > 0 {
				if first == 0 || timestamp < first {
					first = timestamp
				}
				if timestamp > last {
					last = timestamp
				}
			}
		}
		for _, block := range archive.blockRanges {
			include(block.start)
			include(block.end)
		}
		if archive.cblock.count > 0 {
			include(archive.cblock.p0.interval)
			include(archive.cblock.pn1.interval)
			include(archive.cblock.pn2.interval)
		}
		for offset := 0; offset < len(archive.buffer); offset += PointSize {
			include(unpackInt(archive.buffer[offset:]))
		}
		if first > 0 {
			result = append(result, ArchiveWindow{Archive: i, From: first, Until: last + archive.secondsPerPoint})
		}
	}
	return result
}

// CanMirrorMonotonicBatch proves, before UpdateMany, that a single compressed
// archive will expose the aligned values in UpdateMany's final input order.
// The caller holds the writer lock and retains its normal clock/error fences.
// Rollups, boundaries and unusual values require a source read. An existing
// sidecar is safe only when none of the input's slots has an equal/newer owner.
// UpdateMany sorts the input in place before aligning duplicate slots. Repeated
// or reordered new timestamps are safe when they all follow the existing
// watermark; the caller must retain the post-write order for duplicate winners.
func (w *Whisper) CanMirrorMonotonicBatch(input []*TimeSeriesPoint, now int64) bool {
	if !w.compressed || len(w.archives) != 1 || w.aggregationMethod == Mix ||
		w.oooBroken || len(w.NonFatalErrors) != 0 || len(input) == 0 {
		return false
	}
	arc := w.archives[0]
	_, latest := arc.getRange()
	for _, interval := range [2]int{arc.cblock.pn1.interval, arc.cblock.pn2.interval} {
		if interval > latest {
			latest = interval
		}
	}
	for offset := 0; offset < len(arc.buffer); offset += PointSize {
		if interval := unpackInt(arc.buffer[offset:]); interval > latest {
			latest = interval
		}
	}
	for _, point := range input {
		if point == nil || point.Time <= 0 || int64(point.Time) > now ||
			now-int64(point.Time) >= int64(arc.MaxRetention()-arc.secondsPerPoint) ||
			math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			return false
		}
		interval := point.Time - point.Time%arc.secondsPerPoint
		if interval <= latest || interval <= 0 {
			return false
		}
	}
	if w.oooPath != "" {
		// A historical correction need not make every later append decode a
		// compressed block. Check the actual sparse slots while the caller
		// holds the writer lock. Bound the extra random IO for large batches.
		if len(input) > 256 {
			return false
		}
		sidecar, err := w.oooSidecar(false)
		if err != nil || sidecar == nil || w.oooBroken {
			return false
		}
		sideArchive := sidecar.archives[0]
		base, err := sidecar.readInt(sideArchive.Offset())
		if err != nil {
			return false
		}
		if base == 0 {
			return true
		}
		var raw [PointSize]byte
		for _, point := range input {
			interval := point.Time - point.Time%arc.secondsPerPoint
			if err := sidecar.fileReadAt(raw[:], sideArchive.PointOffset(base, interval)); err != nil {
				return false
			}
			// Include explicit NaNs and future circular aliases: a float-only
			// read cannot distinguish those corrections from empty slots.
			if unpackInt(raw[:]) >= interval {
				return false
			}
		}
	}
	return true
}

// BufferedRollupWindows returns every lower-archive window whose visible value
// can still be recomputed from a compressed buffer. Capture it before and after
// a write so flushing an old buffer cannot leave a stale virtual value behind.
func (w *Whisper) BufferedRollupWindows() []ArchiveWindow {
	var result []ArchiveWindow
	if !w.compressed {
		return nil
	}
	for i, arc := range w.archives {
		if !arc.hasBuffer() {
			continue
		}
		units, _, _ := arc.getBufferInfo()
		for _, timestamp := range units {
			if timestamp == 0 {
				continue
			}
			for j := i + 1; j < len(w.archives); j++ {
				step := w.archives[j].secondsPerPoint
				from := timestamp - timestamp%step
				result = append(result, ArchiveWindow{j, from, from + step})
			}
		}
	}
	return result
}

// FetchArchiveWindow reads exactly the requested archive slots, including OOO
// and buffered rollups. Unlike Fetch, it does not select an archive using Now.
// The caller must hold the ordinary Whisper writer lock for a coherent view.
func (w *Whisper) FetchArchiveWindow(window ArchiveWindow) (*TimeSeries, error) {
	if window.Archive < 0 || window.Archive >= len(w.archives) {
		return nil, fmt.Errorf("invalid archive index")
	}
	arc := w.archives[window.Archive]
	step := arc.secondsPerPoint
	if window.From < 0 || window.Until <= window.From || window.From%step != 0 || window.Until%step != 0 || window.Until-window.From > arc.MaxRetention() {
		return nil, fmt.Errorf("invalid aligned archive window")
	}
	return w.fetchFromArchive(arc, window.From-step, window.Until-step)
}
