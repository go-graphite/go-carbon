package whisper

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// OfflineMergeOutOfOrderSnapshot makes a private copy of a compressed file and
// its sidecar, then folds the sidecar into that copy. It never mutates path.
// The caller must ensure the source is quiesced while it is copied.
// When no compressed sidecar exists, it returns path and a no-op cleanup.
func OfflineMergeOutOfOrderSnapshot(path string) (string, func(), error) {
	return OfflineMergeOutOfOrderSnapshotIn(path, "")
}

// OfflineMergeOutOfOrderSnapshotIn chooses the parent of private snapshot
// directories. A writable directory on the source filesystem permits reflinks
// and avoids expanding sparse OOO sidecars on a separate temporary filesystem.
func OfflineMergeOutOfOrderSnapshotIn(path, directory string) (string, func(), error) {
	readOnly := os.O_RDONLY
	w, err := OpenWithOptions(path, &Options{OpenFileFlag: &readOnly})
	if err != nil {
		return "", nil, fmt.Errorf("open snapshot source: %w", err)
	}
	compressed, sidecar := w.compressed, w.oooPath
	broken := w.oooBroken
	if err := w.Close(); err != nil {
		return "", nil, fmt.Errorf("close snapshot source: %w", err)
	}
	if broken {
		return "", nil, fmt.Errorf("snapshot source has incompatible out-of-order sidecar: %w", errOOOIncompatible)
	}
	if !compressed || sidecar == "" {
		return path, func() {}, nil
	}
	dir, err := os.MkdirTemp(directory, "go-whisper-ooo-snapshot-")
	if err != nil {
		return "", nil, fmt.Errorf("create snapshot directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	copyPath := filepath.Join(dir, filepath.Base(path))
	if err := copyFile(path, copyPath); err != nil {
		cleanup()
		return "", nil, err
	}
	if err := copyFile(sidecar, OutOfOrderSidecarPath(copyPath)); err != nil {
		cleanup()
		return "", nil, err
	}
	copyWhisper, err := Open(copyPath)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("open copied snapshot: %w", err)
	}
	// Unix epoch includes all positive historical sidecar points without changing
	// the package-global Now hook. This is an offline, source-quiesced snapshot.
	_, err = copyWhisper.mergeOutOfOrderAt(nil, time.Unix(0, 0))
	closeErr := copyWhisper.Close()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("merge copied out-of-order sidecar: %w", err)
	}
	if closeErr != nil {
		cleanup()
		return "", nil, fmt.Errorf("close copied snapshot: %w", closeErr)
	}
	return copyPath, cleanup, nil
}

func copyFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open snapshot input %s: %w", source, err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("create snapshot output %s: %w", destination, err)
	}
	copyErr := copySnapshotContents(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("copy snapshot %s: %w", source, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close snapshot output %s: %w", destination, closeErr)
	}
	return nil
}

// ArchivePoints returns all non-empty slots from a classic archive, ordered by
// timestamp. Unlike Fetch, it does not consult Now: it is intended for
// archive-preserving migration of historical files.
func (whisper *Whisper) ArchivePoints(index int) ([]TimeSeriesPoint, error) {
	return whisper.archivePoints(index, false)
}

// ArchivePointsWithRollups includes the values computed from compressed archive
// buffers during Fetch. It is intended for migration into an engine that stores
// these rollups eagerly. The caller must quiesce the source and use
// OfflineMergeOutOfOrderSnapshot first, so sidecar corrections are folded into
// the private copy before rollups are evaluated.
func (whisper *Whisper) ArchivePointsWithRollups(index int) ([]TimeSeriesPoint, error) {
	return whisper.archivePoints(index, true)
}

func (whisper *Whisper) archivePoints(index int, includeRollups bool) ([]TimeSeriesPoint, error) {
	if index < 0 || index >= len(whisper.archives) {
		return nil, fmt.Errorf("archive index %d out of range", index)
	}
	if whisper.compressed {
		if whisper.aggregationMethod == Mix {
			return nil, fmt.Errorf("archive snapshots of Mix compressed whisper are not supported")
		}
		// Read only physically stored blocks and buffers. fetchCompressed adds
		// live aggregate tail values that are not archive-preserving.
		var points []dataPoint
		var err error
		if includeRollups {
			points, err = whisper.fetchCompressed(1, int64(maxInt), whisper.archives[index])
		} else {
			points, err = whisper.storedPoints(whisper.archives[index], 1, maxInt)
		}
		if err != nil {
			return nil, fmt.Errorf("read compressed archive %d: %w", index, err)
		}
		var side []TimeSeriesPoint
		if sidecar, err := whisper.oooSidecar(false); err != nil {
			return nil, err
		} else if sidecar != nil {
			if side, err = sidecar.ArchivePoints(index); err != nil {
				return nil, err
			}
		}
		result := mergeOrderedArchivePoints(points, side)
		return result, nil
	}
	// Reuse the bounded, extent-aware classic reader. Virtual sidecars can
	// contain only a few points even with years of logical retention.
	stored, err := readArchivePointsInRange(whisper, index, 0, maxInt)
	if err != nil {
		return nil, err
	}
	points := make([]TimeSeriesPoint, len(stored))
	for i, point := range stored {
		points[i] = TimeSeriesPoint{Time: point.interval, Value: point.value}
	}
	return points, nil
}

// ReplaceArchivePoints writes aligned archive slots directly, without retention
// admission or propagation. It is for consistent import/export only.
func (whisper *Whisper) ReplaceArchivePoints(index int, points []TimeSeriesPoint) error {
	if whisper.compressed {
		return fmt.Errorf("archive snapshots of compressed whisper are not supported")
	}
	if index < 0 || index >= len(whisper.archives) {
		return fmt.Errorf("archive index %d out of range", index)
	}
	archive := whisper.archives[index]
	if err := whisper.fileWriteAt(make([]byte, archive.Size()), archive.Offset()); err != nil {
		return fmt.Errorf("clear archive %d: %w", index, err)
	}
	if len(points) == 0 {
		return nil
	}
	pointPointers := make([]*TimeSeriesPoint, len(points))
	for i := range points {
		pointPointers[i] = &points[i]
	}
	return whisper.archiveUpdateManyDataPoints(archive, alignPoints(archive, pointPointers), false)
}

// mergeOrderedArchivePoints returns stored compressed points and sidecar
// points as one series ordered by timestamp. It resolves duplicates exactly as
// a timestamp map would: among stored points the last one read wins, stored
// points win over sidecar points, and among sidecar points the first one wins.
// Stored points with a non-positive interval are empty slots and are skipped.
// Blocks are normally read in time order, so the sorts only run on input that
// is out of order; a stable sort keeps the read order of equal timestamps.
func mergeOrderedArchivePoints(stored []dataPoint, side []TimeSeriesPoint) []TimeSeriesPoint {
	byInterval := func(a, b dataPoint) int { return cmp.Compare(a.interval, b.interval) }
	if !slices.IsSortedFunc(stored, byInterval) {
		stored = slices.Clone(stored)
		slices.SortStableFunc(stored, byInterval)
	}
	byTime := func(a, b TimeSeriesPoint) int { return cmp.Compare(a.Time, b.Time) }
	if !slices.IsSortedFunc(side, byTime) {
		side = slices.Clone(side)
		slices.SortStableFunc(side, byTime)
	}
	result := make([]TimeSeriesPoint, 0, len(stored)+len(side))
	j := 0
	for i := 0; i < len(stored); i++ {
		point := stored[i]
		if point.interval <= 0 {
			continue
		}
		// The last of equal stored timestamps wins.
		for i+1 < len(stored) && stored[i+1].interval == point.interval {
			i++
			point = stored[i]
		}
		for ; j < len(side) && side[j].Time < point.interval; j++ {
			if n := len(result); n == 0 || result[n-1].Time != side[j].Time {
				result = append(result, side[j])
			}
		}
		// A stored point hides sidecar points with its timestamp.
		for ; j < len(side) && side[j].Time == point.interval; j++ {
		}
		result = append(result, TimeSeriesPoint{Time: point.interval, Value: point.value})
	}
	for ; j < len(side); j++ {
		if n := len(result); n == 0 || result[n-1].Time != side[j].Time {
			result = append(result, side[j])
		}
	}
	return result
}
