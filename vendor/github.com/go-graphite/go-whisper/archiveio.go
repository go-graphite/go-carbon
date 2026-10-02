package whisper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// OfflineMergeOutOfOrderSnapshot makes a private copy of a compressed file and
// its sidecar, then folds the sidecar into that copy. It never mutates path.
// The caller must ensure the source is quiesced while it is copied.
// When no compressed sidecar exists, it returns path and a no-op cleanup.
func OfflineMergeOutOfOrderSnapshot(path string) (string, func(), error) {
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
	dir, err := os.MkdirTemp("", "go-whisper-ooo-snapshot-")
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
	_, copyErr := io.Copy(out, in)
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
	if index < 0 || index >= len(whisper.archives) {
		return nil, fmt.Errorf("archive index %d out of range", index)
	}
	if whisper.compressed {
		if whisper.aggregationMethod == Mix {
			return nil, fmt.Errorf("archive snapshots of Mix compressed whisper are not supported")
		}
		// Read only physically stored blocks and buffers. fetchCompressed adds
		// live aggregate tail values that are not archive-preserving.
		points, err := whisper.storedPoints(whisper.archives[index], 1, maxInt)
		if err != nil {
			return nil, fmt.Errorf("read compressed archive %d: %w", index, err)
		}
		byTime := make(map[int]float64, len(points))
		for _, point := range points {
			if point.interval > 0 {
				byTime[point.interval] = point.value
			}
		}
		if sidecar, err := whisper.oooSidecar(false); err != nil {
			return nil, err
		} else if sidecar != nil {
			sidePoints, err := sidecar.ArchivePoints(index)
			if err != nil {
				return nil, err
			}
			for _, point := range sidePoints {
				if _, ok := byTime[point.Time]; !ok {
					byTime[point.Time] = point.Value
				}
			}
		}
		result := make([]TimeSeriesPoint, 0, len(byTime))
		for timestamp, value := range byTime {
			result = append(result, TimeSeriesPoint{Time: timestamp, Value: value})
		}
		sort.Slice(result, func(i, j int) bool { return result[i].Time < result[j].Time })
		return result, nil
	}
	archive := whisper.archives[index]
	buf := make([]byte, archive.Size())
	if err := whisper.fileReadAt(buf, archive.Offset()); err != nil {
		return nil, fmt.Errorf("read archive %d: %w", index, err)
	}
	points := make([]TimeSeriesPoint, 0, archive.numberOfPoints)
	for offset := 0; offset < len(buf); offset += PointSize {
		point := unpackDataPoint(buf[offset : offset+PointSize])
		if point.interval > 0 {
			points = append(points, TimeSeriesPoint{Time: point.interval, Value: point.value})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
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
