package whisper

import (
	"errors"
	"fmt"
)

// ErrArchiveAppendOrder is returned by AppendArchivePoints when a point is
// not strictly newer than the archive's blocks or the previous point.
var ErrArchiveAppendOrder = errors.New("archive append: point is not newer than the archive's blocks")

// The three functions below let an importer merge two copies of one series
// without decoding and re-encoding the older copy. A copy whose donor carries
// every archive is already authoritative at every level, so its points are
// appended to each archive as they are, with no rollup propagation:
//
//  1. ArchiveBlockWatermark on the older file (the base) and the newer one.
//  2. AppendArchivePoints with the newer copy's block points per archive.
//  3. ReplaceBuffers from the newer copy, so its unfinished windows keep
//     producing the same pending rollups and later writes propagate as they
//     would on the newer copy.
//
// Only the base's newest block of each archive is re-encoded; older blocks
// are untouched. The caller compares the merged archives with its expected
// union and falls back to a full rewrite when they differ.

// ArchiveBlockWatermark returns the newest interval encoded in the blocks of
// a compressed archive, or 0 when its blocks are empty. Buffered points and
// rollups evaluated from buffers are not included.
func (whisper *Whisper) ArchiveBlockWatermark(index int) (int, error) {
	archive, err := whisper.appendableArchive(index)
	if err != nil {
		return 0, err
	}
	return archive.cblock.pn1.interval, nil
}

// AppendArchivePoints appends points to the blocks of one compressed archive.
// Points are written as they are: they are not propagated into coarser
// archives and do not pass through the archive's buffer. Every point must be
// on the archive's grid, and the points must be strictly increasing and newer
// than ArchiveBlockWatermark; otherwise nothing is written. Blocks that still
// hold retained points grow instead of being reused, as for UpdateMany. The
// header, including block ranges, is written before it returns.
//
// The out-of-order sidecar is not consulted. A stored point hides a sidecar
// point with the same timestamp on reads, so fold the sidecar first
// (MergeOutOfOrder) when its points must survive.
func (whisper *Whisper) AppendArchivePoints(index int, points []TimeSeriesPoint) error {
	archive, err := whisper.appendableArchive(index)
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}
	watermark := archive.cblock.pn1.interval
	dps := make([]dataPoint, len(points))
	for i, p := range points {
		if p.Time <= 0 || p.Time%archive.secondsPerPoint != 0 {
			return fmt.Errorf("archive append %d: timestamp %d is not on the %ds grid", index, p.Time, archive.secondsPerPoint)
		}
		if p.Time <= watermark {
			return fmt.Errorf("%w: archive %d timestamp %d, newest %d", ErrArchiveAppendOrder, index, p.Time, watermark)
		}
		watermark = p.Time
		dps[i] = dataPoint{interval: p.Time, value: p.Value}
	}
	if _, err := archive.appendToBlockAndRotate(dps); err != nil {
		return err
	}
	return whisper.WriteHeaderCompressed()
}

// ReplaceBuffers copies every archive's propagation buffer from src and
// writes the header. src must be compressed with the same archives,
// aggregation method and xFilesFactor.
func (whisper *Whisper) ReplaceBuffers(src *Whisper) error {
	if !whisper.compressed || src == nil || !src.compressed {
		return fmt.Errorf("replace buffers: both files must be compressed")
	}
	if whisper.aggregationMethod == Mix {
		return fmt.Errorf("replace buffers: Mix aggregation is not supported")
	}
	if whisper.aggregationMethod != src.aggregationMethod || whisper.xFilesFactor != src.xFilesFactor || len(whisper.archives) != len(src.archives) {
		return fmt.Errorf("replace buffers: different aggregation or archive count")
	}
	for i, archive := range whisper.archives {
		other := src.archives[i]
		if archive.secondsPerPoint != other.secondsPerPoint || archive.numberOfPoints != other.numberOfPoints || len(archive.buffer) != len(other.buffer) {
			return fmt.Errorf("replace buffers: archive %d layout differs", i)
		}
	}
	for i, archive := range whisper.archives {
		copy(archive.buffer, src.archives[i].buffer)
	}
	return whisper.WriteHeaderCompressed()
}

func (whisper *Whisper) appendableArchive(index int) (*archiveInfo, error) {
	if !whisper.compressed {
		return nil, fmt.Errorf("archive append: file is not compressed")
	}
	if whisper.aggregationMethod == Mix {
		return nil, fmt.Errorf("archive append: Mix aggregation is not supported")
	}
	if index < 0 || index >= len(whisper.archives) {
		return nil, fmt.Errorf("archive index %d out of range", index)
	}
	return whisper.archives[index], nil
}
