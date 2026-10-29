package whisper

import (
	"errors"
	"io"
	"syscall"
)

// Linux SEEK_DATA/SEEK_HOLE values. Only the Linux constructor uses a real
// file; other platforms retain the ordinary reader.
const seekArchiveData, seekArchiveHole = 3, 4

type archiveExtentReader struct {
	nextExtent func(int64, int64, int64) (int64, int64, error)
	seek       func(int64, int) (int64, error)
	size       func() (int64, error)
	dataFrom   int64
	dataTo     int64
	disabled   bool
}

// next returns complete records overlapping the next data extent. A record
// can straddle a filesystem-block boundary, so extent offsets must be rounded
// relative to this archive, rather than the beginning of the file.
func (r *archiveExtentReader) next(start, end, archiveStart int64) (from, until int64, err error) {
	from, until = start, end
	if r == nil || r.disabled {
		return
	}
	if r.nextExtent != nil {
		return r.nextExtent(start, end, archiveStart)
	}
	if start >= r.dataFrom && start < r.dataTo {
		if until > r.dataTo {
			until = r.dataTo
		}
		return
	}
	position, e := r.seek(0, io.SeekCurrent)
	if e != nil {
		r.disabled = true
		return
	}
	defer func() {
		if _, e := r.seek(position, io.SeekStart); e != nil {
			err = e
		}
	}()
	data, e := r.seek(start, seekArchiveData)
	if errors.Is(e, syscall.ENXIO) {
		// A concurrent truncation must still reach the ordinary ReadAt EOF
		// check rather than looking like an empty tail of a sparse archive.
		size, statErr := r.size()
		if statErr != nil || size < end {
			r.disabled = true
			return
		}
		return end, end, nil
	}
	if e != nil {
		r.disabled = true
		return
	}
	if data < start {
		r.disabled = true
		return
	}
	if data >= end {
		return end, end, nil
	}
	hole, e := r.seek(data, seekArchiveHole)
	if e != nil || hole <= data {
		r.disabled = true
		return
	}
	from = archiveStart + (data-archiveStart)/PointSize*PointSize
	until = archiveStart + (hole-archiveStart+PointSize-1)/PointSize*PointSize
	if from < start {
		from = start
	}
	if until > end {
		until = end
	}
	r.dataFrom, r.dataTo = from, until
	return
}
