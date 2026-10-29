package whisper

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"path/filepath"
	"sort"
	"testing"
)

type snapshotCountingFile struct {
	file
	bytes, maxRead int
	fail           bool
}

func (f *snapshotCountingFile) ReadAt(b []byte, off int64) (int, error) {
	if f.fail {
		return 0, io.ErrUnexpectedEOF
	}
	f.bytes += len(b)
	if len(b) > f.maxRead {
		f.maxRead = len(b)
	}
	return f.file.ReadAt(b, off)
}

type snapshotExtentFile struct {
	*snapshotCountingFile
	offsets []int64
}

func (f *snapshotExtentFile) NextStorageExtent(start, end, archiveStart int64) (int64, int64, error) {
	if f.fail {
		return 0, 0, io.ErrUnexpectedEOF
	}
	for _, off := range f.offsets {
		if off+PointSize > start && off < end {
			from, until := off, off+PointSize
			if from < start {
				from = start
			}
			if until > end {
				until = end
			}
			return from, until, nil
		}
	}
	return end, end, nil
}
func fullClassicSnapshot(w *Whisper, index int) ([]TimeSeriesPoint, error) {
	archive := w.archives[index]
	data := make([]byte, archive.Size())
	if err := w.fileReadAt(data, archive.Offset()); err != nil {
		return nil, err
	}
	result := make([]TimeSeriesPoint, 0, archive.numberOfPoints)
	for off := 0; off < len(data); off += PointSize {
		point := unpackDataPoint(data[off : off+PointSize])
		if point.interval > 0 {
			result = append(result, TimeSeriesPoint{Time: point.interval, Value: point.value})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Time < result[j].Time })
	return result, nil
}
func TestArchiveSnapshotClassicMatchesFullReader(t *testing.T) {
	for _, kind := range []string{"empty", "sparse", "dense", "extents"} {
		t.Run(kind, func(t *testing.T) {
			retentions, err := ParseRetentionDefs("1s:8192s")
			if err != nil {
				t.Fatal(err)
			}
			w, err := CreateWithOptions(filepath.Join(t.TempDir(), "archive.wsp"), retentions, Last, 0, &Options{Sparse: true})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			archive := w.archives[0]
			slots := []int{0, 340, 341, 4095, 8191}
			if kind == "empty" {
				slots = nil
			}
			if kind == "dense" {
				slots = make([]int, 8192)
				for i := range slots {
					slots[i] = i
				}
			}
			var offsets []int64
			for i, slot := range slots {
				timestamp := 1_700_000_000 + slot
				if i == 2 {
					timestamp = 1_700_000_000
				} // Keep duplicate timestamps and exact float bits.
				bits := uint64(0x3ff0000000000000) + uint64(i)
				if i == 0 {
					bits = 1 << 63
				}
				if i == 3 {
					bits = 0x7ff80000000000ab
				}
				data := make([]byte, PointSize)
				binary.BigEndian.PutUint32(data[:4], uint32(timestamp))
				binary.BigEndian.PutUint64(data[4:], bits)
				offset := archive.Offset() + int64(slot*PointSize)
				if err := w.fileWriteAt(data, offset); err != nil {
					t.Fatal(err)
				}
				offsets = append(offsets, offset)
			}
			want, err := fullClassicSnapshot(w, 0)
			if err != nil {
				t.Fatal(err)
			}
			counter := &snapshotCountingFile{file: w.file}
			w.file = counter
			if kind == "extents" {
				w.file = &snapshotExtentFile{snapshotCountingFile: counter, offsets: offsets}
			}
			for _, visible := range []bool{false, true} {
				var got []TimeSeriesPoint
				if visible {
					got, err = w.ArchivePointsWithRollups(0)
				} else {
					got, err = w.ArchivePoints(0)
				}
				if err != nil || len(got) != len(want) || (got == nil) != (want == nil) {
					t.Fatal(got, want, err)
				}
				for i := range got {
					if got[i].Time != want[i].Time || math.Float64bits(got[i].Value) != math.Float64bits(want[i].Value) {
						t.Fatal("snapshot changed", i, got[i], want[i])
					}
				}
			}
			if counter.maxRead > 4096*PointSize {
				t.Fatal("unbounded archive buffer", counter.maxRead)
			}
			if kind == "extents" && counter.bytes != 2*len(slots)*PointSize {
				t.Fatal("read holes", counter.bytes)
			}
			counter.fail = true
			if _, err := w.ArchivePoints(0); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("lost read error", err)
			}
		})
	}
}
