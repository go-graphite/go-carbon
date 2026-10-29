package chunkstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// A private test transport, deliberately not a proposed storage representation.
// It proves the existing main/OOO/rename state machine can operate without any
// physical per-metric files before adding the exact-block database transport.
type continuationMemoryFiles struct {
	files map[string]*continuationMemoryData
	typed bool
}

const continuationMemoryPage = 4096

type continuationMemoryData struct {
	size  int64
	pages map[int64][]byte
}

func continuationMemoryImage(data []byte) *continuationMemoryData {
	image := &continuationMemoryData{size: int64(len(data)), pages: make(map[int64][]byte)}
	for offset := 0; offset < len(data); offset += continuationMemoryPage {
		page := data[offset:min(offset+continuationMemoryPage, len(data))]
		for _, b := range page {
			if b != 0 {
				image.pages[int64(offset/continuationMemoryPage)] = append(make([]byte, 0, continuationMemoryPage), page...)
				break
			}
		}
	}
	return image
}

type continuationMemoryFile struct {
	name     string
	data     *continuationMemoryData
	position int64
	readOnly bool
	closed   bool
}

func (f *continuationMemoryFiles) OpenFile(name string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	data, exists := f.files[name]
	if exists && flags&os.O_EXCL != 0 && flags&os.O_CREATE != 0 {
		return nil, os.ErrExist
	}
	if !exists {
		if flags&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		data = &continuationMemoryData{}
		f.files[name] = data
	}
	readOnly := flags&(os.O_WRONLY|os.O_RDWR) == 0
	if flags&os.O_TRUNC != 0 {
		if readOnly {
			return nil, os.ErrPermission
		}
		data.size, data.pages = 0, nil
	}
	file := &continuationMemoryFile{name: name, data: data, readOnly: readOnly}
	if f.typed {
		return &continuationTypedMemoryFile{continuationMemoryFile: file}, nil
	}
	return file, nil
}
func (f *continuationMemoryFiles) Stat(name string) (os.FileInfo, error) {
	if data, ok := f.files[name]; ok {
		return continuationMemoryInfo{name: filepath.Base(name), size: data.size}, nil
	}
	return nil, os.ErrNotExist
}
func (f *continuationMemoryFiles) Remove(name string) error {
	if _, ok := f.files[name]; !ok {
		return os.ErrNotExist
	}
	delete(f.files, name)
	return nil
}
func (f *continuationMemoryFiles) Rename(source, destination string) error {
	if data, ok := f.files[source]; ok {
		f.files[destination] = data
		delete(f.files, source)
		return nil
	}
	return os.ErrNotExist
}
func (f *continuationMemoryFile) Name() string { return f.name }
func (f *continuationMemoryFile) Fd() uintptr  { return ^uintptr(0) }
func (f *continuationMemoryFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return nil
}
func (f *continuationMemoryFile) ReadAt(data []byte, offset int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, errors.New("negative read")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if offset >= f.data.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(data)), f.data.size-offset))
	clear(data[:n])
	for copied := 0; copied < n; {
		page, within := (offset+int64(copied))/continuationMemoryPage, (offset+int64(copied))%continuationMemoryPage
		count := min(n-copied, continuationMemoryPage-int(within))
		if stored := f.data.pages[page]; int(within) < len(stored) {
			copy(data[copied:copied+count], stored[within:min(int(within)+count, len(stored))])
		}
		copied += count
	}
	if n != len(data) {
		return n, io.EOF
	}
	return n, nil
}
func (f *continuationMemoryFile) WriteAt(data []byte, offset int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.readOnly {
		return 0, os.ErrPermission
	}
	if offset < 0 || offset+int64(len(data)) < offset {
		return 0, errors.New("invalid write offset")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if f.data.pages == nil {
		f.data.pages = make(map[int64][]byte)
	}
	for copied := 0; copied < len(data); {
		page, within := (offset+int64(copied))/continuationMemoryPage, (offset+int64(copied))%continuationMemoryPage
		count := min(len(data)-copied, continuationMemoryPage-int(within))
		stored := f.data.pages[page]
		if len(stored) < continuationMemoryPage {
			stored = append(stored, make([]byte, continuationMemoryPage-len(stored))...)
			f.data.pages[page] = stored
		}
		copy(stored[within:int(within)+count], data[copied:copied+count])
		copied += count
	}
	f.data.size = max(f.data.size, offset+int64(len(data)))
	return len(data), nil
}
func (f *continuationMemoryFile) Read(data []byte) (int, error) {
	n, err := f.ReadAt(data, f.position)
	f.position += int64(n)
	return n, err
}
func (f *continuationMemoryFile) Write(data []byte) (int, error) {
	n, err := f.WriteAt(data, f.position)
	f.position += int64(n)
	return n, err
}
func (f *continuationMemoryFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	position := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		position += f.position
	case io.SeekEnd:
		position += f.data.size
	default:
		return 0, errors.New("invalid whence")
	}
	if position < 0 {
		return 0, errors.New("negative seek")
	}
	f.position = position
	return position, nil
}
func (f *continuationMemoryFile) Truncate(size int64) error {
	if f.closed {
		return os.ErrClosed
	}
	if f.readOnly {
		return os.ErrPermission
	}
	if size < 0 {
		return errors.New("negative size")
	}
	if size < f.data.size {
		for page, data := range f.data.pages {
			start := page * continuationMemoryPage
			if start >= size {
				delete(f.data.pages, page)
			} else if size-start < int64(len(data)) {
				clear(data[size-start:])
			}
		}
	}
	f.data.size = size
	return nil
}

type continuationMemoryInfo struct {
	name string
	size int64
}

func (i continuationMemoryInfo) Name() string       { return i.name }
func (i continuationMemoryInfo) Size() int64        { return i.size }
func (i continuationMemoryInfo) Mode() os.FileMode  { return 0600 }
func (i continuationMemoryInfo) ModTime() time.Time { return time.Time{} }
func (i continuationMemoryInfo) IsDir() bool        { return false }
func (i continuationMemoryInfo) Sys() interface{}   { return nil }

type continuationTypedMemoryFile struct {
	*continuationMemoryFile
	blockBytes []byte
}

func (f *continuationTypedMemoryFile) Close() error {
	f.blockBytes = nil
	return f.continuationMemoryFile.Close()
}

func (f *continuationTypedMemoryFile) ReadStorageBlock(block whisper.ContinuationBlock) ([]whisper.TimeSeriesPoint, error) {
	if cap(f.blockBytes) < block.Size {
		f.blockBytes = make([]byte, block.Size)
	}
	data := f.blockBytes[:block.Size]
	if _, err := f.ReadAt(data, int64(block.Offset)); err != nil {
		return nil, err
	}
	return whisper.DecodeContinuationBlock(block, data)
}

// Whole zero pages contain no classic/OOO records. Include complete records
// on both sides of page boundaries, including a record split across two pages.
func continuationDataExtent(pages []int64, start, end, archiveStart int64) (int64, int64, error) {
	from, until := end, end
	for _, page := range pages {
		first, last := page*continuationMemoryPage, (page+1)*continuationMemoryPage
		if last <= start || first >= end {
			continue
		}
		first = archiveStart + (max(first, archiveStart)-archiveStart)/whisper.PointSize*whisper.PointSize
		last = archiveStart + (last-archiveStart+whisper.PointSize-1)/whisper.PointSize*whisper.PointSize
		first, last = max(first, start), min(last, end)
		if first < from {
			from, until = first, last
		}
	}
	return from, until, nil
}

func (f *continuationTypedMemoryFile) NextStorageExtent(start, end, archiveStart int64) (int64, int64, error) {
	if f.closed {
		return 0, 0, os.ErrClosed
	}
	if end > f.data.size {
		return start, end, nil // The ordinary read must report a truncated file.
	}
	pages := make([]int64, 0, len(f.data.pages))
	for page := range f.data.pages {
		pages = append(pages, page)
	}
	return continuationDataExtent(pages, start, end, archiveStart)
}

func (f *continuationTypedMemoryFile) AppendStorageBlock(block whisper.ContinuationBlock, previous int, added []whisper.TimeSeriesPoint) error {
	var points []whisper.TimeSeriesPoint
	if previous > 0 {
		old := block
		old.First, old.Last, old.Count = 0, 0, previous
		var err error
		points, err = f.ReadStorageBlock(old)
		if err != nil {
			return err
		}
	}
	points = append(points, added...)
	for first := 0; first < len(points); first += chunkSlots {
		last := min(first+chunkSlots, len(points))
		var input, output chunk
		for i, p := range points[first:last] {
			input.set(i, Point{Timestamp: int64(p.Time), Value: p.Value})
		}
		if err := decodeChunk(encodeExactChunk(&input), &output); err != nil {
			return err
		}
		for i := range last - first {
			points[first+i] = whisper.TimeSeriesPoint{Time: int(output.Points[i].Timestamp), Value: output.Points[i].Value}
		}
	}
	data, err := whisper.EncodeContinuationBlock(block, points)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, int64(block.Offset))
	return err
}
