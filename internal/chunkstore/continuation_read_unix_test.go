//go:build unix

package chunkstore

import (
	"bytes"
	"errors"
	"io"
	"os"

	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sys/unix"
)

var continuationPhysicalReadFS whisper.StorageFileSystem = continuationReadFiles{}

type continuationReadFiles struct{}

func (continuationReadFiles) OpenFile(name string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	file, err := os.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	var data []byte
	if stat.Size() > 0 {
		data, err = unix.Mmap(int(file.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return &continuationReadFile{File: file, data: data}, nil
}

func (continuationReadFiles) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }
func (continuationReadFiles) Remove(name string) error              { return os.Remove(name) }
func (continuationReadFiles) Rename(from, to string) error          { return os.Rename(from, to) }

// Only immutable comparison/transcoding handles use this mapping. Writers
// retain their ordinary OS files, and close before a comparison opens them.
type continuationReadFile struct {
	*os.File
	data    []byte
	pages   []int64
	indexed bool
}

func (f *continuationReadFile) ReadAt(data []byte, offset int64) (int, error) {
	if len(f.data) == 0 || offset < 0 || len(data) == 0 {
		return f.File.ReadAt(data, offset)
	}
	if offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(data, f.data[offset:])
	if n != len(data) {
		return n, io.EOF
	}
	return n, nil
}

func (f *continuationReadFile) Close() error {
	var err error
	if f.data != nil {
		err = unix.Munmap(f.data)
		f.data = nil
	}
	f.pages = nil
	return errors.Join(err, f.File.Close())
}

func (f *continuationReadFile) NextStorageExtent(start, end, archiveStart int64) (int64, int64, error) {
	if end > int64(len(f.data)) {
		return start, end, nil
	}
	if !f.indexed {
		for offset := 0; offset < len(f.data); offset += continuationMemoryPage {
			page := f.data[offset:min(offset+continuationMemoryPage, len(f.data))]
			if bytes.Count(page, []byte{0}) != len(page) {
				f.pages = append(f.pages, int64(offset/continuationMemoryPage))
			}
		}
		f.indexed = true
	}
	return continuationDataExtent(f.pages, start, end, archiveStart)
}
