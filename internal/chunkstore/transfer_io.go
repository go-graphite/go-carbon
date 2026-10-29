package chunkstore

import (
	"bufio"
	"errors"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

// Transfer staging has one sequential writer/reader per file. Coalesce its
// small SST block writes/reads without changing block size or read amplification
// of the ingested tables. These buffers never replace the live Pebble cache.
type transferWriteFile struct {
	vfs.File
	buffer *bufio.Writer
}

func (f *transferWriteFile) Write(p []byte) (int, error) { return f.buffer.Write(p) }
func (f *transferWriteFile) Sync() error {
	if err := f.buffer.Flush(); err != nil {
		return err
	}
	return f.File.Sync()
}
func (f *transferWriteFile) Close() error { return errors.Join(f.buffer.Flush(), f.File.Close()) }

type transferReadFile struct {
	vfs.File
	buffer []byte
	offset int64
	valid  int
	size   int64
}

func (f *transferReadFile) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || len(p) == 0 || len(p) > len(f.buffer) {
		return f.File.ReadAt(p, offset)
	}
	if offset >= f.offset && offset+int64(len(p)) <= f.offset+int64(f.valid) {
		return copy(p, f.buffer[offset-f.offset:]), nil
	}
	if offset >= f.size {
		return 0, io.EOF
	}
	f.offset = offset
	f.valid = 0
	n, err := f.File.ReadAt(f.buffer[:min(int64(len(f.buffer)), f.size-offset)], offset)
	f.valid = n
	copied := copy(p, f.buffer[:n])
	if copied == len(p) {
		return copied, nil
	}
	if err == nil {
		err = io.EOF
	}
	return copied, err
}
