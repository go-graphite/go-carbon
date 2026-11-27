package v2core

import (
	"bufio"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Keep the final durability sync observable separately from SST encoding.
type coreMeasuredFile struct {
	vfs.File
	owner  *coreOwner
	buffer *bufio.Writer
}

type coreFileBuffer struct {
	writer *bufio.Writer
}

// One owner builds only one SST at a time. Reuse its output buffer across data,
// extension and frontier files, and drain it before the original durable Sync.
func (o *coreOwner) stageFile(f vfs.File) vfs.File {
	if o.fileBuffer.writer == nil {
		o.fileBuffer.writer = bufio.NewWriterSize(f, 1<<20)
	} else {
		o.fileBuffer.writer.Reset(f)
	}
	return &coreMeasuredFile{File: f, owner: o, buffer: o.fileBuffer.writer}
}
func (f *coreMeasuredFile) Write(p []byte) (int, error) {
	n, err := f.buffer.Write(p)
	if err == nil && f.owner.stage != nil {
		err = f.buffer.Flush()
		if err == nil {
			f.owner.stage("part-write")
		}
	}
	return n, err
}
func (f *coreMeasuredFile) Close() error {
	err := f.buffer.Flush()
	closeErr := f.File.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func (f *coreMeasuredFile) Sync() error {
	if err := f.buffer.Flush(); err != nil {
		return err
	}

	start := time.Now()
	err := f.File.Sync()
	n := uint64(time.Since(start))
	f.owner.syncNS.Add(n)
	f.owner.syncCalls.Add(1)
	for old := f.owner.syncMaxNS.Load(); n > old; old = f.owner.syncMaxNS.Load() {
		if f.owner.syncMaxNS.CompareAndSwap(old, n) {
			break
		}
	}
	return err
}
