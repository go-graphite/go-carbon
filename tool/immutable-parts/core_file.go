package main

import (
	"bufio"
	"bytes"
	"io"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Keep the final durability sync observable separately from SST encoding.
type coreMeasuredFile struct {
	vfs.File
	owner      *coreOwner
	buffer     *bufio.Writer
	whole      *bytes.Buffer
	prepared   bool
	prepareErr error
}

type coreFileBuffer struct {
	writer *bufio.Writer
	// Opt-in offline experiment: retain one encoded SST until its exact size
	// is known, preallocate it, then write and sync before ingestion. This adds
	// at most one encoded SST's retained capacity per owner to the memory budget.
	preallocate bool
	whole       bytes.Buffer
}

// One owner builds only one SST at a time. Reuse its output buffer across data,
// extension and frontier files, and drain it before the original durable Sync.
func (o *coreOwner) stageFile(f vfs.File) vfs.File {
	if o.fileBuffer.preallocate {
		o.fileBuffer.whole.Reset()
		return &coreMeasuredFile{File: f, owner: o, whole: &o.fileBuffer.whole}
	}
	if o.fileBuffer.writer == nil {
		o.fileBuffer.writer = bufio.NewWriterSize(f, 1<<20)
	} else {
		o.fileBuffer.writer.Reset(f)
	}
	return &coreMeasuredFile{File: f, owner: o, buffer: o.fileBuffer.writer}
}
func (f *coreMeasuredFile) Write(p []byte) (int, error) {
	if f.whole != nil {
		if f.prepared {
			return 0, io.ErrClosedPipe
		}
		return f.whole.Write(p)
	}
	return f.buffer.Write(p)
}
func (f *coreMeasuredFile) flush() error {
	if f.whole == nil {
		return f.buffer.Flush()
	}
	if f.prepared {
		return f.prepareErr
	}
	f.prepared = true
	data := f.whole.Bytes()
	start := time.Now()
	err := f.File.Preallocate(0, int64(len(data)))
	f.owner.preallocateNS.Add(uint64(time.Since(start)))
	if err == nil {
		start = time.Now()
		var n int
		n, err = f.File.Write(data)
		f.owner.fileWriteNS.Add(uint64(time.Since(start)))
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	f.prepareErr = err
	return err
}
func (f *coreMeasuredFile) Close() error {
	err := f.flush()
	closeErr := f.File.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func (f *coreMeasuredFile) Sync() error {
	if err := f.flush(); err != nil {
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
