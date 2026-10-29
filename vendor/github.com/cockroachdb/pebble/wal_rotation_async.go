// Copyright 2026 The Pebble Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pebble

import (
	"io"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

// pendingWAL keeps IO off the commit mutex during a handoff. LogWriter already
// buffers records and performs Write/Sync on its background flusher. Waiting
// here delays that flusher, not NoSync commits. Closing ready publishes file.
type pendingWAL struct {
	ready chan struct{}
	file  vfs.File
}

var _ io.WriteCloser = (*pendingWAL)(nil)

func (w *pendingWAL) Write(p []byte) (int, error) {
	<-w.ready
	return w.file.Write(p)
}

func (w *pendingWAL) Sync() error {
	<-w.ready
	return w.file.Sync()
}

func (w *pendingWAL) Close() error {
	<-w.ready
	return w.file.Close()
}

// DB.mu must be held. Only DB.mu is dropped, if commitPipeline.mu is also held.
func (d *DB) waitWALRotationLocked() {
	for done := d.mu.log.rotationDone; done != nil; done = d.mu.log.rotationDone {
		select {
		case <-done:
			return
		default:
		}
		d.mu.Unlock()
		<-done
		d.mu.Lock()
	}
}

// rotateWALAsync preserves the synchronous rotation's on-disk ordering:
// old EOF+fsync -> create/reuse next WAL -> directory fsync -> new writes.
// Only the in-memory installation happens earlier. A flush gate keeps the old
// WAL out of the recycler until the IO handoff finishes. Waiting before another
// rotation bounds handoffs and their WAL buffers to one, even on a stuck disk.
// Both DB.mu and commitPipeline.mu must be held by the caller.
func (d *DB) rotateWALAsync() (newLogNum FileNum, prevLogSize uint64) {
	d.waitWALRotationLocked()
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	newLogNum = d.mu.versions.getNextFileNum()
	previous := d.mu.log.LogWriter
	prevLogSize = uint64(previous.Size())
	last := &d.mu.log.queue[len(d.mu.log.queue)-1]
	if last.fileSize < prevLogSize {
		last.fileSize = prevLogSize
	}

	done := make(chan struct{})
	d.mu.log.rotationDone = done
	d.mu.mem.queue[len(d.mu.mem.queue)-1].walReady = done
	next := &pendingWAL{ready: make(chan struct{})}
	d.mu.log.queue = append(d.mu.log.queue, fileInfo{fileNum: newLogNum.DiskFileNum()})
	d.mu.versions.metrics.WAL.Files++
	d.mu.log.LogWriter = record.NewLogWriter(next, newLogNum, record.LogWriterConfig{
		WALFsyncLatency:    d.mu.log.metrics.fsyncLatency,
		WALMinSyncInterval: d.opts.WALMinSyncInterval,
		QueueSemChan:       d.commit.logSyncQSem,
	})
	if d.mu.log.registerLogWriterForTesting != nil {
		d.mu.log.registerLogWriterForTesting(d.mu.log.LogWriter)
	}
	// Capture fields while holding DB.mu. The task owns previous and next.
	preallocateSize := d.walPreallocateSize()
	go func() {
		if err := previous.Close(); err != nil {
			panic(err) // Same fail-stop policy as synchronous rotation.
		}
		metrics := previous.Metrics()
		file, size, err := d.createWALFile(newLogNum, jobID, preallocateSize)
		if err != nil {
			panic(err)
		}
		next.file = file
		close(next.ready)

		d.mu.Lock()
		if err := d.mu.log.metrics.Merge(metrics); err != nil {
			d.opts.Logger.Infof("metrics error: %s", err)
		}
		d.mu.log.queue[len(d.mu.log.queue)-1].fileSize = size
		close(done)
		// Close may have started while the IO was running. Its waiter still
		// needs done, but it must not start new background flushes.
		if d.closed.Load() == nil {
			d.maybeScheduleFlush()
		}
		d.mu.Unlock()
	}()
	return
}

// createWALFile is shared by synchronous and asynchronous rotation. The caller
// must have durably closed the previous log and serialize rotations. DB.mu must
// not be held. No newly created file receives writes until directory Sync ends.
func (d *DB) createWALFile(newLogNum FileNum, jobID, preallocateSize int) (vfs.File, uint64, error) {
	name := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, newLogNum.DiskFileNum())
	recycled, recycleOK := d.logRecycler.peek()
	var file vfs.File
	var err error
	if recycleOK {
		oldName := base.MakeFilepath(d.opts.FS, d.walDirname, fileTypeLog, recycled.fileNum)
		file, err = d.opts.FS.ReuseForWrite(oldName, name)
	} else {
		file, err = d.opts.FS.Create(name)
	}
	base.MustExist(d.opts.FS, name, d.opts.Logger, err)
	var size uint64
	if err == nil && recycleOK {
		info, statErr := file.Stat()
		err = statErr
		if err == nil {
			size = uint64(info.Size())
		}
	}
	if err == nil {
		err = d.walDir.Sync()
	}
	if recycleOK {
		err = firstError(err, d.logRecycler.pop(recycled.fileNum.FileNum()))
	}
	d.opts.EventListener.WALCreated(WALCreateInfo{
		JobID: jobID, Path: name, FileNum: newLogNum,
		RecycledFileNum: recycled.fileNum.FileNum(), Err: err,
	})
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, 0, err
	}
	return vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: d.opts.NoSyncOnClose, BytesPerSync: d.opts.WALBytesPerSync,
		PreallocateSize: preallocateSize,
	}), size, nil
}
