// Copyright 2026 The go-carbon Authors. This file is a go-carbon patch to the
// vendored Pebble and is not part of upstream Pebble.

package pebble

import (
	"context"
	"io"
)

// ReadView is a consistent read-only view of the DB, like a Snapshot, for a
// short series of reads. It holds the read state (memtables and version) and
// the visible sequence number current when it was opened, as an Iterator
// does. Unlike a Snapshot it is not registered with the DB, so opening does
// not take DB.mu. Closing the last reference to a retired read state can
// acquire DB.mu to release its version or schedule obsolete-file cleanup.
// Like an Iterator it retains memtables and sstables until it is closed.
//
// Get and NewIter read at the view; Close must be called once its reads and
// their iterators and value closers are done.
type ReadView struct {
	db        *DB
	readState *readState
	seqNum    uint64
}

var _ Reader = (*ReadView)(nil)

// NewReadView opens a ReadView of the DB's current state.
func (d *DB) NewReadView() *ReadView {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	// As in getInternal and newIter, the sequence number is read after the
	// read state.
	readState := d.loadReadState()
	return &ReadView{db: d, readState: readState, seqNum: d.mu.versions.visibleSeqNum.Load()}
}

// Get is DB.Get at the view. The value is valid until the closer is closed.
func (v *ReadView) Get(key []byte) ([]byte, io.Closer, error) {
	d := v.db
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	readState := v.readState
	readState.ref()
	seqNum := v.seqNum

	buf := getIterAllocPool.Get().(*getIterAlloc)

	get := &buf.get
	*get = getIter{
		logger:   d.opts.Logger,
		comparer: d.opts.Comparer,
		newIters: d.newIters,
		snapshot: seqNum,
		key:      key,
		mem:      readState.memtables,
		l0:       readState.current.L0SublevelFiles,
		version:  readState.current,
	}

	// Strip off memtables which cannot possibly contain the seqNum being read
	// at.
	for len(get.mem) > 0 {
		n := len(get.mem)
		if logSeqNum := get.mem[n-1].logSeqNum; logSeqNum < seqNum {
			break
		}
		get.mem = get.mem[:n-1]
	}

	i := &buf.dbi
	pointIter := get
	*i = Iterator{
		ctx:          context.Background(),
		getIterAlloc: buf,
		iter:         pointIter,
		pointIter:    pointIter,
		merge:        d.merge,
		comparer:     *d.opts.Comparer,
		readState:    readState,
		keyBuf:       buf.keyBuf,
	}

	if !i.First() {
		err := i.Close()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, ErrNotFound
	}
	return i.Value(), i, nil
}

// NewIter returns an iterator over the view.
func (v *ReadView) NewIter(o *IterOptions) (*Iterator, error) {
	return v.db.newIter(context.Background(), nil /* batch */, snapshotIterOpts{seqNum: v.seqNum, readState: v.readState}, o), nil
}

// Close releases the view's read state.
func (v *ReadView) Close() error {
	if v.readState == nil {
		panic(ErrClosed)
	}
	v.readState.unref()
	v.readState = nil
	return nil
}
