package v2core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"runtime"
	"sort"
	"syscall"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The pointer-free indexes are decoded once; record blocks remain mapped and
// compressed. One owner-local scratch block serves consecutive first touches.
// The existing metrics map is the only materialized mutable name state.
// File bytes never change, including while a replacement is renamed.
type mappedRollupCheckpoint struct {
	buf, index, names []byte
	table             [][]byte
	blockOffset       uint64
	block             []byte
}
type checkpointIndexEntry struct {
	id, offset, size     uint64
	nameOffset, nameSize uint64
}

func openMappedCheckpoint(db *pebble.DB, snapshot *pebble.Snapshot, path string, identities *PolicyCatalog) (*coreRollups, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errCheckpoint
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 28 || st.Size() > int64(math.MaxInt) {
		return nil, errCheckpoint
	}
	buf, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	r, err := loadCheckpoint(db, snapshot, buf, identities)
	if err != nil {
		return nil, errors.Join(err, syscall.Munmap(buf))
	}
	if r.checkpoint == nil {
		if err := syscall.Munmap(buf); err != nil {
			return nil, err
		}
	} else {
		runtime.AddCleanup(r.checkpoint, func(b []byte) { _ = syscall.Munmap(b) }, buf)
	}
	return r, nil
}

func (c *mappedRollupCheckpoint) record(i int) (uint64, []byte, error) {
	e := c.index[i*24 : (i+1)*24]
	id, off, n := binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:]), binary.LittleEndian.Uint64(e[16:])
	if off > uint64(len(c.buf))-8 {
		return 0, nil, errCheckpoint
	}
	if c.block == nil || c.blockOffset != off {
		length := binary.LittleEndian.Uint64(c.buf[off:])
		if length > uint64(len(c.buf))-off-8 {
			return 0, nil, errCheckpoint
		}
		block, err := rollupCheckpointUnzstd.DecodeAll(c.buf[off+8:off+8+length], c.block[:0])
		if err != nil {
			return 0, nil, errCheckpoint
		}
		c.block, c.blockOffset = block, off
	}
	start, length := n>>32, uint64(uint32(n))
	if start > uint64(len(c.block)) || length > uint64(len(c.block))-start {
		return 0, nil, errCheckpoint
	}
	return id, c.block[start : start+length], nil
}

func (r *coreRollups) loadCheckpointID(id uint64, series map[v2part.ID]*rollupScanEntry) error {
	c := r.checkpoint
	if c == nil || r.metrics[id] != nil {
		return nil
	}
	i := sort.Search(len(c.index)/24, func(i int) bool { return binary.LittleEndian.Uint64(c.index[i*24:]) >= id })
	if i == len(c.index)/24 || binary.LittleEndian.Uint64(c.index[i*24:]) != id {
		return nil
	}
	_, record, err := c.record(i)
	if err != nil {
		return err
	}
	d := checkpointDecoder{buf: record}
	if err = r.decodeCheckpointRecord(&d, c.table, series); err != nil {
		return err
	}
	runtime.KeepAlive(c)
	if len(d.buf) != 0 || r.metrics[id] == nil {
		return errCheckpoint
	}
	return nil
}

func (r *coreRollups) loadAllCheckpoint() error {
	c := r.checkpoint
	if c == nil {
		return nil
	}
	for i := 0; i < len(c.index)/24; i++ {
		id, _, err := c.record(i)
		if err != nil {
			return err
		}
		if err = r.loadCheckpointID(id, nil); err != nil {
			return err
		}
	}
	r.checkpoint = nil
	runtime.KeepAlive(c)
	return nil
}

// Cold names are searchable without constructing process-seeded fingerprints.
// This preserves duplicate-name rejection even when the old ID is untouched.
func (r *coreRollups) loadCheckpointName(name []byte) error {
	c := r.checkpoint
	if c == nil {
		return nil
	}
	var decodeErr error
	entry := func(i int) (uint64, []byte) {
		ordinal := binary.LittleEndian.Uint64(c.names[i*8:])
		if ordinal >= uint64(len(c.index)/24) {
			decodeErr = errCheckpoint
			return 0, nil
		}
		_, record, err := c.record(int(ordinal))
		if err != nil {
			decodeErr = err
			return 0, nil
		}
		d := checkpointDecoder{buf: record}
		id := d.uvarint()
		d.uvarint()
		n := d.bytes()
		if d.err != nil {
			decodeErr = d.err
		}
		return id, n
	}
	i := sort.Search(len(c.names)/8, func(i int) bool { _, n := entry(i); return bytes.Compare(n, name) >= 0 })
	if decodeErr != nil {
		return decodeErr
	}
	if i < len(c.names)/8 {
		id, n := entry(i)
		if bytes.Equal(n, name) {
			return r.loadCheckpointID(id, nil)
		}
	}
	return decodeErr
}
