package whisperio

import (
	"errors"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// One borrowed group belongs to one open file during immutable snapshot reads.
// Fragmented extent reads can revisit a group; retain its decode until the next
// group. No cache crosses an operation, a file, or an OOO mutation.
type classicSnapshotGroup struct {
	block          store.PointBlock
	archive, index int
	valid          bool
}

func (f *continuationFile) classicSnapshotGroup(archive int, a whisper.StorageArchiveLayout, index int) (*store.PointBlock, error) {
	if _, buffered := f.fs.kv.(*continuationPointBuffer); buffered || f.data.references {
		return f.classicGroup(archive, a, index, false)
	}
	if skipUnallocatedGroups && !f.classicGroupAllocated(a, index) {
		unallocatedGroupSkips.Add(1)
		return &emptyPointBlock, nil
	}
	if f.snapshotGroup == nil {
		f.snapshotGroup = new(classicSnapshotGroup)
	}
	g := f.snapshotGroup
	if g.valid && g.archive == archive && g.index == index {
		return &g.block, nil
	}
	g.valid = false
	key := continuationBodyKey(f.data.id, 'p', archive, 0, index)
	var value []byte
	var err error
	if f.sidecar {
		value, err = f.fs.probe(key)
	} else {
		value, err = f.fs.kv.Get(key)
	}
	if errors.Is(err, store.ErrNotFound) {
		g.block = store.PointBlock{}
	} else if err != nil {
		return nil, err
	} else if err = store.DecodePointBlock(value, &g.block); err != nil {
		return nil, err
	}
	g.archive, g.index, g.valid = archive, index, true
	return &g.block, nil
}
