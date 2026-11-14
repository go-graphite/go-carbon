package chunkstore

import (
	"bytes"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

// transferView pins one read state and sequence number for a single preparation.
// All cursors are clones of this unpositioned iterator: independently opening DB
// iterators would allow catalog, revision and archive reads to disagree. Unlike
// a registered Snapshot, closing the view does not unconditionally acquire
// Pebble's DB mutex or run its snapshot-elision compaction picker.
// Do not pool these across preparations: epochs must precede the view's sequence
// number, and retaining an iterator pins its memtables and table version.
type transferView struct{ root *pebble.Iterator }

func (s *Store) newTransferView() (*transferView, error) {
	it, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	return &transferView{root: it}, nil
}

func (v *transferView) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return v.root.Clone(pebble.CloneOptions{IterOptions: opts})
}

func (v *transferView) Get(key []byte) ([]byte, io.Closer, error) {
	it, err := v.NewIter(nil)
	if err != nil {
		return nil, nil, err
	}
	if it.SeekGE(key) && bytes.Equal(it.Key(), key) {
		return it.Value(), it, nil
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return nil, nil, err
	}
	return nil, nil, pebble.ErrNotFound
}

func (v *transferView) Close() error { return v.root.Close() }
