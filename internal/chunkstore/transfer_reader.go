package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
)

// One batch reuses table readers instead of reopening every LSM level for
// every catalog, revision and continuation lookup. The caller owns its view.
type transferReader struct {
	reader                                                              pebble.Reader
	catalog, revisions, continuation, chunks, buckets, receipts, staged *pebble.Iterator
}

func (r *transferReader) close() {
	for _, slot := range []**pebble.Iterator{&r.catalog, &r.revisions, &r.continuation, &r.chunks, &r.buckets, &r.receipts, &r.staged} {
		if *slot != nil {
			_ = (*slot).Close()
			*slot = nil
		}
	}
}
func (r *transferReader) iterator(slot **pebble.Iterator, lower, upper []byte) (*pebble.Iterator, error) {
	if *slot == nil {
		it, err := r.reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, err
		}
		*slot = it
	}
	return *slot, nil
}
func (r *transferReader) lookup(slot **pebble.Iterator, lower, upper, key []byte) ([]byte, error) {
	it, err := r.iterator(slot, lower, upper)
	if err != nil {
		return nil, err
	}
	if !it.SeekGE(key) || !bytes.Equal(it.Key(), key) {
		if err := it.Error(); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	return it.Value(), nil
}
func (r *transferReader) metadata(name string) (Metadata, error) {
	value, err := r.lookup(&r.catalog, catalogLower, catalogUpper, catalogKey(name))
	if err != nil {
		return Metadata{}, err
	}
	m, err := decodeCatalog(name, value)
	if err != nil {
		return Metadata{}, err
	}
	return r.revision(m)
}
func (r *transferReader) revision(m Metadata) (Metadata, error) {
	value, err := r.lookup(&r.revisions, revisionLower, revisionUpper, revisionKey(m))
	if err != nil {
		return Metadata{}, err
	}
	if err := decodeRevision(value, &m); err != nil {
		return Metadata{}, err
	}
	return m, nil
}
func (r *transferReader) visit(ctx context.Context, slot **pebble.Iterator, lower, upper, prefix []byte, fn func([]byte, []byte) error) error {
	it, err := r.iterator(slot, lower, upper)
	if err != nil {
		return err
	}
	for valid := it.SeekGE(prefix); valid && bytes.HasPrefix(it.Key(), prefix); valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Error()
}
func (r *transferReader) deleteStaged(m Metadata, scope []byte) (bool, error) {
	value, err := r.lookup(&r.staged, []byte{'d'}, []byte{'e'}, transferDeleteKey(m))
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(value) == 40 && bytes.Equal(value[:32], scope) && binary.BigEndian.Uint64(value[32:]) == m.Revision, nil
}

// ListTransferNames pages the catalog without loading mutable revisions that
// a name inventory cannot use. Ownership checks still exclude original files.
func (s *Store) ListTransferNames(ctx context.Context, prefix, after string, limit int) ([]string, error) {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 10000 {
		return nil, errors.New("catalog page limit must be 1..10000")
	}
	lower, upper := catalogPageBounds(prefix, after)
	if upper == nil || bytes.Compare(lower, upper) >= 0 {
		return []string{}, nil
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	it, err := view.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	names := make([]string, 0, limit)
	for it.First(); it.Valid() && len(names) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := string(it.Key()[2:])
		m, err := decodeCatalog(name, it.Value())
		if err != nil {
			return nil, err
		}
		if err := s.checkFileOwnership(m); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, it.Error()
}
