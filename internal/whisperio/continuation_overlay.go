package whisperio

import (
	"bytes"
	"errors"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A short-lived copy-on-write view used by canonical snapshot export. The
// underlying database snapshot is immutable; ordinary Fetch never needs this.
type continuationOverlay struct {
	base     store.ContinuationIO
	values   map[string][]byte
	deleted  map[string]bool
	prefixes [][]byte
}

func newContinuationOverlay(base store.ContinuationIO) *continuationOverlay {
	return &continuationOverlay{base: base, values: make(map[string][]byte), deleted: make(map[string]bool)}
}

// TimeBuckets follows the underlying store, so blocks the overlay writes keep
// the layout an ordinary update would choose.
func (o *continuationOverlay) TimeBuckets() bool {
	b, ok := o.base.(store.ContinuationTimeBuckets)
	return ok && b.TimeBuckets()
}

func (o *continuationOverlay) hidden(key []byte) bool {
	if o.deleted[string(key)] {
		return true
	}
	for _, p := range o.prefixes {
		if bytes.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

func (o *continuationOverlay) Get(key []byte) ([]byte, error) {
	if v, ok := o.values[string(key)]; ok {
		return bytes.Clone(v), nil
	}
	if o.hidden(key) {
		return nil, store.ErrNotFound
	}
	return o.base.Get(key)
}

func (o *continuationOverlay) Set(key, value []byte) error {
	o.values[string(key)] = bytes.Clone(value)
	delete(o.deleted, string(key))
	return nil
}

func (o *continuationOverlay) Delete(key []byte) error {
	delete(o.values, string(key))
	o.deleted[string(key)] = true
	return nil
}

func (o *continuationOverlay) DeletePrefix(prefix []byte) error {
	for k := range o.values {
		if bytes.HasPrefix([]byte(k), prefix) {
			delete(o.values, k)
		}
	}
	o.prefixes = append(o.prefixes, bytes.Clone(prefix))
	return nil
}

func (o *continuationOverlay) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	values := make(map[string][]byte)
	err := o.base.Visit(prefix, func(k, v []byte) error {
		if !o.hidden(k) {
			values[string(k)] = bytes.Clone(v)
		}
		return nil
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	for k, v := range o.values {
		if bytes.HasPrefix([]byte(k), prefix) {
			values[k] = v
		}
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := visit([]byte(k), bytes.Clone(values[k])); err != nil {
			return err
		}
	}
	return nil
}

// commit applies the overlay's changes to its base: the deletions first, then
// every value it holds, in key order. The base then reads as the overlay did.
// Deletions the base holds nothing for cost a fresh stage nothing (see the
// store's pending-key deletes), so committing an import writes each final key
// once and no tombstone.
func (o *continuationOverlay) commit() error {
	for _, p := range o.prefixes {
		if err := o.base.DeletePrefix(p); err != nil {
			return err
		}
	}
	deleted := make([]string, 0, len(o.deleted))
	for k := range o.deleted {
		deleted = append(deleted, k)
	}
	sort.Strings(deleted)
	for _, k := range deleted {
		if err := o.base.Delete([]byte(k)); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(o.values))
	for k := range o.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := o.base.Set([]byte(k), o.values[k]); err != nil {
			return err
		}
	}
	return nil
}

// emptyContinuationIO holds nothing; it is the base of a scratch overlay.
type emptyContinuationIO struct{}

func (emptyContinuationIO) Get([]byte) ([]byte, error) { return nil, store.ErrNotFound }
func (emptyContinuationIO) Set([]byte, []byte) error {
	return errors.New("empty continuation store is read-only")
}
func (emptyContinuationIO) Delete([]byte) error       { return nil }
func (emptyContinuationIO) DeletePrefix([]byte) error { return nil }
func (emptyContinuationIO) Visit([]byte, func([]byte, []byte) error) error {
	return nil
}
