package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"
)

// digestReadBatch owns a single immutable view and its continuation iterator.
// Only DigestBatch's sequential loop can use the private context carrying it.
// In particular, it must never fill the live state cache from this older view.
type digestReadBatch struct {
	store *Store
	view  *pebble.ReadView
	walk  transferReader
	meta  Metadata
}
type digestReadBatchKey struct{}

func (s *Store) digestReadBatch(ctx context.Context) *digestReadBatch {
	b, _ := ctx.Value(digestReadBatchKey{}).(*digestReadBatch)
	if b != nil && b.store == s {
		return b
	}
	return nil
}

// DigestBatch reads names from one snapshot, sharing LSM table iterators across
// names. visit consumes one result before the next name is materialized. A
// missing name is reported with ErrNotFound; other errors stop the batch.
// The callback must not retain or use its context outside the callback.
func (s *Store) DigestBatch(ctx context.Context, names []string, visit func(context.Context, int) error) error {
	if len(names) == 0 || len(names) > MaxTransferMetrics {
		return ErrTransferLimit
	}
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	b := &digestReadBatch{store: s, view: s.db.NewReadView()}
	defer b.view.Close()
	b.walk.reader = b.view
	defer b.walk.close()
	order := make([]int, len(names))
	metas := make([]Metadata, len(names))
	found := make([]bool, len(names))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return names[order[i]] < names[order[j]] })
	for _, i := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, err := b.walk.lookup(&b.walk.catalog, catalogLower, catalogUpper, catalogKey(names[i]))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		m, err := decodeCatalog(names[i], value)
		if err != nil {
			return err
		}
		m, err = b.walk.revision(m)
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("read revision: %w", pebble.ErrNotFound)
		}
		if err != nil {
			return err
		}
		metas[i], found[i] = m, true
	}
	// Continuation keys are ordered by metric identity, rather than name.
	sort.Slice(order, func(i, j int) bool { return metas[order[i]].ID < metas[order[j]].ID })
	ctx = context.WithValue(ctx, digestReadBatchKey{}, b)
	for _, i := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.meta = metas[i]
		if !found[i] {
			b.meta = Metadata{MetricConfig: MetricConfig{Name: names[i]}}
		}
		if err := visit(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (b *digestReadBatch) metadata(name string) (Metadata, error) {
	if b.meta.Name != name {
		return Metadata{}, errors.New("digest batch context used for another name")
	}
	if b.meta.ID == 0 {
		return Metadata{}, ErrNotFound
	}
	return b.meta, nil
}
