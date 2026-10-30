package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
)

// DeleteBatchIfUnchanged commits at most 512 expiration candidates together.
// Results correspond to input order; a conflict leaves that metric untouched.
func (s *Store) DeleteBatchIfUnchanged(ctx context.Context, expected []Metadata) ([]error, error) {
	if len(expected) > 512 {
		return nil, errors.New("delete batch exceeds 512 metrics")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	stripes := make([]uint64, 0, len(expected))
	names := make(map[string]bool, len(expected))
	for _, m := range expected {
		if names[m.Name] {
			return nil, errors.New("duplicate delete candidate")
		}
		names[m.Name] = true
		stripes = append(stripes, s.metricStripe(m.Name))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, stripe := range stripes {
		s.metricMu[stripe].Lock()
	}
	for _, m := range expected {
		s.beginStateMutation(m.Name)
	}
	defer func() {
		for _, m := range expected {
			s.finishStateMutation(m.Name)
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}()
	results := make([]error, len(expected))
	b := s.db.NewBatch()
	defer b.Close()
	removed := make([]Metadata, 0, len(expected))
	view := s.db.NewReadView()
	defer view.Close()
	reader := transferReader{reader: view}
	defer reader.close()
	for i, want := range expected {
		m, err := reader.metadata(want.Name)
		if errors.Is(err, ErrNotFound) {
			results[i] = err
			continue
		}
		if err != nil {
			return nil, err
		}
		if m.ID != want.ID || m.Generation != want.Generation || m.Revision != want.Revision || m.FileMigration == FileCleanupPending {
			results[i] = ErrConflict
			continue
		}
		if err := s.appendExpirationDelete(ctx, b, &reader, m); err != nil {
			return nil, err
		}
		removed = append(removed, m)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(removed) == 0 {
		return results, nil
	}
	records := make([]SidecarRecord, len(removed))
	for i, m := range removed {
		records[i].Name = m.Name
	}
	if err := s.commitSidecarRecords(b, records, func(b *pebble.Batch) error {
		// Catalog, revision, chunks and sidecars remain one atomic WAL record.
		// A crash may lose an expiration; a later sweep retries it. Subsequent
		// durable writes sync this delete too, so old data cannot reappear over
		// a recreated metric. Discovery rebuilds from the recovered catalog.
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		s.pendingSync.Store(true)
		return nil
	}); err != nil {
		return nil, err
	}
	for _, m := range removed {
		s.catalogChanged(m.Name)
	}
	return results, nil
}

// Range tombstones invalidate Pebble's memtable-wide fragment cache on every
// commit. Readers then wait for all of that memtable's tombstones to fragment,
// even when they read unrelated metrics. Expiration visits the existing keys
// instead; the stripes exclude writers until these point deletes commit.
func (s *Store) appendExpirationDelete(ctx context.Context, b *pebble.Batch, reader *transferReader, m Metadata) error {
	if err := b.Delete(catalogKey(m.Name), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	s.canonicalRun(m.ID).Clear(m.ID)
	remove := func(key, _ []byte) error { return b.Delete(key, nil) }
	// Include older generations, as administrative deletion does.
	if err := reader.visit(ctx, &reader.chunks, []byte{'c'}, []byte{'d'}, chunkPrefix(m.ID, 0)[:9], remove); err != nil {
		return err
	}
	if !m.Continuation {
		return nil
	}
	if s.timeBuckets {
		dropper, ok := s.continuation.(ContinuationDropper)
		if !ok {
			return fmt.Errorf("%w: continuation codec cannot drop time-partitioned keys", ErrFormat)
		}
		io := expirationContinuationIO{s.newContinuationIO(ctx, reader.reader, b, m)}
		if err := dropper.Drop(ctx, io); err != nil {
			return err
		}
	}
	return reader.visit(ctx, &reader.continuation, []byte{'u'}, []byte{'v'}, continuationPrefix(m.ID, 0)[:9], remove)
}

// Drop only reads committed keys: this delete batch creates no continuation
// data. Visit can stream point deletes directly into the separate batch.
type expirationContinuationIO struct{ *continuationIO }

func (io expirationContinuationIO) DeletePrefix(prefix []byte) error {
	return io.Visit(prefix, func(key, _ []byte) error { return io.Delete(key) })
}

// ExpirationPressure samples only the pressure needed by maintenance work.
// Full Stats also gathers filesystem and continuation accounting.
func (s *Store) ExpirationPressure() (l0 int64, debt uint64, stalled bool) {
	m := s.db.Metrics()
	s.stallMu.Lock()
	stalled = !s.stallSince.IsZero()
	s.stallMu.Unlock()
	return int64(m.Levels[0].Sublevels), m.Compact.EstimatedDebt, stalled
}
