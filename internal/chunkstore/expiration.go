package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

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
	// A consistent view collects keys without excluding writers. Validation
	// below uses a new view after acquiring the mutation stripes.
	b := s.db.NewBatch()
	defer b.Close()
	spans, results, err := s.prepareExpirationDeletes(ctx, b, expected)
	if err != nil {
		return nil, err
	}
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
	removed := make([]Metadata, 0, len(expected))
	changed := false
	view := s.db.NewReadView()
	defer view.Close()
	reader := transferReader{reader: view}
	defer reader.close()
	for i, want := range expected {
		if spans[i].end == 0 {
			continue
		}
		m, err := reader.metadata(want.Name)
		if errors.Is(err, ErrNotFound) {
			results[i] = err
			changed = true
			continue
		}
		if err != nil {
			return nil, err
		}
		if !expirationMatches(m, want) {
			results[i] = ErrConflict
			changed = true
			continue
		}
		s.canonicalRun(m.ID).Clear(m.ID)
		removed = append(removed, m)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(removed) == 0 {
		return results, nil
	}
	if changed {
		// The common path commits the prepared batch verbatim. Rebuild only
		// when a concurrent mutation invalidated one of its candidates.
		filtered := s.db.NewBatch()
		defer filtered.Close()
		data := b.Repr()
		for i, span := range spans {
			if span.end == 0 || results[i] != nil {
				continue
			}
			entries := pebble.BatchReader(data[span.start:span.end])
			for len(entries) > 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				kind, key, value, ok, err := entries.Next()
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, pebble.ErrInvalidBatch
				}
				switch kind {
				case pebble.InternalKeyKindDelete:
					err = filtered.Delete(key, nil)
				case pebble.InternalKeyKindSet:
					// ContinuationDropper implementations may update a descriptor
					// as well as delete data; retain its complete candidate span.
					err = filtered.Set(key, value, nil)
				default:
					return nil, fmt.Errorf("%w: unexpected expiration record", ErrFormat)
				}
				if err != nil {
					return nil, err
				}
			}
		}
		b = filtered
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

func expirationMatches(m, want Metadata) bool {
	return m.ID == want.ID && m.Generation == want.Generation && m.Revision == want.Revision && m.FileMigration != FileCleanupPending
}

type expirationSpan struct{ start, end int }

const expirationStripesPerPartition = 8

// ExpirationPartition returns an opaque store-local routing key. Group a batch
// by this key and send equal keys to the same worker for the entire sweep.
// Each key covers a bounded set of mutation stripes disjoint from all other
// keys. Callers must not persist keys or assume their range.
func (s *Store) ExpirationPartition(name string) uint64 {
	return s.metricStripe(name) / expirationStripesPerPartition
}

func (s *Store) prepareExpirationDeletes(ctx context.Context, batch *pebble.Batch, expected []Metadata) ([]expirationSpan, []error, error) {
	spans, results := make([]expirationSpan, len(expected)), make([]error, len(expected))
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
			return nil, nil, err
		}
		if !expirationMatches(m, want) {
			results[i] = ErrConflict
			continue
		}
		spans[i].start = batch.Len()
		if err := s.appendExpirationDelete(ctx, batch, &reader, m); err != nil {
			return nil, nil, err
		}
		spans[i].end = batch.Len()
	}
	return spans, results, nil
}

// Range tombstones invalidate Pebble's memtable-wide fragment cache on every
// commit. Readers then wait for all of that memtable's tombstones to fragment,
// even when they read unrelated metrics. Expiration visits the existing keys
// instead, then revalidates under the mutation stripes before committing.
func (s *Store) appendExpirationDelete(ctx context.Context, b *pebble.Batch, reader *transferReader, m Metadata) error {
	if err := b.Delete(catalogKey(m.Name), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	// Include older generations, as administrative deletion does.
	if err := reader.deleteExpirationPrefix(ctx, b, &reader.chunks, []byte{'c'}, []byte{'d'}, chunkPrefix(m.ID, 0)[:9]); err != nil {
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
		io := expirationContinuationIO{s.newContinuationIO(ctx, reader.reader, b, m), reader}
		if err := dropper.Drop(ctx, io); err != nil {
			return err
		}
	}
	return reader.deleteExpirationPrefix(ctx, b, &reader.continuation, continuationLower, continuationUpper, continuationPrefix(m.ID, 0)[:9])
}

func (r *transferReader) deleteExpirationPrefix(ctx context.Context, batch *pebble.Batch, slot **pebble.Iterator, lower, upper, prefix []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	it, err := r.iterator(slot, lower, upper)
	if err != nil {
		return err
	}
	// Bounds stop before the next metric's block. Reuse the table readers but
	// restore their broad bounds for subsequent uses, including older generations.
	it.SetBounds(prefix, prefixEnd(prefix))
	defer it.SetBounds(lower, upper)
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := batch.Delete(it.Key(), nil); err != nil {
			return err
		}
	}
	return it.Error()
}

// Drop only reads committed keys: this delete batch creates no continuation
// data. Visit can stream point deletes directly into the separate batch.
type expirationContinuationIO struct {
	*continuationIO
	walk *transferReader
}

func (io expirationContinuationIO) DeletePrefix(prefix []byte) error {
	slot, lower, upper := &io.walk.continuation, continuationLower, continuationUpper
	if isContinuationBucketSuffix(prefix) {
		slot, lower, upper = &io.walk.buckets, []byte{'t'}, []byte{'u'}
	}
	// Delete stored keys directly. Ordinary Visit copies suffixes and values,
	// and Delete reconstructs the same key; expiration needs none of that.
	return io.walk.deleteExpirationPrefix(io.ctx, io.batch, slot, lower, upper, io.key(prefix))
}

// expirationPressureCache shares the relatively expensive database metrics
// sample across every expiration worker. Its period is fixed before use; zero
// selects the default. Write stalls are checked separately on every call.
type expirationPressureCache struct {
	mu      sync.Mutex
	period  time.Duration
	sampled time.Time
	l0      int64
	debt    uint64
}

const expirationPressurePeriod = time.Second

func (c *expirationPressureCache) load(now time.Time, sample func() (int64, uint64)) (int64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	period := c.period
	if period <= 0 {
		period = expirationPressurePeriod
	}
	if c.sampled.IsZero() || now.Sub(c.sampled) >= period {
		c.l0, c.debt = sample()
		c.sampled = now
	}
	return c.l0, c.debt
}

func (s *Store) expirationStalled() bool {
	s.stallMu.Lock()
	defer s.stallMu.Unlock()
	return !s.stallSince.IsZero()
}

// ExpirationPressure shares L0/debt samples for at most one second. The active
// write-stall signal is always live, even while another worker samples metrics.
func (s *Store) ExpirationPressure() (l0 int64, debt uint64, stalled bool) {
	if s.expirationStalled() {
		return 0, 0, true
	}
	l0, debt = s.expirationPressure.load(time.Now(), func() (int64, uint64) {
		m := s.db.Metrics()
		return int64(m.Levels[0].Sublevels), m.Compact.EstimatedDebt
	})
	// A stall may have begun while this caller waited for the shared sample.
	return l0, debt, s.expirationStalled()
}
