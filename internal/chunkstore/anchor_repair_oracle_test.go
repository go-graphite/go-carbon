package chunkstore

// Frozen pre-optimisation publication oracle; do not share the fast path.
import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// RepairArchiveAnchors prepares from immutable committed bytes outside writer
// stripes. Publication validates the original revision under the ordinary
// transfer lock; a racing foreground update causes a fresh preparation. Cache
// input remains with the sole live daemon and is never flushed or replayed here.
func (s *Store) RepairArchiveAnchorsOracleForTest(ctx context.Context, name string, apply bool) (AnchorRepairResult, error) {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return AnchorRepairResult{}, err
	}
	codec, ok := s.continuation.(ContinuationAnchorRepairer)
	if !ok {
		return AnchorRepairResult{}, ErrFormat
	}
	for attempt := 0; attempt < 4; attempt++ {
		preparedAt := time.Now()
		// A census must inspect the main-file descriptor before reading its
		// body. ExportTransfer would copy every cold compressed group merely
		// to discover that it needs no classic-ring repair.
		view := s.db.NewReadView()
		m, err := metadataFrom(view, name)
		if err == nil {
			err = s.checkFileOwnership(m)
		}
		if err == nil && m.FileMigration == FileCleanupPending {
			err = ErrConflict
		}
		if err != nil {
			view.Close()
			return AnchorRepairResult{}, err
		}
		r := TransferMetric{Config: m.MetricConfig, Version: transferVersion(m), Continuation: m.Continuation}
		result := AnchorRepairResult{Name: name, Version: r.Version, Retries: attempt}
		if !m.Continuation {
			view.Close()
			return result, nil
		}
		kv := s.newContinuationIO(ctx, view, nil, m).withIterator()
		plan, err := codec.PlanArchiveAnchors(ctx, kv)
		closeErr := kv.Close()
		viewErr := view.Close()
		err = errors.Join(err, closeErr, viewErr)
		result.Archives = plan.Archives
		result.PreparationNanos = time.Since(preparedAt).Nanoseconds()
		if err != nil || !apply || len(plan.Writes)+len(plan.Deletes) == 0 {
			return result, err
		}
		for _, a := range plan.Archives {
			if a.Blocked != "" {
				return result, errors.New("anchor repair requires source-aware reconciliation: " + a.Blocked)
			}
		}
		applied, retry, err := s.publishArchiveAnchorsOracleForTest(ctx, r, plan, &result)
		if retry {
			continue
		}
		result.Applied = applied
		return result, err
	}
	return AnchorRepairResult{}, ErrConflict
}

func (s *Store) publishArchiveAnchorsOracleForTest(ctx context.Context, r TransferMetric, plan AnchorRepairPlan, result *AnchorRepairResult) (bool, bool, error) {
	// Encoding and batch allocation also stay outside the writer stripe.
	b := s.db.NewIndexedBatch()
	defer b.Close()
	identity := Metadata{MetricConfig: r.Config, ID: r.Version.ID, Generation: r.Version.Generation, Continuation: true}
	kv := s.newContinuationIO(ctx, b, b, identity)
	for _, k := range plan.Deletes {
		if err := kv.Delete(k); err != nil {
			return false, false, err
		}
	}
	for _, p := range plan.Writes {
		if err := kv.Set(p.Key, p.Value); err != nil {
			return false, false, err
		}
	}
	unlock := s.lockTransfer([]string{r.Config.Name}, nil)
	lockedAt := time.Now()
	release := func() { result.LockNanos += time.Since(lockedAt).Nanoseconds(); unlock() }
	m, err := metadataFrom(s.db, r.Config.Name)
	if err == nil && (transferVersion(m) != r.Version || !samePolicy(m.MetricConfig, r.Config) || !m.Continuation) {
		release()
		return false, true, nil
	}
	if err == nil {
		err = s.checkFileOwnership(m)
	}
	if err == nil && m.FileMigration == FileCleanupPending {
		err = ErrConflict
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && m.Revision == math.MaxUint64 {
		err = errors.New("metric revision exhausted")
	}
	if err == nil {
		m.Revision++
		err = b.Set(revisionKey(m), revisionBytes(m), nil)
	}
	if err == nil {
		err = b.Commit(pebble.NoSync)
		if err == nil {
			s.pendingSync.Store(true)
		}
	}
	release()
	if err != nil {
		return false, false, err
	}
	s.invalidateReadCache()
	// Durability waits after releasing writer stripes. A lost acknowledgement
	// may cause a repeated inspection; a completed rotation is then a no-op.
	if err = s.syncWAL(); err != nil {
		return true, false, err
	}
	return true, false, nil
}
