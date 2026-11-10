package chunkstore

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// AnchorArchive records physical classic-ring consistency, independently of
// Fetch (which cannot diagnose an empty anchor) and logical snapshot export.
type AnchorArchive struct {
	Index           int    `json:"index"`
	Points          int    `json:"points"`
	Misplaced       int    `json:"misplaced"`
	DuplicatePoints int    `json:"duplicate_points,omitempty"`
	Changed         bool   `json:"changed"`
	Blocked         string `json:"blocked,omitempty"`
}
type AnchorRepairPlan struct {
	Archives []AnchorArchive `json:"archives"`
	Writes   []RelayoutKV    `json:"-"`
	Deletes  [][]byte        `json:"-"`
}
type ContinuationAnchorRepairer interface {
	PlanArchiveAnchors(context.Context, ContinuationIO) (AnchorRepairPlan, error)
}
type AnchorRepairResult struct {
	Name             string          `json:"name"`
	Version          TransferVersion `json:"version"`
	Archives         []AnchorArchive `json:"archives"`
	Applied          bool            `json:"applied"`
	PreparationNanos int64           `json:"preparation_ns"`
	// LockNanos is the maximum hold across attempts, including cache cleanup.
	LockNanos int64 `json:"lock_ns"`
	Retries   int   `json:"retries"`
}

// RepairArchiveAnchors prepares from immutable committed bytes outside writer
// stripes. Publication validates the original revision under the ordinary
// transfer lock; a racing foreground update causes a fresh preparation. Cache
// input remains with the sole live daemon and is never flushed or replayed here.
func (s *Store) RepairArchiveAnchors(ctx context.Context, name string, apply bool) (AnchorRepairResult, error) {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return AnchorRepairResult{}, err
	}
	codec, ok := s.continuation.(ContinuationAnchorRepairer)
	if !ok {
		return AnchorRepairResult{}, ErrFormat
	}
	var maxLock int64
	var result AnchorRepairResult
	for attempt := 0; attempt < 4; attempt++ {
		preparedAt := time.Now()
		// A census must inspect the main-file descriptor before reading its
		// body. ExportTransfer would copy every cold compressed group merely
		// to discover that it needs no classic-ring repair.
		// Read before opening the view, including when a writer is currently
		// active. Its end increment will force a retry after we get the stripe.
		epoch := s.mutationEpoch[s.metricStripe(name)].Load()
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
		result = AnchorRepairResult{Name: name, Version: r.Version, Retries: attempt, LockNanos: maxLock}
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
		applied, retry, err := s.publishArchiveAnchors(ctx, m, epoch, plan, &result)
		maxLock = max(maxLock, result.LockNanos)
		if retry {
			continue
		}
		result.Applied = applied
		return result, err
	}
	return result, ErrConflict
}

func (s *Store) publishArchiveAnchors(ctx context.Context, m Metadata, epoch uint64, plan AnchorRepairPlan, result *AnchorRepairResult) (bool, bool, error) {
	// Encoding and batch allocation also stay outside the writer stripe.
	b := s.db.NewIndexedBatch()
	defer b.Close()
	kv := s.newContinuationIO(ctx, b, b, m)
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
	if m.Revision == math.MaxUint64 {
		return false, false, errors.New("metric revision exhausted")
	}
	m.Revision++
	err := b.Set(revisionKey(m), revisionBytes(m), nil)
	if err != nil {
		return false, false, err
	}
	unlock := s.lockMetric(m.Name)
	lockedAt := time.Now()
	mutating := false
	release := func() {
		if mutating {
			s.finishStateMutation(m.Name)
		}
		// Capture while still owning the stripe: a scheduling delay after
		// Unlock is not lock hold time and must not trip the client's bound.
		held := time.Since(lockedAt).Nanoseconds()
		unlock()
		result.LockNanos = max(result.LockNanos, held)
	}
	// Equality proves that no mutation (including delete/recreate, policy,
	// generation or revision-neutral maintenance) started or ended since the
	// view was opened. This is stronger than rereading Revision/Generation,
	// and avoids two potentially cold LSM reads while excluding writers.
	if epoch == math.MaxUint64 || s.mutationEpoch[s.metricStripe(m.Name)].Load() != epoch {
		release()
		return false, true, nil
	}
	err = errors.Join(ctx.Err(), s.checkFileOwnership(m))
	if err == nil {
		s.beginStateMutation(m.Name)
		mutating = true
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
