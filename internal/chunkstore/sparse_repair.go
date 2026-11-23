package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// SparseRepairUnsupported means the caller must use ordinary ImportTransfer.
// A revision conflict is ErrConflict instead: reverify before retrying either path.
type SparseRepairUnsupported struct{ Reason string }

func (e *SparseRepairUnsupported) Error() string { return "sparse repair unsupported: " + e.Reason }

// RepairPoint preserves every source float bit across the transport boundary.
type RepairPoint struct {
	Timestamp int64
	Bits      uint64
}
type RepairArchive struct {
	Archive   int
	Retention Retention
	Points    []RepairPoint
}

// SparseRepair carries verifier-selected source points absent from the target.
// Archives are independently authoritative; repair never propagates rollups.
// For equivalence to a full import the caller must supply ALL missing source
// timestamps in ALL archives, including retained raw history outside serving
// retention. A serving-window-only repair still requires full final verification.
type SparseRepair struct {
	Expected TransferVersion
	Archives []RepairArchive
}
type SparseRepairPlan struct{ Writes []RelayoutKV }
type SparseRepairResult struct {
	Version       TransferVersion
	GroupsWritten int
}
type ContinuationSparseRepairer interface {
	PlanSparseRepair(context.Context, ContinuationIO, Metadata, SparseRepair, func() time.Time) (SparseRepairPlan, error)
}

func (p SparseRepair) Validate() error {
	if p.Expected.Name == "" || validateName(p.Expected.Name) != nil {
		return ErrFormat
	}
	seen := make(map[int]bool, len(p.Archives))
	for _, a := range p.Archives {
		if a.Archive < 0 || a.Retention.Step <= 0 || a.Retention.Count <= 0 || seen[a.Archive] {
			return ErrFormat
		}
		seen[a.Archive] = true
		for i, v := range a.Points {
			if v.Timestamp <= 0 || v.Timestamp > math.MaxUint32 || v.Timestamp%int64(a.Retention.Step) != 0 || i > 0 && v.Timestamp <= a.Points[i-1].Timestamp {
				return ErrFormat
			}
		}
	}
	return nil
}

// RepairSparse prepares only affected classic groups on an immutable view and
// atomically publishes them in the existing generation after checking its full
// target revision. Concurrent committed writes win by forcing reverification;
// later queued writes follow the normal writer. This is a storage primitive,
// not a durable transfer receipt or a substitute for final donor verification.
func (s *Store) RepairSparse(ctx context.Context, p SparseRepair) (result SparseRepairResult, err error) {
	if err = errors.Join(p.Validate(), ctx.Err(), s.fileOwnershipError(), s.checkNativeReady()); err != nil {
		return
	}
	if s.readOnly {
		return result, ErrReadOnly
	}
	if failure := s.nativeFailure.Load(); failure != nil {
		return result, failure.err
	}
	codec, ok := s.continuation.(ContinuationSparseRepairer)
	if !ok {
		return result, &SparseRepairUnsupported{"codec"}
	}
	epoch := s.mutationEpoch[s.metricStripe(p.Expected.Name)].Load()
	view := s.db.NewReadView()
	m, err := metadataFrom(view, p.Expected.Name)
	if errors.Is(err, ErrNotFound) {
		err = &SparseRepairUnsupported{"missing target"}
	}
	if err == nil {
		err = s.checkFileOwnership(m)
	}
	if err == nil && (!m.Continuation || m.FileMigration == FileCleanupPending) {
		err = &SparseRepairUnsupported{"target storage"}
	}
	if err == nil && transferVersion(m) != p.Expected {
		err = ErrConflict
	}
	if err == nil && m.Revision == math.MaxUint64 {
		err = ErrConflict
	}
	if err != nil {
		view.Close()
		return result, err
	}
	kv := s.newContinuationIO(ctx, view, nil, m).withIterator()
	now := s.now()
	plan, err := codec.PlanSparseRepair(ctx, kv, m, p, func() time.Time { return now })
	err = errors.Join(err, kv.Close(), view.Close())
	if err != nil {
		return result, err
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	out := s.newContinuationIO(ctx, b, b, m)
	out.indexRevision = m.Revision + 1
	for _, w := range plan.Writes {
		if w.Value == nil {
			err = out.Delete(w.Key)
		} else {
			err = out.Set(w.Key, w.Value)
		}
		if err != nil {
			return result, err
		}
	}
	next := m
	if len(plan.Writes) != 0 {
		next.Revision++
		if err = b.Set(revisionKey(next), revisionBytes(next), nil); err != nil {
			return result, err
		}
	}
	// TryLock avoids waiting behind a reserved import or an unbounded writer.
	mu := &s.metricMu[s.metricStripe(m.Name)]
	if !mu.TryLock() {
		return result, ErrConflict
	}
	func() {
		defer mu.Unlock()
		if s.importPendingLocked(m.Name) != nil || epoch == math.MaxUint64 || s.mutationEpoch[s.metricStripe(m.Name)].Load() != epoch {
			err = ErrConflict
			return
		}
		current, e := metadataFrom(s.db, m.Name)
		if errors.Is(e, ErrNotFound) || e == nil && !sameTransferTarget(current, m) {
			err = ErrConflict
			return
		}
		if e != nil {
			err = e
			return
		}
		if err = errors.Join(ctx.Err(), s.checkFileOwnership(current)); err != nil {
			return
		}
		if len(plan.Writes) == 0 {
			result.Version = transferVersion(current)
			return
		}
		s.beginStateMutation(m.Name)
		defer s.finishStateMutation(m.Name)
		if err = b.Commit(pebble.NoSync); err != nil {
			return
		}
		s.pendingSync.Store(true)
		result.Version = transferVersion(next)
		for _, w := range plan.Writes {
			if len(w.Key) == 20 && w.Key[9] == 'p' {
				result.GroupsWritten++
			}
		}
	}()
	if err == nil && len(plan.Writes) != 0 {
		s.invalidateReadCache()
		err = s.syncWAL()
	}
	if err != nil {
		return result, fmt.Errorf("repair %s: %w", m.Name, err)
	}
	return result, nil
}
