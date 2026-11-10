package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
)

// publishPointPatch is the common finite/MissingOnly publication boundary.
// Preparation is outside the writer stripe; all bytes commit together while
// intake is excluded. Neither path invokes the normal propagating writer.
func (s *Store) publishPointPatch(ctx context.Context, m Metadata, epoch uint64, writes []RelayoutKV, noop bool, receiptKey []byte, receipt any, fence FiniteCacheFence) (result FinitePatchResult, err error) {
	result.Version, result.Noop = transferVersion(m), noop
	if fence == nil || m.Revision == math.MaxUint64 {
		return result, ErrConflict
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	out := s.newContinuationIO(ctx, b, b, m)
	for _, w := range writes {
		if err = out.Set(w.Key, w.Value); err != nil {
			return result, err
		}
	}
	if !noop {
		raw, encodeErr := json.Marshal(receipt)
		if encodeErr != nil {
			return result, encodeErr
		}
		if err = b.Set(receiptKey, raw, nil); err != nil {
			return result, err
		}
		m.Revision++
		if err = b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return result, err
		}
	}
	mu := &s.metricMu[s.metricStripe(m.Name)]
	if !mu.TryLock() {
		return result, ErrConflict
	}
	lockedAt := time.Now()
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	err = fence(lockCtx, m.Name, func() error {
		if epoch == math.MaxUint64 || s.mutationEpoch[s.metricStripe(m.Name)].Load() != epoch {
			return ErrConflict
		}
		if err := errors.Join(lockCtx.Err(), s.checkFileOwnership(m)); err != nil {
			return err
		}
		if noop {
			return nil
		}
		s.beginStateMutation(m.Name)
		defer s.finishStateMutation(m.Name)
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		s.pendingSync.Store(true)
		result.Applied, result.Version = true, transferVersion(m)
		return nil
	})
	cancel()
	result.LockNanos = time.Since(lockedAt).Nanoseconds()
	mu.Unlock()
	if result.Applied {
		s.invalidateReadCache()
		err = errors.Join(err, s.syncWAL())
	}
	if result.LockNanos > int64(10*time.Millisecond) {
		err = errors.Join(err, errors.New("point patch stripe exceeded 10ms; reconcile, never retry"))
	}
	return result, err
}
