package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// ProofRelease selects a current generation and optionally one exact window.
// Zero ID/Generation means the current generation; zero bounds means all scopes.
type ProofRelease struct {
	Name           string
	ID, Generation uint64
	From, Until    int64
}

type ProofReleaseResult struct {
	Name     string
	Status   string // released, absent, conflict, or error
	Released int
	Error    string `json:",omitempty"`
}

type ProofResetReceipt struct{ Finished time.Time }

type proofCounters struct{ tracked, untracked, registered, released, resets, errors atomic.Uint64 }
type ProofStats struct{ TrackedWrites, UntrackedWrites, Registered, Released, Resets, Errors uint64 }

func (s *Store) ProofStats() ProofStats {
	c := &s.proofCounters
	return ProofStats{c.tracked.Load(), c.untracked.Load(), c.registered.Load(), c.released.Load(), c.resets.Load(), c.errors.Load()}
}
func (s *Store) recordProofWrite(tracked bool) {
	if tracked {
		s.proofCounters.tracked.Add(1)
	} else {
		s.proofCounters.untracked.Add(1)
	}
}

func (r ProofRelease) Validate() error {
	if err := validateName(r.Name); err != nil {
		return err
	}
	if (r.ID == 0) != (r.Generation == 0) {
		return errors.New("ID and Generation must be supplied together")
	}
	if (r.From != 0 || r.Until != 0) && (r.From <= 0 || r.Until <= r.From) {
		return errors.New("invalid exact proof window")
	}
	return nil
}

// ReleaseMutationProofs serializes with registration and all pending imports and
// updates. Successful removals commit together durably; retries are idempotent.
func (s *Store) ReleaseMutationProofs(ctx context.Context, requests []ProofRelease) []ProofReleaseResult {
	out := make([]ProofReleaseResult, len(requests))
	names := make([]string, 0, len(requests))
	for _, r := range requests {
		if r.Validate() == nil {
			names = append(names, r.Name)
		}
	}
	// The same sorted-stripe/reservation protocol as lockMetric, extended to a
	// bounded group so closeout pays one WAL sync per page, not per metric.
	s.lockImportFreeNames(names, false)
	stripes := s.transferStripes(names)
	defer func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}()
	batch := s.db.NewIndexedBatch()
	defer batch.Close()
	for i, r := range requests {
		result, err := s.stageProofRelease(ctx, batch, r)
		if err != nil {
			result.Status, result.Error = "error", err.Error()
		}
		out[i] = result
	}
	if batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			for i := range out {
				if out[i].Status == "released" {
					out[i].Status, out[i].Error = "error", err.Error()
				}
			}
		}
	}
	for _, result := range out {
		if result.Status == "error" || result.Status == "conflict" {
			s.proofCounters.errors.Add(1)
		}
		if result.Status == "released" {
			s.proofCounters.released.Add(uint64(result.Released))
		}
	}
	return out
}

func (s *Store) stageProofRelease(ctx context.Context, batch *pebble.Batch, r ProofRelease) (result ProofReleaseResult, err error) {
	result.Name = r.Name
	if err = r.Validate(); err != nil {
		return
	}
	if s.readOnly {
		return result, ErrReadOnly
	}
	if err = ctx.Err(); err != nil {
		return
	}
	m, err := metadataFrom(s.db, r.Name)
	if errors.Is(err, ErrNotFound) {
		result.Status = "absent"
		return result, nil
	}
	if err != nil {
		return
	}
	if r.ID != 0 && (m.ID != r.ID || m.Generation != r.Generation) {
		result.Status = "conflict"
		return result, nil
	}
	h, err := readMutationHistory(batch, m)
	// An all-scope release can also retire unusable metadata. Exact releases
	// cannot interpret a corrupt/unsupported record and fail closed.
	if err != nil && r.From != 0 {
		return
	}
	if err == nil && h.Version == 0 {
		result.Status = "absent"
		return result, nil
	}
	kept := h.Scopes[:0]
	for _, scope := range h.Scopes {
		if r.From == 0 || scope.From == r.From && scope.Until == r.Until {
			result.Released++
		} else {
			kept = append(kept, scope)
		}
	}
	if r.From == 0 || len(kept) == 0 {
		err = batch.Delete(mutationKey(m), nil)
	} else if result.Released > 0 {
		h.Scopes = kept
		var data []byte
		data, err = json.Marshal(h)
		if err == nil {
			err = batch.Set(mutationKey(m), data, nil)
		}
	}
	if err != nil {
		return
	}
	result.Status = "released"
	return
}

// ResetMutationProofs drains every staged mutation before deleting exactly the
// history namespace. Holding all stripes excludes registration and new stages;
// existing reservations must finish publication before the fence is acquired.
// No pre-reset batch can therefore commit history after this synced tombstone.
func (s *Store) ResetMutationProofs(ctx context.Context) (receipt ProofResetReceipt, err error) {
	defer func() {
		if err != nil {
			s.proofCounters.errors.Add(1)
		}
	}()
	if s.readOnly {
		return receipt, ErrReadOnly
	}
	if err = ctx.Err(); err != nil {
		return
	}
	unlock := s.lockTransferWriters()
	defer unlock()
	if err = ctx.Err(); err != nil {
		return
	}
	if err = s.db.DeleteRange([]byte("proof/ranges/"), []byte("proof/ranges0"), pebble.Sync); err != nil {
		return receipt, fmt.Errorf("reset mutation proofs: %w", err)
	}
	s.proofCounters.resets.Add(1)
	receipt.Finished = time.Now().UTC()
	return
}
