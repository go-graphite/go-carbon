package chunkstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/pebble"
)

// FinitePatch is an exact, single-slot correction, never an intake batch.
// Expected fences the entire committed image, including every other archive.
type FinitePatch struct {
	Expected   TransferVersion `json:"expected"`
	Archive    int             `json:"archive"`
	Step       int             `json:"step"`
	Timestamp  int64           `json:"timestamp"`
	BeforeBits string          `json:"before_bits"`
	DonorBits  string          `json:"donor_bits"`
	PlanSHA256 string          `json:"plan_sha256"`
	Nonce      string          `json:"nonce"`
	Apply      bool            `json:"apply"`
}
type FinitePatchPlan struct {
	Writes []RelayoutKV
	Noop   bool
}
type ContinuationFinitePatcher interface {
	PlanFinitePatch(context.Context, ContinuationIO, FinitePatch) (FinitePatchPlan, error)
}

// FiniteCacheFence must hold the live cache's exclusion through publish and
// refuse queued, recovered and in-flight input. A missing fence refuses apply.
type FiniteCacheFence func(context.Context, string, func() error) error
type FinitePatchResult struct {
	Version    TransferVersion `json:"version"`
	Applied    bool            `json:"applied"`
	Noop       bool            `json:"noop"`
	PlanSHA256 string          `json:"plan_sha256"`
	Nonce      string          `json:"nonce"`
	LockNanos  int64           `json:"lock_ns"`
}

func (p FinitePatch) Validate() error {
	for _, v := range []string{p.BeforeBits, p.DonorBits} {
		if len(v) != 16 {
			return errors.New("exact 16-hex finite bits required")
		}
		u, err := strconv.ParseUint(v, 16, 64)
		if err != nil || fmt.Sprintf("%016x", u) != v || math.IsNaN(math.Float64frombits(u)) || math.IsInf(math.Float64frombits(u), 0) {
			return errors.New("finite bits required")
		}
	}
	sha, err := hex.DecodeString(p.PlanSHA256)
	nonce, nerr := hex.DecodeString(p.Nonce)
	if err != nil || len(sha) != 32 || hex.EncodeToString(sha) != p.PlanSHA256 || nerr != nil || len(nonce) != 16 || hex.EncodeToString(nonce) != p.Nonce || validateName(p.Expected.Name) != nil || p.Expected.ID == 0 || p.Expected.Generation == 0 || p.Archive != 0 || p.Step <= 0 || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(p.Step) != 0 || p.BeforeBits == p.DonorBits {
		return errors.New("invalid one-point finite patch scope")
	}
	return nil
}

// PatchFinite prepares exactly one physical point block from an immutable
// view. It never calls Update/Fill/Replace and therefore cannot propagate a
// rollup, rotate a ring, drain cache input or change metric identity/policy.
// There is one publication attempt. WAL durability follows stripe release.
func (s *Store) PatchFinite(ctx context.Context, p FinitePatch, fence FiniteCacheFence) (result FinitePatchResult, err error) {
	result = FinitePatchResult{Version: p.Expected, PlanSHA256: p.PlanSHA256, Nonce: p.Nonce}
	if err = errors.Join(p.Validate(), ctx.Err(), s.fileOwnershipError(), s.checkNativeReady()); err != nil {
		return
	}
	if failure := s.nativeFailure.Load(); failure != nil {
		return result, failure.err
	}
	// A durable plan receipt plus a separate admission mutex enforce cap one
	// across requests and across metrics, without holding any writer stripe
	// during preparation. A consumed plan is never replayed after lost ACK.
	receiptKey := []byte("FINITE/receipt/" + p.PlanSHA256)
	if p.Apply {
		if !s.finitePatchMu.TryLock() {
			return result, ErrConflict
		}
		defer s.finitePatchMu.Unlock()
		_, closer, readErr := s.db.Get(receiptKey)
		if readErr == nil {
			closer.Close()
			return result, fmt.Errorf("finite plan already consumed: %w", ErrConflict)
		}
		if !errors.Is(readErr, pebble.ErrNotFound) {
			return result, readErr
		}
	}
	codec, ok := s.continuation.(ContinuationFinitePatcher)
	if !ok {
		return result, ErrFormat
	}
	epoch := s.mutationEpoch[s.metricStripe(p.Expected.Name)].Load()
	view := s.db.NewReadView()
	m, err := metadataFrom(view, p.Expected.Name)
	if err == nil {
		err = s.checkFileOwnership(m)
	}
	if err == nil && (!m.Continuation || transferVersion(m) != p.Expected || m.FileMigration == FileCleanupPending) {
		err = ErrConflict
	}
	if err != nil {
		view.Close()
		return result, err
	}
	kv := s.newContinuationIO(ctx, view, nil, m).withIterator()
	plan, err := codec.PlanFinitePatch(ctx, kv, p)
	err = errors.Join(err, kv.Close(), view.Close())
	if err != nil {
		return result, err
	}
	result.Noop = plan.Noop
	if !p.Apply {
		return result, nil
	}
	if fence == nil {
		return result, fmt.Errorf("live cache fence unavailable: %w", ErrConflict)
	}
	if len(plan.Writes) > 1 || (!plan.Noop && len(plan.Writes) != 1) {
		return result, ErrConflict
	}
	published, err := s.publishPointPatch(ctx, m, epoch, plan.Writes, plan.Noop, receiptKey, p, fence)
	published.PlanSHA256, published.Nonce = p.PlanSHA256, p.Nonce
	if published.Applied {
		s.nativeCounters.finitePatches.Add(1)
	}
	return published, err
}
