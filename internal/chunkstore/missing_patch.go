package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

const MissingOnlyMaxPoints = 128

// BeforeBits is empty only for an absent timestamp. Otherwise it must be an
// exact NaN payload. A finite preimage is never a MissingOnly request.
type MissingPoint struct {
	Archive    int    `json:"archive"`
	Step       int    `json:"step"`
	Count      int    `json:"count"`
	Timestamp  int64  `json:"timestamp"`
	BeforeBits string `json:"before_bits"`
	DonorBits  string `json:"donor_bits"`
}
type MissingPatch struct {
	Expected            TransferVersion `json:"expected"`
	Points              []MissingPoint  `json:"points"`
	PlanSHA256          string          `json:"plan_sha256"`
	BatchSHA256         string          `json:"batch_sha256"`
	ParentProofSHA256   string          `json:"parent_proof_sha256"`
	ProjectionSHA256    string          `json:"projection_sha256"`
	SourceBindingSHA256 string          `json:"source_binding_sha256"`
	Nonce               string          `json:"nonce"`
	Apply               bool            `json:"apply"`
}
type MissingPatchResult struct {
	FinitePatchResult
	BatchSHA256     string `json:"batch_sha256"`
	RequestedSHA256 string `json:"requested_sha256"`
	PreimageSHA256  string `json:"preimage_sha256"`
	AppliedSHA256   string `json:"applied_sha256"`
	PointsApplied   int    `json:"points_applied"`
}
type ContinuationMissingPatcher interface {
	PlanMissingPatch(context.Context, ContinuationIO, MissingPatch, int64) ([]RelayoutKV, error)
}

func exactHex(s string, n int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == n && hex.EncodeToString(b) == s
}
func MissingPointHash(points []MissingPoint, mode string) string {
	var b strings.Builder
	for _, p := range points {
		before, donor := p.BeforeBits, p.DonorBits
		if mode == "preimage" {
			donor = ""
		}
		if mode == "applied" {
			before = ""
		}
		fmt.Fprintf(&b, "%d %d %d %d %s %s\n", p.Archive, p.Step, p.Count, p.Timestamp, before, donor)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
func (p MissingPatch) Validate() error {
	if validateName(p.Expected.Name) != nil || p.Expected.ID == 0 || p.Expected.Generation == 0 || len(p.Points) == 0 || len(p.Points) > MissingOnlyMaxPoints || !exactHex(p.Nonce, 16) {
		return errors.New("MissingOnly requires one existing name and 1..128 points")
	}
	for _, s := range []string{p.PlanSHA256, p.BatchSHA256, p.ParentProofSHA256, p.ProjectionSHA256, p.SourceBindingSHA256} {
		if !exactHex(s, 32) {
			return errors.New("MissingOnly immutable SHA256 binding required")
		}
	}
	for i, v := range p.Points {
		if v.Archive < 0 || v.Step <= 0 || v.Count <= 0 || v.Timestamp <= 0 || v.Timestamp > math.MaxUint32 || v.Timestamp%int64(v.Step) != 0 {
			return errors.New("invalid MissingOnly archive/grid")
		}
		if i > 0 && (v.Archive < p.Points[i-1].Archive || (v.Archive == p.Points[i-1].Archive && v.Timestamp <= p.Points[i-1].Timestamp)) {
			return errors.New("MissingOnly points must be unique and ordered")
		}
		if v.BeforeBits != "" {
			u, _ := strconv.ParseUint(v.BeforeBits, 16, 64)
			if !exactHex(v.BeforeBits, 8) || !math.IsNaN(math.Float64frombits(u)) {
				return errors.New("MissingOnly refuses finite preimages")
			}
		}
		u, _ := strconv.ParseUint(v.DonorBits, 16, 64)
		if !exactHex(v.DonorBits, 8) || math.IsNaN(math.Float64frombits(u)) {
			return errors.New("MissingOnly readable non-NaN donor bits required")
		}
	}
	if p.BatchSHA256 != MissingPointHash(p.Points, "requested") {
		return errors.New("MissingOnly batch hash differs")
	}
	return nil
}

func (s *Store) PatchMissing(ctx context.Context, p MissingPatch, fence FiniteCacheFence) (result MissingPatchResult, err error) {
	result.FinitePatchResult = FinitePatchResult{Version: p.Expected, PlanSHA256: p.PlanSHA256, Nonce: p.Nonce}
	result.BatchSHA256 = p.BatchSHA256
	result.RequestedSHA256, result.PreimageSHA256 = MissingPointHash(p.Points, "requested"), MissingPointHash(p.Points, "preimage")
	if err = errors.Join(p.Validate(), ctx.Err(), s.fileOwnershipError(), s.checkNativeReady()); err != nil {
		return
	}
	if failure := s.nativeFailure.Load(); failure != nil {
		return result, failure.err
	}
	if !s.finitePatchMu.TryLock() {
		return result, ErrConflict
	}
	defer s.finitePatchMu.Unlock()
	key := []byte("MISSING/receipt/" + p.PlanSHA256 + "/" + p.BatchSHA256)
	if p.Apply {
		_, closer, e := s.db.Get(key)
		if e == nil {
			closer.Close()
			return result, fmt.Errorf("MissingOnly batch consumed: %w", ErrConflict)
		}
		if !errors.Is(e, pebble.ErrNotFound) {
			return result, e
		}
	}
	codec, ok := s.continuation.(ContinuationMissingPatcher)
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
	writes, err := codec.PlanMissingPatch(ctx, kv, p, s.now().Unix())
	err = errors.Join(err, kv.Close(), view.Close())
	if err != nil {
		return result, err
	}
	if len(writes) == 0 || len(writes) > MissingOnlyMaxPoints+8 {
		return result, ErrConflict
	}
	if !p.Apply {
		_, err = s.publishPointPatch(ctx, m, epoch, nil, true, nil, nil, fence)
		return result, err
	}
	published, err := s.publishPointPatch(ctx, m, epoch, writes, false, key, p, fence)
	published.PlanSHA256, published.Nonce = p.PlanSHA256, p.Nonce
	result.FinitePatchResult = published
	if published.Applied {
		result.PointsApplied = len(p.Points)
		result.AppliedSHA256 = MissingPointHash(p.Points, "applied")
	}
	return result, err
}
