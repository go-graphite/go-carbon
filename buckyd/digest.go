package buckyd

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// DigestRequest asks for per-archive point digests of exact names over
// [From, Until). Zero bounds are open. Replica comparison streams these
// digests instead of points and fetches points only where digests differ.
type DigestRequest struct {
	Names []string
	From  int64
	Until int64
}

// DigestArchive is one archive's retention plus the count and hash of its
// finite points inside the window. Hash is the first 8 bytes (hex) of
// sha256 over big-endian (uint64 timestamp, uint64 float64 bits) in
// timestamp order; an empty window hashes nothing.
type DigestArchive struct {
	Step   int
	Points int
	Count  int
	Hash   string
}

// DigestResult is one requested name, in request order. Found is false for a
// name the store does not hold.
type DigestResult struct {
	Name     string
	Found    bool
	Archives []DigestArchive `json:",omitempty"`
}

func (r DigestRequest) validate() error {
	if len(r.Names) == 0 || len(r.Names) > chunkstore.MaxTransferMetrics {
		return fmt.Errorf("digest needs 1..%d names", chunkstore.MaxTransferMetrics)
	}
	if r.From < 0 || r.Until < 0 || (r.Until != 0 && r.From >= r.Until) {
		return errors.New("digest window needs 0 <= From < Until (0 = open)")
	}
	seen := make(map[string]struct{}, len(r.Names))
	for _, name := range r.Names {
		if name == "" {
			return errors.New("empty digest name")
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate digest name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// digest reads each name's logical snapshot (the points reads and imports
// see) and hashes the window. It is read-only and never flushes the cache:
// callers compare past windows.
func (s *Service) digest(ctx context.Context, r DigestRequest) ([]DigestResult, error) {
	out := make([]DigestResult, len(r.Names))
	for i, name := range r.Names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i].Name = name
		snapshot, err := s.store.Snapshot(ctx, name)
		if errors.Is(err, chunkstore.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("digest %s: %w", name, err)
		}
		out[i].Found = true
		out[i].Archives = make([]DigestArchive, len(snapshot.Archives))
		for a, archive := range snapshot.Archives {
			out[i].Archives[a] = digestArchive(archive, r.From, r.Until)
		}
	}
	return out, nil
}

func digestArchive(archive chunkstore.Archive, from, until int64) DigestArchive {
	points := archive.Points
	byTime := func(a, b chunkstore.Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) }
	if !slices.IsSortedFunc(points, byTime) {
		points = slices.Clone(points)
		slices.SortStableFunc(points, byTime)
	}
	h := sha256.New()
	var buf [16]byte
	count := 0
	for _, p := range points {
		ts := p.Timestamp
		if ts < from || (until != 0 && ts >= until) || math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
			continue
		}
		binary.BigEndian.PutUint64(buf[:8], uint64(ts))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(p.Value))
		_, _ = h.Write(buf[:])
		count++
	}
	sum := h.Sum(nil)
	return DigestArchive{Step: archive.Retention.Step, Points: archive.Retention.Count, Count: count, Hash: hex.EncodeToString(sum[:8])}
}
