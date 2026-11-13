package buckyd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// DigestRequest asks for point digests of exact names over [From, Until)
// in Unix seconds, 0 < From < Until. Replica comparison streams these digests
// instead of points and fetches points only where digests differ.
type DigestRequest struct {
	Names []string
	From  int64
	Until int64
}

// DigestArchive is the archive that serves the window (the one a render of
// [From, Until) reads: Step and its retention Points) plus the count and hash
// of its finite points inside the window. Hash is the first 8 bytes (hex) of
// sha256 over big-endian (uint64 timestamp, uint64 float64 bits) in timestamp
// order; an empty window hashes nothing.
type DigestArchive struct {
	Step   int
	Points int
	Count  int
	Hash   string
}

// DigestResult is one requested name, in request order. Found is false for a
// name the store does not hold. Archives holds the serving archive; it is
// empty when the window lies wholly outside the name's retention.
type DigestResult struct {
	Name     string
	Found    bool
	Archives []DigestArchive `json:",omitempty"`
}

func (r DigestRequest) validate() error {
	if len(r.Names) == 0 || len(r.Names) > chunkstore.MaxTransferMetrics {
		return fmt.Errorf("digest needs 1..%d names", chunkstore.MaxTransferMetrics)
	}
	if r.From <= 0 || r.Until <= r.From || r.Until > math.MaxInt32 {
		return errors.New("digest window needs 0 < From < Until (Unix seconds)")
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

// digest reads only the window, through the same archive selection and range
// read as render (Store.Fetch), and hashes its finite points. Decoding a full
// snapshot instead costs every archive's whole history per name. It is
// read-only and never flushes the cache: callers compare past windows.
func (s *Service) digest(ctx context.Context, r DigestRequest) ([]DigestResult, error) {
	out := make([]DigestResult, len(r.Names))
	for i, name := range r.Names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i].Name = name
		// Fetch starts one step after its from time (whisper intervals), so
		// ask from From-1 to keep a point at From; digestSeries drops points
		// outside [From, Until).
		series, err := s.store.Fetch(ctx, name, int(r.From)-1, int(r.Until))
		if errors.Is(err, chunkstore.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("digest %s: %w", name, err)
		}
		out[i].Found = true
		if series != nil && series.Step > 0 {
			out[i].Archives = []DigestArchive{digestSeries(series, r.From, r.Until)}
		}
	}
	return out, nil
}

func digestSeries(series *chunkstore.Series, from, until int64) DigestArchive {
	d := DigestArchive{Step: series.Step}
	for _, retention := range series.Metadata.Retentions {
		if retention.Step == series.Step {
			d.Points = retention.Count
			break
		}
	}
	h := sha256.New()
	var buf [16]byte
	step := int64(series.Step)
	for i, v := range series.Values {
		ts := int64(series.FromTime) + int64(i)*step
		if ts < from || ts >= until || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		binary.BigEndian.PutUint64(buf[:8], uint64(ts))
		binary.BigEndian.PutUint64(buf[8:], math.Float64bits(v))
		_, _ = h.Write(buf[:])
		d.Count++
	}
	sum := h.Sum(nil)
	d.Hash = hex.EncodeToString(sum[:8])
	return d
}
