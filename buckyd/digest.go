package buckyd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math"
	"sync"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// DigestRequest asks for point digests of exact names over [From, Until)
// in Unix seconds, 0 < From < Until. Replica comparison streams these digests
// instead of points and fetches points only where digests differ.
type DigestRequest struct {
	Names []string
	// Mode sum-v1 returns every archive; zero bounds request full history.
	Mode  string `json:",omitempty"`
	Step  int    `json:",omitempty"`
	From  int64
	Until int64
}

// DigestArchive is the archive that serves the window (the one a render of
// [From, Until) reads: Step and its retention Points) plus the count and hash
// of its finite points inside the window. Hash is the first 8 bytes (hex) of
// sha256 over big-endian (uint64 timestamp, uint64 float64 bits) in timestamp
// order; an empty window hashes nothing. In sum-v1 mode Hash is the 64-bit
// additive fingerprint instead, From/Until identify each clipped archive, and
// every archive is returned. Clients must never compare different modes.
type DigestArchive struct {
	Step   int
	Points int
	Count  int
	Hash   string
	From   int `json:",omitempty"`
	Until  int `json:",omitempty"`
}

// DigestResult is one requested name, in request order. Found is false for a
// name the store does not hold. Archives holds the serving archive; it is
// empty when the window lies wholly outside the name's retention.
type DigestResult struct {
	Incarnation   string `json:",omitempty"`
	Process       string `json:",omitempty"`
	Predecessor   string `json:",omitempty"`
	LineageReason string `json:",omitempty"`
	Name          string
	Proof         string `json:",omitempty"`
	Reason        string `json:",omitempty"`
	Mode          string `json:",omitempty"`
	Found         bool
	Archives      []DigestArchive `json:",omitempty"`
}

func (r DigestRequest) validate() error {
	if r.Mode == "proof-v2" && r.Step != 0 {
		return errors.New("window mutation proofs require zero step")
	}
	if r.Mode == "proof-v1" && (r.From != 0 || r.Until != 0 || r.Step != 0) {
		return errors.New("mutation proofs require zero bounds and step")
	}
	if len(r.Names) == 0 || len(r.Names) > chunkstore.MaxTransferMetrics {
		return fmt.Errorf("digest needs 1..%d names", chunkstore.MaxTransferMetrics)
	}
	if r.Mode != "" && r.Mode != "sum-v1" && r.Mode != "proof-v1" && r.Mode != "proof-v2" {
		return errors.New("unknown digest mode")
	}
	if !(r.Mode == "proof-v1" || r.Mode == "sum-v1" && r.From == 0 && r.Until == 0) && (r.From <= 0 || r.Until <= r.From || r.Until > math.MaxInt32) {
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
// read-only with respect to points and never flushes live input: callers compare
// past windows. sum-v1 may populate its disposable persisted digest cache.
func (s *Service) digest(ctx context.Context, r DigestRequest) ([]DigestResult, error) {
	out := make([]DigestResult, len(r.Names))
	err := s.store.DigestBatch(ctx, r.Names, func(ctx context.Context, i int) error {
		name := r.Names[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		out[i].Name = name
		out[i].Mode = r.Mode
		if r.Mode == "proof-v2" {
			out[i].Incarnation, out[i].Process, out[i].Predecessor, out[i].LineageReason = s.store.ProofLineage()
			_, e := s.store.Metadata(ctx, name)
			if e != nil && !errors.Is(e, chunkstore.ErrNotFound) {
				return e
			}
			out[i].Found = e == nil
			out[i].Proof, out[i].Reason, e = s.store.MutationProof(ctx, name, r.From, r.Until)
			return e
		}
		if r.Mode == "proof-v1" {
			m, err := s.store.Metadata(ctx, name)
			if err != nil && !errors.Is(err, chunkstore.ErrNotFound) {
				return err
			}
			out[i].Found = err == nil
			// Legacy files can change outside the native revision fence. Such
			// ownership is explicitly unknown, never a reusable clean proof.
			if s.proofEpoch == "" || s.store.IncrementalSource() != "" && (!out[i].Found || m.FileMigration != chunkstore.FileReclaimed) {
				return nil
			}
			data, err := json.Marshal(struct {
				Epoch   string
				Found   bool
				Version chunkstore.TransferVersion
				Config  chunkstore.MetricConfig
			}{s.proofEpoch, out[i].Found, chunkstore.TransferVersion{Name: name, ID: m.ID, Generation: m.Generation, Revision: m.Revision}, m.MetricConfig})
			if err != nil {
				return err
			}
			proof := sha256.Sum256(data)
			out[i].Proof = hex.EncodeToString(proof[:])
			return nil
		}
		if r.Mode == "sum-v1" {
			archives, err := s.store.SumDigest(ctx, name, int(r.From), int(r.Until), r.Step)
			if errors.Is(err, chunkstore.ErrNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("sum digest %s: %w", name, err)
			}
			out[i].Found = true
			for _, a := range archives {
				out[i].Archives = append(out[i].Archives, DigestArchive{Step: a.Step, Points: a.Points, Count: int(a.Count), Hash: a.Sum, From: a.From, Until: a.Until})
			}
			return nil
		}
		// Fetch starts one step after its from time (whisper intervals), so
		// ask from From-1 to keep a point at From; digestSeries drops points
		// outside [From, Until).
		series, err := s.store.Fetch(ctx, name, int(r.From)-1, int(r.Until))
		if errors.Is(err, chunkstore.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("digest %s: %w", name, err)
		}
		out[i].Found = true
		if series != nil && series.Step > 0 {
			out[i].Archives = []DigestArchive{digestSeries(series, r.From, r.Until)}
		}
		return nil
	})
	return out, err
}

// digestScratch is per-call hashing state. The hasher and its batch buffer are
// reused because survivors serve thousands of digests per second.
type digestScratch struct {
	hash hash.Hash
	buf  [digestBatchPoints * 16]byte
}

const (
	digestBatchPoints = 256
	// float64 exponent bits; all set means NaN or an infinity.
	digestNonFinite = 0x7ff0000000000000
)

var digestScratchPool = sync.Pool{New: func() any { return &digestScratch{hash: sha256.New()} }}

// digestSeries hashes the finite points of series inside [from, until). It
// visits only the slots inside the window and hashes them in batches; the bytes
// fed to sha256 are the same (timestamp, float64 bits) pairs in slot order.
func digestSeries(series *chunkstore.Series, from, until int64) DigestArchive {
	d := DigestArchive{Step: series.Step}
	for _, retention := range series.Metadata.Retentions {
		if retention.Step == series.Step {
			d.Points = retention.Count
			break
		}
	}
	scratch := digestScratchPool.Get().(*digestScratch)
	defer digestScratchPool.Put(scratch)
	h, buf := scratch.hash, scratch.buf[:]
	h.Reset()
	step, first := int64(series.Step), int64(series.FromTime)
	lo, hi := digestSlots(first, step, from, until, len(series.Values))
	n := 0
	for i, v := range series.Values[lo:hi] {
		bits := math.Float64bits(v)
		if bits&digestNonFinite == digestNonFinite {
			continue
		}
		binary.BigEndian.PutUint64(buf[n:], uint64(first+int64(lo+i)*step))
		binary.BigEndian.PutUint64(buf[n+8:], bits)
		d.Count++
		if n += 16; n == len(buf) {
			_, _ = h.Write(buf)
			n = 0
		}
	}
	_, _ = h.Write(buf[:n])
	var sum [sha256.Size]byte
	d.Hash = hex.EncodeToString(h.Sum(sum[:0])[:8])
	return d
}

// digestSlots returns the slot range [lo, hi) whose timestamps
// first+i*step fall inside [from, until), clamped to [0, n). step > 0.
func digestSlots(first, step, from, until int64, n int) (int, int) {
	slot := func(t int64) int {
		// ceil((t-first)/step) for either sign, clamped to [0, n].
		d := t - first
		q := d / step
		if d%step > 0 {
			q++
		}
		return int(max(0, min(q, int64(n))))
	}
	lo, hi := slot(from), slot(until)
	return lo, max(lo, hi)
}
