package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/cockroachdb/pebble"
)

// ContinuationDigestPrefix reserves disposable, generation-local digest keys.
// These keys never cross store-copy transfers or contribute to source receipts.
const ContinuationDigestPrefix = "ds"

// PointSum is a decomposable fingerprint of finite, canonical points. It is a
// probabilistic comparison, not a cryptographic integrity or authenticity proof.
type PointSum struct {
	Sum   uint64
	Count uint64
}

func (d *PointSum) Add(timestamp int64, value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	// Two independent SplitMix64 avalanches preserve every float bit, including -0.
	mix := func(x uint64) uint64 {
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		return x ^ x>>31
	}
	d.Sum += mix(mix(uint64(timestamp)+0x9e3779b97f4a7c15) ^ bits.RotateLeft64(math.Float64bits(value), 29))
	d.Count++
}
func (d *PointSum) Merge(other PointSum) { d.Sum += other.Sum; d.Count += other.Count }

// SumArchive covers one archive's canonical [From,Until) view, clipped to its
// retention at a single clock instant. Sum is hex to preserve all bits in JSON.
type SumArchive struct {
	Archive, Step, Points int
	From, Until           int
	Sum                   string
	Count                 uint64
}

type ContinuationSumDigester interface {
	SumDigest(context.Context, ContinuationIO, Metadata, int, int, func() time.Time, ...int) ([]SumArchive, error)
}

// digestCacheIO collects disposable cache writes separately from source state.
// A per-metric revision fence validates the entries; publication rechecks the
// snapshot revision under the writer stripe. No cache state changes source data.
type digestCacheIO struct {
	*continuationIO
	pending map[string][]byte
	reset   bool
}

func (io *digestCacheIO) ClearDigestCache() { io.reset = true }

func (io *digestCacheIO) CacheDigest(key, value []byte) {
	io.pending[string(key)] = append([]byte(nil), value...)
}

// SumDigest reads all archives, unlike Fetch's age-selected serving archive.
// Zero bounds request full retained history. No source points are changed.
func (s *Store) SumDigest(ctx context.Context, name string, from, until int, steps ...int) ([]SumArchive, error) {
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if err := s.fileOwnershipError(); err != nil {
		return nil, err
	}
	release, err := s.acquireReadSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	view := s.db.NewReadView()
	defer view.Close()
	m, err := metadataFrom(view, name)
	if err != nil {
		return nil, err
	}
	if err := s.checkFileOwnership(m); err != nil {
		return nil, err
	}
	if !m.Continuation {
		return s.sumOrdinary(ctx, view, m, from, until, steps...)
	}
	codec, ok := s.continuation.(ContinuationSumDigester)
	if !ok {
		return nil, errors.New("sum digest unavailable for continuation codec")
	}
	io := &digestCacheIO{continuationIO: s.newContinuationIO(ctx, view, nil, m).withIterator(), pending: make(map[string][]byte)}
	defer io.Close()
	at := s.now()
	result, err := codec.SumDigest(ctx, io, m, from, until, func() time.Time { return at }, steps...)
	if err != nil {
		return nil, err
	}
	if len(io.pending) != 0 || io.reset {
		// Avoid holding a writer stripe during the expensive first read.
		unlock := s.lockMetricMutation(name)
		defer unlock()
		current, err := metadataFrom(s.db, name)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil && current.ID == m.ID && current.Generation == m.Generation && current.Revision == m.Revision {
			batch := s.db.NewIndexedBatch()
			defer batch.Close()
			if io.reset {
				writer := s.newContinuationIO(ctx, batch, batch, m)
				if err := writer.DeletePrefix([]byte(ContinuationDigestPrefix)); err != nil {
					return nil, err
				}
			}
			for key, value := range io.pending {
				if err := batch.Set(io.key([]byte(key)), value, nil); err != nil {
					return nil, err
				}
			}
			if err := batch.Commit(pebble.NoSync); err != nil {
				return nil, fmt.Errorf("digest cache: %w", err)
			}
		}
	}
	return result, nil
}

// DigestBounds is shared with codecs so full-history and ranged reads use the
// same archive retention and half-open alignment. Empty bounds are permitted.
func DigestBounds(r Retention, now, from, until int) (int, int) {
	if from == 0 {
		from = now - r.MaxRetention() + 1
	}
	if until == 0 {
		until = now + 1
	}
	from = max(from, now-r.MaxRetention()+1, 0)
	until = min(until, now+1)
	ceil := func(v int) int { return (v + r.Step - 1) / r.Step * r.Step }
	return ceil(from), ceil(until)
}
func (s *Store) sumOrdinary(ctx context.Context, view pebble.Reader, m Metadata, from, until int, steps ...int) ([]SumArchive, error) {
	out := make([]SumArchive, 0, len(m.Retentions))
	now := int(s.now().Unix())
	for ai, r := range m.Retentions {
		if len(steps) > 0 && steps[0] > 0 && r.Step != steps[0] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lo, hi := DigestBounds(r, now, from, until)
		var d PointSum
		if hi > lo {
			values, err := getRange(view, m, ai, lo, hi)
			if err != nil {
				return nil, err
			}
			for i, v := range values {
				d.Add(int64(lo+i*r.Step), v)
			}
		}
		out = append(out, SumArchive{ai, r.Step, r.Count, lo, hi, fmt.Sprintf("%016x", d.Sum), d.Count})
	}
	return out, nil
}
