package mixedread

import (
	"context"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type latencyByteKey struct{}
type latencyBytes struct{ KV, Decoded, Calls uint64 }

// These wrappers exist only for a separate byte diagnostic. Latency gate
// binaries use the original codec unless GO_CARBON_MIXED_BYTES is enabled.
type latencyByteCodec struct{ *whisperio.ContinuationCodec }

func (c *latencyByteCodec) FetchWindowInto(ctx context.Context, kv chunkstore.ContinuationIO, m chunkstore.Metadata, archive, from, until int, dst []float64, covered []bool, now func() time.Time) error {
	counter, _ := ctx.Value(latencyByteKey{}).(*latencyBytes)
	if counter == nil {
		return c.ContinuationCodec.FetchWindowInto(ctx, kv, m, archive, from, until, dst, covered, now)
	}
	counter.Calls++
	for i := range dst {
		if covered == nil || !covered[i] {
			counter.Decoded += 8
		}
	}
	return c.ContinuationCodec.FetchWindowInto(ctx, &latencyByteIO{ContinuationIO: kv, counter: counter}, m, archive, from, until, dst, covered, now)
}

func (c *latencyByteCodec) Fetch(ctx context.Context, kv chunkstore.ContinuationIO, m chunkstore.Metadata, from, until int, now func() time.Time) (*chunkstore.Series, error) {
	counter, _ := ctx.Value(latencyByteKey{}).(*latencyBytes)
	if counter == nil {
		return c.ContinuationCodec.Fetch(ctx, kv, m, from, until, now)
	}
	counter.Calls++
	got, err := c.ContinuationCodec.Fetch(ctx, &latencyByteIO{ContinuationIO: kv, counter: counter}, m, from, until, now)
	if got != nil {
		counter.Decoded += uint64(len(got.Values)) * 8
	}
	return got, err
}

type latencyByteIO struct {
	chunkstore.ContinuationIO
	counter *latencyBytes
}

func (c *latencyByteIO) Get(k []byte) ([]byte, error) {
	v, e := c.ContinuationIO.Get(k)
	c.counter.KV += uint64(len(v))
	return v, e
}
func (c *latencyByteIO) Probe(k []byte) ([]byte, error) {
	v, e := c.ContinuationIO.(chunkstore.ContinuationProber).Probe(k)
	c.counter.KV += uint64(len(v))
	return v, e
}
func (c *latencyByteIO) visit(fn func([]byte, []byte) error) func([]byte, []byte) error {
	return func(k, v []byte) error { c.counter.KV += uint64(len(v)); return fn(k, v) }
}
func (c *latencyByteIO) Visit(k []byte, fn func([]byte, []byte) error) error {
	return c.ContinuationIO.Visit(k, c.visit(fn))
}
func (c *latencyByteIO) VisitRange(a, b []byte, fn func([]byte, []byte) error) error {
	return c.ContinuationIO.(chunkstore.ContinuationRangeReader).VisitRange(a, b, c.visit(fn))
}
func (c *latencyByteIO) VisitPage(p, s []byte, n int, fn func([]byte, []byte) error) (bool, error) {
	return c.ContinuationIO.(chunkstore.ContinuationPageReader).VisitPage(p, s, n, c.visit(fn))
}
func (c *latencyByteIO) TimeBuckets() bool {
	return c.ContinuationIO.(chunkstore.ContinuationTimeBuckets).TimeBuckets()
}
func (c *latencyByteIO) ContinuationRevision() uint64 {
	return c.ContinuationIO.(chunkstore.ContinuationRevision).ContinuationRevision()
}
