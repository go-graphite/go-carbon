package v2backend

import (
	"slices"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
)

// Only the seal goroutine uses these buffers. Nothing in a published view may
// point into them: SST writers copy bytes before reset, and reducers own plans.
type frameSpan struct{ start, end, count int }
type sealScratch struct {
	spans  map[string]frameSpan
	frames []frame
	bytes  sealBytes
}

func (s *sealScratch) group(fs []frame) map[string]frameSpan {
	if s.spans == nil {
		s.spans = make(map[string]frameSpan)
	}
	clear(s.spans)
	for _, f := range fs {
		v := s.spans[f.name]
		v.count++
		s.spans[f.name] = v
	}
	offset := 0
	for name, v := range s.spans {
		v.start = offset
		v.end = offset
		offset += v.count
		s.spans[name] = v
	}
	s.frames = slices.Grow(s.frames[:0], len(fs))[:len(fs)]
	for _, f := range fs {
		v := s.spans[f.name]
		s.frames[v.end] = f
		v.end++
		s.spans[f.name] = v
	}
	return s.spans
}
func (s *sealScratch) reset() { clear(s.frames); clear(s.spans); s.bytes.reset() }

// Fixed size chunks keep previously returned slices stable when the arena grows.
// An unusually large key uses its own chunk. All retained capacity is bounded
// by the owner's already bounded frozen head, rather than an unbounded pool.
type sealBytes struct {
	chunks      [][]byte
	chunk, used int
}

func (a *sealBytes) copy(b []byte) []byte {
	for a.chunk < len(a.chunks) && len(a.chunks[a.chunk])-a.used < len(b) {
		a.chunk++
		a.used = 0
	}
	if a.chunk == len(a.chunks) {
		a.chunks = append(a.chunks, make([]byte, max(64<<10, len(b))))
	}
	out := a.chunks[a.chunk][a.used : a.used+len(b) : a.used+len(b)]
	copy(out, b)
	a.used += len(b)
	return out
}
func (a *sealBytes) reset() { a.chunk = 0; a.used = 0 }
func samePolicy(a, b daemonstore.Metric) bool {
	return a.AggregationMethod == b.AggregationMethod && a.XFilesFactor == b.XFilesFactor && slices.Equal(a.Archives, b.Archives)
}
