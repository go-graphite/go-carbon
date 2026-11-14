package whisperio

import (
	"context"
	"sync"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type snapshotScratch struct {
	archives [][]store.Point
	block    []whisper.TimeSeriesPoint
}

var snapshotPool = sync.Pool{New: func() any { return &snapshotScratch{} }}

func (s *snapshotScratch) buffer(i, size int) []store.Point {
	if s == nil {
		return make([]store.Point, size)
	}
	for len(s.archives) <= i {
		s.archives = append(s.archives, nil)
	}
	if cap(s.archives[i]) < size {
		s.archives[i] = make([]store.Point, size)
	}
	return s.archives[i][:size]
}

func (s *snapshotScratch) keep(i int, points []store.Point) {
	if s != nil {
		s.archives[i] = points
	}
}

func (s *snapshotScratch) release() {
	// Bound retention across all archives, not by advertised ring capacities.
	remaining := 1 << 20 // 16MiB of pointer-free points per lease.
	if cap(s.block) > remaining {
		s.block = nil
	} else {
		remaining -= cap(s.block)
	}
	for i, p := range s.archives {
		if cap(p) > remaining {
			s.archives[i] = nil
		} else {
			remaining -= cap(p)
		}
	}
	snapshotPool.Put(s)
}

func (c *ContinuationCodec) LeaseSnapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, func(), error) {
	// Keep the independently allocating reader available as a test oracle.
	if c.keepClassicSnapshotReads || c.keepSnapshotReadAllocations {
		snapshot, err := c.Snapshot(ctx, kv, m, clock)
		return snapshot, nil, err
	}
	scratch := snapshotPool.Get().(*snapshotScratch)
	snapshot, err := c.snapshot(ctx, kv, m, clock, scratch)
	if err != nil {
		scratch.release()
		return store.Snapshot{}, nil, err
	}
	return snapshot, scratch.release, nil
}

var _ store.ContinuationSnapshotLeaser = (*ContinuationCodec)(nil)
