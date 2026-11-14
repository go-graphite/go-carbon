package chunkstore

import (
	"context"
	"sync"
	"time"
)

// ContinuationSnapshotLeaser lends private mutable point arrays until release.
// The importer releases them only after encoding the result and its proof;
// ordinary Snapshot callers continue to receive independently owned arrays.
type ContinuationSnapshotLeaser interface {
	LeaseSnapshot(context.Context, ContinuationIO, Metadata, func() time.Time) (Snapshot, func(), error)
}

type transferSnapshotLeases []func()

func (l *transferSnapshotLeases) close() {
	for _, release := range *l {
		release()
	}
	*l = (*l)[:0]
}

func (l *transferSnapshotLeases) snapshot(ctx context.Context, codec ContinuationCodec, kv ContinuationIO, m Metadata, clock func() time.Time) (Snapshot, error) {
	if codec, ok := codec.(ContinuationSnapshotLeaser); ok {
		snapshot, release, err := codec.LeaseSnapshot(ctx, kv, m, clock)
		if release != nil {
			*l = append(*l, release)
		}
		return snapshot, err
	}
	return codec.Snapshot(ctx, kv, m, clock)
}

// Size by validated points, never by the sender's advertised ring capacity.
// Drop unusually large maps rather than retaining them for subsequent names.
var transferSlotPool = sync.Pool{New: func() any { return make(map[int64]struct{}) }}

func borrowTransferSlots() map[int64]struct{} {
	return transferSlotPool.Get().(map[int64]struct{})
}

func releaseTransferSlots(slots map[int64]struct{}) {
	if len(slots) > 1<<18 {
		return
	}
	clear(slots)
	transferSlotPool.Put(slots)
}

type transferUnionScratch struct{ archives [][]Point }

var transferUnionPool = sync.Pool{New: func() any { return &transferUnionScratch{} }}

func (s *transferUnionScratch) buffer(i int) []Point {
	for len(s.archives) <= i {
		s.archives = append(s.archives, nil)
	}
	return s.archives[i][:0]
}

func (s *transferUnionScratch) release() {
	remaining := 1 << 20
	for i, p := range s.archives {
		if cap(p) > remaining {
			s.archives[i] = nil
		} else {
			remaining -= cap(p)
		}
	}
	transferUnionPool.Put(s)
}
