package v2core

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// Sequence is the input order, independent of sorting or part publication.
type row struct {
	ID        uint64
	Time      int64
	Bits, Seq uint64
}

func less(a, b row) int {
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	if a.Time < b.Time {
		return -1
	}
	if a.Time > b.Time {
		return 1
	}
	if a.Seq < b.Seq {
		return -1
	}
	if a.Seq > b.Seq {
		return 1
	}
	return 0
}

// A publisher owns one reference. Acquisition only increments a nonzero count;
// the last release closes the Pebble snapshot, after all readers are done.
type pinned struct {
	s    *pebble.Snapshot
	refs atomic.Int64
}

func (p *pinned) release() {
	if p.refs.Add(-1) == 0 {
		p.s.Close()
	}
}

type publication struct{ current atomic.Pointer[pinned] }

func (p *publication) publish(s *pebble.Snapshot) {
	n := &pinned{s: s}
	n.refs.Store(1)
	if old := p.current.Swap(n); old != nil {
		old.release()
	}
}

func (p *publication) clear() {
	if n := p.current.Swap(nil); n != nil {
		n.release()
	}
}
