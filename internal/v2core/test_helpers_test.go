package v2core

import (
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Engine helpers that only tests call.

// The optional hook owns its snapshot and receives a detached immutable head.
// It runs on the owner; callers must bound retained bytes and release snapshots.
func openCoreWithHook(root string, cfg coreConfig, hook func(int, *pebble.Snapshot, []row) error) (*coreEngine, error) {
	return openCoreWithHooks(root, cfg, coreHooks{Publish: hook})
}

// globalFrontier is conservative across shards and producers. Call only while
// opening, before starting producers, or after a completed seal barrier.
func (e *coreEngine) globalFrontier() []uint64 {
	out := slices.Clone(e.owners[0].meta.Cursors)
	for _, o := range e.owners[1:] {
		for p, c := range o.meta.Cursors {
			out[p] = min(out[p], c)
		}
	}
	return out
}

func (p *publication) acquire() *pinned {
	for {
		n := p.current.Load()
		if n == nil {
			return nil
		}
		for r := n.refs.Load(); r > 0; r = n.refs.Load() {
			if n.refs.CompareAndSwap(r, r+1) {
				return n
			}
		}
	}
}

func (p *pendingRows) len() int { return len(p.rows) }

// pendingBatchesFit reports whether every application has 1..pendingPacketRows
// points, the range Outbox.Start accepts.
func pendingBatchesFit(batches []coreBatch) bool {
	for i := range batches {
		if n := len(batches[i].Points); n == 0 || n > pendingPacketRows {
			return false
		}
	}
	return len(batches) > 0
}

// batches rebuilds the original applications exactly, with their
// points in one arena so each batch's Points has cap == len, as submission
// produces them.
func (p *pendingRows) batches(lookup func(uint64) *ApplicationPolicy) []coreBatch {
	return p.decodeFrom(lookup)
}

func (o *coreOwner) selectMerge(snapshot *pebble.Snapshot) ([]v2part.Descriptor, error) {
	limit, _, err := effectiveMergeLimit(o.dir)
	if err != nil {
		return nil, err
	}
	return o.selectMergeLimit(snapshot, limit, false)
}
