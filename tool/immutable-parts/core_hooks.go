package main

import (
	"bytes"
	"errors"
	"slices"

	"github.com/cockroachdb/pebble"
)

// coreBatch retains an original apply boundary. Policy is an opaque, durable
// policy description supplied by the caller, never inferred from point times.
// Order and Clock are the original apply order and ingest clock. Every point
// belongs to ID. The numeric ID remains an offline experiment.
type coreBatch struct {
	ID     uint64
	Order  uint64
	Clock  int64
	Policy []byte
	Points []row
}
type coreSealInput struct {
	Compact *compactRollupSelection
	Rows    []row
	Batches []coreBatch
	Next    coreFrontier
}

// coreEncodedSeal borrows an immutable encoded head until the packet ACK.
// Plan runs only on its owner and returns detached durable values.
type coreEncodedSeal struct {
	Points int
	Plan   func(coreFrontier) (coreSealPlan, error)
}
type coreKV struct{ Key, Value []byte }
type coreSealPlan struct {
	// Each file is nonempty, strictly sorted, and disjoint from other files.
	// Prefixes 0..2 and ff are reserved for raw data and the owner frontier.
	Files [][]coreKV
	// Commit publishes staged owner-local state after durable atomic ingestion.
	// It cannot fail and must not retain mutable input buffers.
	Commit func()
}
type coreHooks struct {
	OwnerCPUs []int
	Publish   func(int, *pebble.Snapshot, []row) error
	// PlanSeal is pure: no publication/mutation until Commit. Input buffers are
	// owner-local and valid only for this call. The planner must detach retained
	// output. An error prevents raw data, archives and frontier from committing.
	PlanSeal func(int, coreSealInput) (coreSealPlan, error)
}

func validateCoreSealPlan(p coreSealPlan) error {
	var last []byte
	for _, file := range p.Files {
		if len(file) == 0 {
			return errors.New("empty seal extension file")
		}
		for i, kv := range file {
			if len(kv.Key) == 0 || kv.Key[0] < 3 || kv.Key[0] == 255 {
				return errors.New("reserved seal extension key")
			}
			if i > 0 && bytes.Compare(file[i-1].Key, kv.Key) >= 0 {
				return errors.New("seal extension keys not strictly sorted")
			}
		}
		if last != nil && bytes.Compare(last, file[0].Key) >= 0 {
			return errors.New("seal extension file ranges overlap or are unordered")
		}
		last = file[len(file)-1].Key
	}
	return nil
}

// submitBatches preserves metric grouping, policy bytes, clocks and ordering.
// Each producer calls serially, as for submit. No implicit regrouping is safe.
func (e *coreEngine) submitBatches(producer int, batches []coreBatch, cursor uint64, seal bool) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return errors.New("invalid batch producer/cursor")
	}
	var rows []row
	for _, b := range batches {
		for _, r := range b.Points {
			if r.ID != b.ID {
				return errors.New("mixed metric apply batch")
			}
			rows = append(rows, r)
		}
	}
	return e.submitPacket(producer, rows, batches, cursor, seal)
}
func (o *coreOwner) publishFrontier(f coreFrontier) {
	f.Cursors = slices.Clone(f.Cursors)
	o.frontier.Store(&f)
}

// durableFrontiers is safe during intake. Each vector is an atomic committed
// owner snapshot. Their minimum is conservative even across concurrent seals.
// A caller must also obey its journal's fsync frontier before retiring logs.
func (e *coreEngine) durableFrontiers() [][]uint64 {
	out := make([][]uint64, len(e.owners))
	for i, o := range e.owners {
		out[i] = slices.Clone(o.frontier.Load().Cursors)
	}
	return out
}

// An empty affinity list leaves scheduling to the runtime. Explicit lists are
// benchmark options and never change persisted data layout or routing.
func coreAffinity(cpus []int, index int) (func(), error) {
	if len(cpus) == 0 {
		return func() {}, nil
	}
	return pinCoreThread(cpus[index%len(cpus)])
}
