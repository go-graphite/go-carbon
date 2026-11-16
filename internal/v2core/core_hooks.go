package v2core

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
	Rows    []row
	Batches []coreBatch
	Next    coreFrontier
}
type coreKV struct{ Key, Value []byte }
type coreSealPlan struct {
	// ReplaceRaw suppresses numeric sorting and raw output. The caller supplies
	// the authoritative replacement, catalogue and receipt keys in Files. A
	// nonempty input requires at least one file; empty cursor barriers need none.
	ReplaceRaw bool
	// Each file is nonempty, strictly sorted, and disjoint from other files.
	// Prefixes 0..2 are reserved for raw data. Prefix ff is reserved except
	// ff V 2: this checkpoint namespace sorts strictly before frontier ff f.
	Files [][]coreKV
	// Commit publishes staged owner-local state after durable atomic ingestion.
	// It cannot fail and must not retain mutable input buffers.
	Commit func()
	// Abort releases private planning resources if durable ingestion fails,
	// including validation or planner errors. It must not change durable state.
	// Exactly one of Commit or Abort is selected for a returned plan. Neither
	// callback may fail. A process crash can bypass both; replay is authoritative.
	Abort func()
}
type coreHooks struct {
	OwnerCPUs []int
	Publish   func(int, *pebble.Snapshot, []row) error
	// PublishBatches transfers an immutable snapshot and detached original pending
	// applications. The consumer owns snapshot.Close, including on errors.
	// Point and policy arenas are immutable and must never be modified.
	// The final argument identifies the sealed generation; pending-only updates
	// retain it. It runs on the owner; readers never call back into owner state.
	PublishBatches func(int, *pebble.Snapshot, []coreBatch, uint64) error
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
			if len(kv.Key) == 0 || kv.Key[0] < 3 || (kv.Key[0] == 255 && !bytes.HasPrefix(kv.Key, []byte{255, 'V', '2'})) {
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
	return e.submitBatchesMode(producer, batches, cursor, seal, true)
}

func (e *coreEngine) submitBatchesMode(producer int, batches []coreBatch, cursor uint64, seal, waitSeal bool) error {
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
	return e.submitPacketMode(producer, rows, batches, cursor, seal, waitSeal)
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
