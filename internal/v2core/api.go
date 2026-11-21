// Package v2core provides the offline numeric immutable-part owner engine.
// Callers own the durable input journal and canonical numeric ID allocation.
// This experimental format is not the portable production parts format.
package v2core

import (
	"errors"
	"math"
	"os"

	"github.com/cockroachdb/pebble"
)

// Row preserves value bits and original input sequence exactly.
type Row = row

// Config fixes the persisted routing and journal identity.
type Config = coreConfig

// Batch preserves one original apply boundary, policy and ingest clock.
type Batch = coreBatch

// Frontier is an owner's durable replay position.
type Frontier = coreFrontier

// SealInput contains borrowed owner-local input, valid during PlanSeal only.
type SealInput = coreSealInput

// KV is one extension record, ordered by key.
type KV = coreKV

// SealPlan stages additional or replacement SSTs, post-durability publication
// and pre-durability cleanup. Replacement callers own the persisted data format.
type SealPlan = coreSealPlan

// Hooks connects seal planning and immutable snapshot publication.
type Hooks = coreHooks

// Engine owns the shard workers. Each producer must append serially.
// Close must run after all producers stop.
type Engine = coreEngine

func Open(root string, cfg Config, hooks Hooks) (*Engine, error) {
	return openCoreWithHooks(root, cfg, hooks)
}

// OpenReadOnly opens immutable snapshots without rebuilding writable reducer
// state. All admission and direct database writes are refused. Close after
// releasing snapshots. The fixed-corpus raw/journal oracle is its first consumer.
func OpenReadOnly(root string, cfg Config) (*Engine, error) {
	return openCoreMode(root, cfg, Hooks{}, true)
}

// DBs borrows the engine-owned databases in owner order. A caller may recover
// or initialize its own namespaces before the first Append/AppendBatches call.
// After intake starts, publish writes only through PlanSeal. Never close these
// handles, replace their files or modify the engine's reserved raw/frontier keys.
// The returned slice is detached; handles remain valid only until Engine.Close.
func (e *coreEngine) DBs() []*pebble.DB {
	out := make([]*pebble.DB, len(e.owners))
	for i, o := range e.owners {
		out[i] = o.db
	}
	return out
}

// Append copies rows before returning; cursor strictly increases per producer.
// A seal waits for durable ingestion in every owner.
func (e *coreEngine) Append(producer int, rows []Row, cursor uint64, seal bool) error {
	return e.submit(producer, rows, cursor, seal)
}

// AppendPipelined copies rows into the same bounded owner queues as Append.
// A seal preserves the per-producer FIFO boundary but returns after admission,
// before durable core ingestion. Its dispatch credit returns only after the
// seal completes. Callers must fsync their input journal before acknowledging
// input; core durability is observed through DurableFrontiers, a synchronous
// Append seal, or Close. Owner failures are sticky and also returned by Close.
func (e *coreEngine) AppendPipelined(producer int, rows []Row, cursor uint64, seal bool) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return os.ErrInvalid
	}
	return e.submitPacketMode(producer, rows, nil, nil, cursor, seal, false)
}

// AppendBatches also preserves the original application groups for seal hooks.
func (e *coreEngine) AppendBatches(producer int, batches []Batch, cursor uint64, seal bool) error {
	return e.submitBatches(producer, batches, cursor, seal)
}

// AppendBatchesPipelined preserves the original applications with the bounded
// admission and durability semantics of AppendPipelined. A synchronous batch
// seal or Close fences prior pipelined applications in the same producer FIFO.
func (e *coreEngine) AppendBatchesPipelined(producer int, batches []Batch, cursor uint64, seal bool) error {
	return e.submitBatchesMode(producer, batches, cursor, seal, false)
}

// AppendOutboxPipelined submits an outbox with the semantics of
// AppendBatchesPipelined. On return the outbox is empty and holds the arrays
// the owners released, ready for the producer's next applications.
func (e *coreEngine) AppendOutboxPipelined(producer int, out *Outbox, cursor uint64, seal bool) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 || !e.cfg.Rollups || out == nil || out.catalog != e.identities {
		return errors.New("invalid outbox producer/cursor")
	}
	return e.submitPacketMode(producer, nil, nil, out, cursor, seal, false)
}

// ResumeMerges releases merges held by DeferMerges. It is idempotent.
func (e *coreEngine) ResumeMerges() {
	if e.mergeHold != nil && e.resumed.CompareAndSwap(false, true) {
		close(e.mergeHold)
	}
}

// DurableFrontiers returns conservative owner snapshots safe during intake.
// Log retirement must also respect the journal's own fsync frontier.
func (e *coreEngine) DurableFrontiers() [][]uint64 { return e.durableFrontiers() }

// DurableFrontier is the producer cursor every owner has durably committed:
// the minimum of its DurableFrontiers column, read without copying them.
func (e *coreEngine) DurableFrontier(producer int) uint64 {
	if len(e.owners) == 0 {
		return 0
	}
	durable := uint64(math.MaxUint64)
	for _, o := range e.owners {
		durable = min(durable, o.frontier.Load().Cursors[producer])
	}
	return durable
}

// BenchmarkStats samples atomic owner counters and Pebble metrics. Call while
// the experiment is open, before Close; it never reads mutable reducer state.
func (e *coreEngine) BenchmarkStats() map[string]any {
	var calls, nanos, maximum, parts, written uint64
	var files int64
	for _, o := range e.owners {
		calls += o.syncCalls.Load()
		nanos += o.syncNS.Load()
		maximum = max(maximum, o.syncMaxNS.Load())
		parts += o.parts.Load()
		written += o.bytes.Load()
		for _, level := range o.db.Metrics().Levels {
			files += level.NumFiles
		}
	}
	return map[string]any{"sst_sync_calls": calls, "sst_sync_seconds": float64(nanos) / 1e9, "sst_sync_max_seconds": float64(maximum) / 1e9, "sealed_parts": parts, "sst_bytes": written, "live_sst_files": files}
}

func (e *coreEngine) Close() error { return e.close() }
