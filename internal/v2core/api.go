// Package v2core provides the offline numeric immutable-part owner engine.
// Callers own the durable input journal and canonical numeric ID allocation.
// This experimental format is not the portable production parts format.
package v2core

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

// SealPlan stages additional SSTs and a nonfailing post-durability publication.
type SealPlan = coreSealPlan

// Hooks connects seal planning and immutable snapshot publication.
type Hooks = coreHooks

// Engine owns the shard workers. Each producer must append serially.
// Close must run after all producers stop.
type Engine = coreEngine

func Open(root string, cfg Config, hooks Hooks) (*Engine, error) {
	return openCoreWithHooks(root, cfg, hooks)
}

// Append copies rows before returning; cursor strictly increases per producer.
// A seal waits for durable ingestion in every owner.
func (e *coreEngine) Append(producer int, rows []Row, cursor uint64, seal bool) error {
	return e.submit(producer, rows, cursor, seal)
}

// AppendBatches also preserves the original application groups for seal hooks.
func (e *coreEngine) AppendBatches(producer int, batches []Batch, cursor uint64, seal bool) error {
	return e.submitBatches(producer, batches, cursor, seal)
}

// DurableFrontiers returns conservative owner snapshots safe during intake.
// Log retirement must also respect the journal's own fsync frontier.
func (e *coreEngine) DurableFrontiers() [][]uint64 { return e.durableFrontiers() }
func (e *coreEngine) Close() error                 { return e.close() }
