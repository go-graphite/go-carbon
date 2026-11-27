package v2core

import (
	"bytes"
	"errors"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
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
	// Registered carries an immutable definition across the application boundary.
	// Policy must be nil when Registered is supplied.
	Registered *ApplicationPolicy
	Points     []row
}

// rowID is the batch's head row ID: its packed handle's slot, else its ID.
func (b *coreBatch) rowID() uint64 {
	if h := b.Registered; h != nil && h.id == b.ID {
		return h.rowID()
	}
	return b.ID
}

type coreSealInput struct {
	Rows []row
	// Pending is the frozen pending view; the rollup planner reads its rows
	// in place. Batches carries the same applications decoded, for PlanSeal
	// hooks and callers without a view; the planner reads Pending when set.
	Pending PendingView
	Batches []coreBatch
	Next    coreFrontier
}
type coreKV struct {
	Key, Value []byte
	// Record is immutable planner-owned storage valid through Commit/Abort.
	// The shared writer packs it directly; metadata continues to use Value.
	Record *sealrollup.Record
	// Name optionally carries the planner's metric name, so writing a planned
	// part shares it rather than copying it out of Key. Empty means unknown.
	Name v2part.ID
}

func (kv coreKV) encodedValue() []byte {
	if kv.Record != nil {
		return sealrollup.EncodeRecord(*kv.Record)
	}
	return kv.Value
}

type coreSealPlan struct {
	// ReplaceRaw suppresses numeric sorting and raw output. The caller supplies
	// the authoritative replacement, catalogue and receipt keys in Files. A
	// nonempty input requires at least one file; empty cursor barriers need none.
	ReplaceRaw bool
	// Each file is nonempty and strictly sorted. Extension LSM files have
	// disjoint key ranges; external archive originals have distinct PartSpecs.
	// Prefixes 0..2 are reserved for raw data. Prefix ff is reserved except
	// ff V 2: this checkpoint namespace sorts strictly before frontier ff f.
	Files [][]coreKV
	// PartSpecs is optional trusted coverage prepared by the rollup planner.
	// It avoids reconstructing immutable policies from every metadata row.
	PartSpecs []v2part.Spec
	// Parts are rollup planner parts with trusted coverage, written from
	// planned tuples into separate immutable files and never LSM-ingested.
	Parts []plannedRollupPart
	// Names counts the point-bearing names in this seal, excluding idle
	// closures. It is telemetry only.
	Names int
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
	// Identities shares canonical journal metadata with the writer and reader.
	Identities *PolicyCatalog

	// PartSource identifies the durable original ordering stream for sealed parts.
	// Zero uses the persisted journal identity (or an offline incarnation).
	PartSource v2part.Provenance

	// DenseRollups is retained for source compatibility; all rollup seals are dense.
	DenseRollups bool

	// IDKeys makes seals write ID-keyed (GCI2) parts through the store name
	// dictionary under <root>/dict. It needs v2part.IDKeys on; parts already
	// written with name keys stay readable.
	IDKeys bool

	// Expire decides whether a name idle for its ExpiryTTL window expires now;
	// it is asked about no other name. It runs on the serial seal owner, after
	// admitted batches are applied, and must remain false until journal
	// replay finishes.
	Expire func(RollupPolicy, RollupMetadata, time.Time) bool
	// ExpiryTTL, required with Expire, is a name's expiry window at now
	// (<= 0: never) and when that answer may next change (zero: never).
	ExpiryTTL func(RollupPolicy, time.Time) (time.Duration, time.Time)
	Now       func() time.Time

	// Stage is an optional fault-injection observer for offline recovery tests.
	// It runs on owner goroutines; callers must serialize their own observer.
	Stage func(string)

	// SealTiming observes completed owner-local seal phases, including failed attempts.
	// Calls may overlap across owners; observers must synchronize themselves.
	// seal_total contains rollup_plan and publication; these are not additive quantiles.
	SealTiming func(phase string, elapsed time.Duration)

	// Visible observes original batches after successful reader publication.
	// Owners may call concurrently; batches are borrowed and can repeat.
	Visible func(owner int, batches []coreBatch)
	// VisiblePending is Visible over the pending view, without decoding
	// points. When set it replaces Visible.
	VisiblePending func(owner int, pending PendingView)

	Publish func(int, *pebble.Snapshot, []row) error
	// PublishBatches transfers an immutable snapshot and detached original pending
	// applications. The consumer owns snapshot.Close, including on errors.
	// Point and policy arenas are immutable and must never be modified.
	// The final argument identifies the sealed generation; pending-only updates
	// retain it. It runs on the owner; readers never call back into owner state.
	PublishBatches func(int, *pebble.Snapshot, []coreBatch, uint64) error
	// PublishBatchesWithInventory is the same transfer with the immutable binding
	// set captured atomically with snapshot. When set, it replaces PublishBatches.
	// snapshot is nil when the generation and inventory equal the previous
	// successful publication's: the consumer keeps the snapshot it holds.
	PublishBatchesWithInventory func(int, *pebble.Snapshot, []coreBatch, uint64, *RollupInventory) error
	// PublishPending is PublishBatchesWithInventory over the owner's
	// pointer-free pending view (name-once S5a-2). When set it replaces both
	// []Batch hooks, which otherwise receive a per-publication decode.
	PublishPending func(int, *pebble.Snapshot, PendingView, uint64, *RollupInventory) error
	// PlanSeal is pure: no publication/mutation until Commit. Input buffers are
	// owner-local and valid only for this call. The planner must detach retained
	// output. An error prevents raw data, archives and frontier from committing.
	PlanSeal func(int, coreSealInput) (coreSealPlan, error)
}

func validateCoreSealPlan(p coreSealPlan) error {
	if len(p.PartSpecs) != 0 && len(p.PartSpecs) != len(p.Files) {
		return errors.New("seal part specs/files mismatch")
	}
	// Planned parts are validated key by key by their writer; their scopes
	// must be complete, distinct and not shared with byte-path archive files.
	planned := map[v2part.Scope]bool{}
	for _, part := range p.Parts {
		empty := true
		for _, segment := range part.Segments {
			empty = empty && len(segment) == 0
		}
		if empty {
			return errors.New("empty planned rollup part")
		}
		if part.Spec.Encoding != v2part.DenseEncoding || part.Spec.Scope.VShard != v2part.GroupVShard {
			return errors.New("planned rollup part scope")
		}
		if planned[part.Spec.Scope] {
			return errors.New("duplicate seal archive scope")
		}
		planned[part.Spec.Scope] = true
	}
	if len(p.Parts) > 0 && len(p.Files) > 0 {
		return errors.New("planned rollup parts cannot share a seal with extension files")
	}
	// Archive originals are separate immutable files, never ingested as data
	// into the LSM. Their vshard ranges may interleave across time partitions;
	// distinct complete scopes establish disjointness instead of min/max keys.
	archives := len(p.PartSpecs) == len(p.Files)
	scopes := map[v2part.Scope]bool{}
	for i, spec := range p.PartSpecs {
		if spec.Encoding != v2part.DenseEncoding || spec.Scope.VShard != v2part.GroupVShard || !isRollupFile(p.Files[i]) {
			archives = false
			break
		}
		if scopes[spec.Scope] {
			return errors.New("duplicate seal archive scope")
		}
		scopes[spec.Scope] = true
	}
	if len(p.PartSpecs) == 0 && len(p.Files) > 1 {
		// Without planner specs, every GCP3 file is written by
		// WriteDenseRollupPart as one group-wide part whose scope is the first
		// key's group/partition/part; it is retained, never LSM-ingested.
		// Baseline bootstrap writes one such part per month partition, and
		// their vshard ranges interleave just like planned archive parts.
		archives = true
		for _, file := range p.Files {
			if !isRollupFile(file) {
				archives = false
				break
			}
			k, err := v2part.Decode(file[0].Key)
			if err != nil {
				return err
			}
			scope := v2part.Scope{Group: k.Group, VShard: v2part.GroupVShard, Partition: k.Partition, Part: k.Part}
			if scopes[scope] {
				return errors.New("duplicate seal archive scope")
			}
			scopes[scope] = true
		}
	}
	var last []byte
	for _, file := range p.Files {
		if len(file) == 0 {
			return errors.New("empty seal extension file")
		}
		for i, kv := range file {
			if kv.Record != nil && !isRollupFile(file) {
				return errors.New("typed rollup record requires GCP3 extension file")
			}
			if len(kv.Key) == 0 || kv.Key[0] < 3 || (kv.Key[0] == 255 && !bytes.HasPrefix(kv.Key, []byte{255, 'V', '2'})) {
				return errors.New("reserved seal extension key")
			}
			if i > 0 && bytes.Compare(file[i-1].Key, kv.Key) >= 0 {
				return errors.New("seal extension keys not strictly sorted")
			}
		}
		if !archives && last != nil && bytes.Compare(last, file[0].Key) >= 0 {
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
	router := &e.routers[producer]
	if e.cfg.Rollups && len(batches) > 0 {
		// Encode the applications into owner rows, as a producer's Outbox
		// does; Start refuses an application outside 1..32768 points.
		if router.outbox == nil {
			router.outbox = e.NewOutbox()
		}
		out := router.outbox
		for i := range batches {
			if err := e.addBatch(out, &batches[i]); err != nil {
				out.reset()
				return err
			}
		}
		err := e.submitPacketMode(producer, nil, nil, out, cursor, seal, waitSeal)
		if err != nil {
			out.reset()
		}
		return err
	}
	// Submission detaches every row before returning. Reuse producer-local
	// flattening storage instead of growing a new slice for every packet.
	n := 0
	for _, b := range batches {
		n += len(b.Points)
	}
	rows := slices.Grow(router.batchRows[:0], n)
	for _, b := range batches {
		for _, r := range b.Points {
			if r.ID != b.ID {
				return errors.New("mixed metric apply batch")
			}
			rows = append(rows, r)
		}
	}
	router.batchRows = rows
	return e.submitPacketMode(producer, rows, batches, nil, cursor, seal, waitSeal)
}

// addBatch interns one application's identity and adds it to out.
func (e *coreEngine) addBatch(out *Outbox, b *coreBatch) error {
	h := b.Registered
	if h == nil || h.catalog != e.identities || h.id != b.ID {
		policy := b.Policy
		if h != nil {
			if h.policy.policyDefinition == nil {
				return errors.New("uninitialized application policy")
			}
			if len(policy) != 0 {
				return errors.New("registered policy with mutable definition")
			}
			policy = h.definitionBytes()
		}
		var err error
		if h, err = e.identities.intern(b.ID, "", policy); err != nil {
			return err
		}
	} else if len(b.Policy) != 0 {
		return errors.New("registered policy with mutable definition")
	}
	if err := out.Start(h, b.Order, b.Clock, b.Points[0].Time, len(b.Points)); err != nil {
		return err
	}
	for _, r := range b.Points {
		if r.ID != b.ID {
			return errors.New("mixed metric apply batch")
		}
		out.Point(r.Time, r.Bits, r.Seq)
	}
	return nil
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
