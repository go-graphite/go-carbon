package v2core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// RollupPolicy binds an immutable schema to its portable canonical identity.
// It is persisted in the producer catalog before journal references are synced.
type RollupPolicy struct {
	Name   partsformat.ID
	Group  partsformat.Group
	Config sealrollup.Config
}

func EncodeRollupPolicy(p RollupPolicy) ([]byte, error) {
	if p.Name == "" || p.Group == "" {
		return nil, errors.New("rollup identity/group required")
	}
	if _, _, err := partsformat.GroupRange(p.Group); err != nil {
		return nil, err
	}
	if len(p.Name) > partsformat.MaxNameBytes {
		return nil, errors.New("rollup name too long")
	}
	if err := p.Config.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// RollupMetadata accompanies archive replacements in the same atomic ingest.
// LastUpdate is the original admission clock in nanoseconds, clamped against
// backward clocks just as chunkstore activity is. Revision starts at creation.
type RollupMetadata struct {
	ClosedThrough int64 `json:",omitempty"`
	// BaselineOrder certifies original classic slots installed before intake.
	BaselineOrder uint64 `json:",omitempty"`

	// ResetOrder masks every older physical slot in all parts, including copies.
	// The producer order frontier is retained across expiry and restart.
	ResetOrder uint64 `json:",omitempty"`
	Expired    bool   `json:",omitempty"`

	Version    uint8
	ID         uint64
	Policy     []byte
	Frontier   sealrollup.Frontier
	LastUpdate int64
	Revision   uint64
}

type rollupMetric struct {
	*PolicyIdentity
	meta   metricMetadata
	slots  sealrollup.Slots
	vshard uint16
	// rank is 1 + this metric's index in rankOwner.dense, the canonical
	// (group, vshard, length, name) order. Adoption shares metrics between
	// planners, so a rank is valid only for its owner; 0 means unranked.
	rank      uint32
	rankOwner *coreRollups
}
type rollupCoverage struct {
	spec partsformat.Spec
	seen map[[32]byte]bool
}
type coreRollups struct {
	db          *pebble.DB // nil only for standalone, non-durable planner fixtures
	identities  *PolicyCatalog
	lanes       []rollupPlannerLane
	parallelism int
	expire      func(RollupPolicy, RollupMetadata, time.Time) bool
	now         func() time.Time

	stage   func(string)
	metrics map[uint64]*rollupMetric
	// closable is the subset of metrics with a coarse archive. Only these can
	// become closure-due; single-archive names never enter idle scans.
	closable       map[uint64]*rollupMetric
	names          map[partsformat.ID]uint64
	stages         []rollupStage
	positions      map[uint64]int
	batchPositions []int
	pendingNames   map[partsformat.ID]uint64
	samples        []sealrollup.Sample
	batches        []sealrollup.Batch
	files          map[string][][]plannedKV
	arena          []byte
	scratch        *rollupScratch // pooled working set held from planParts to Commit/Abort
	pending        bool
	definitions    map[[32]byte][]partsformat.ArchivePolicy
	coverage       map[string]*rollupCoverage

	// dense holds every ranked installed metric in canonical order, so a seal
	// sorts stages by uint32 rank instead of name bytes. unranked metrics were
	// installed since the last renumber; replaced marks IDs whose metric
	// changed (adoption) so the next renumber drops the stale entry.
	dense      []*rollupMetric
	denseSpare []*rollupMetric
	unranked   []*rollupMetric
	replaced   bool
}

// rankSlack bounds how many installed-but-unranked metrics a planner carries
// relative to its dense slice before one sequential renumber. Unranked stages
// stay exact (binary search plus name tie-break), so this only trades the
// renumber pass (one touch per name) against per-stage search cost.
const rankSlack = 64

// Archive max-u32 is reserved within the core's GCP3 rollup experiment for the
// per-metric policy/frontier/activity envelope. Other archives contain canonical
// EncodeRecord values; BlockStart is the physical slot, Partition is expiry day.
// UUID precedes the metric, so each sealed version has a disjoint immutable key.
const rollupMetadataArchive = uint32(math.MaxUint32)

func newCoreRollups() *coreRollups {
	return &coreRollups{identities: &PolicyCatalog{}, metrics: map[uint64]*rollupMetric{}, closable: map[uint64]*rollupMetric{}, names: map[partsformat.ID]uint64{}}
}

// install is the only writer of metrics/closable, so the closure index can
// never miss a coarse-archive metric.
func (r *coreRollups) install(id uint64, m *rollupMetric) {
	old := r.metrics[id]
	r.metrics[id] = m
	if old != m {
		if old != nil || m.rankOwner == r {
			// The stale entry leaves dense at the next renumber; a metric
			// this planner ranked before re-enters unranked.
			r.replaced = true
			m.rank, m.rankOwner = 0, nil
		}
		r.unranked = append(r.unranked, m)
	}
	if len(m.schema().Archives) > 1 {
		r.closable[id] = m
	} else {
		delete(r.closable, id)
	}
}

// idleCandidates returns the metrics an idle pass must visit. An expiry
// callback can fire for any metric; without one only closure can be due.
func (r *coreRollups) idleCandidates() map[uint64]*rollupMetric {
	if r.expire != nil {
		return r.metrics
	}
	return r.closable
}

// needsMetadataSeal reports whether a not-yet-installed metric must seal before
// the next idle deadline: expiry needs its inactivity window anchored, and a
// coarse archive needs metadata before it can close. A single-archive name
// without expiry has neither, so it waits for the ordinary seal.
func (r *coreRollups) needsMetadataSeal(b *coreBatch) bool {
	if r.expire != nil || b.Registered == nil {
		return true
	}
	return len(b.Registered.schema().Archives) > 1
}
func (r *coreRollups) metric(id uint64, policy []byte) (*rollupMetric, error) {
	h, err := r.identities.intern(id, "", policy)
	if err != nil {
		return nil, err
	}
	return newRollupMetric(h), nil
}

func newRollupMetric(h *PolicyIdentity) *rollupMetric {
	return &rollupMetric{PolicyIdentity: h, meta: compactMetadata(RollupMetadata{Version: 1}), slots: *h.policy.prepared.NewSlots(), vshard: h.vshard}
}

type rollupStage struct {
	sortKey    uint64
	vshard     uint16
	metric     *rollupMetric
	next       RollupMetadata
	batches    []sealrollup.Batch
	records    []sealrollup.Record
	fresh      bool
	batchCount int
}

type rollupPlannerLane struct {
	windows   rollupWindowCache
	ranked    []rollupRankRef
	spare     []rollupRankRef
	counts    []int
	output    rollupOutput
	workspace sealrollup.Workspace
	records   []sealrollup.Record
	err       error
}

func (r *coreRollups) planParts(in coreSealInput) (coreSealPlan, error) {
	if r.pending {
		return coreSealPlan{}, errors.New("rollup plan still owns seal arenas")
	}
	r.pending = true
	r.takeScratch()
	returned := false
	defer func() {
		if !returned {
			r.release()
		}
	}()
	stages := r.stages[:0]
	if len(r.unranked) > 0 && len(r.unranked)*rankSlack >= len(r.dense) {
		r.renumber()
	}
	positions := r.positions
	if positions == nil {
		positions = map[uint64]int{}
		r.positions = positions
	}
	names := r.pendingNames
	if names == nil {
		names = map[partsformat.ID]uint64{}
		r.pendingNames = names
	}
	// One stage per distinct name, not per input application: a time-only
	// seal admits several applications per name, so sizing by rows would
	// reserve stages that are never used. Pooled capacity covers the names.
	r.batchPositions = slices.Grow(r.batchPositions[:0], len(in.Batches))[:len(in.Batches)]
	for bi, b := range in.Batches {
		i, ok := positions[b.ID]
		if !ok {
			m := r.metrics[b.ID]
			fresh := m == nil
			if fresh {
				if h := b.Registered; h != nil && h.catalog == r.identities && h.id == b.ID {
					m = newRollupMetric(h)
				} else {
					policy := b.Policy
					if h != nil {
						policy = h.definitionBytes()
					}
					var err error
					m, err = r.metric(b.ID, policy)
					if err != nil {
						return coreSealPlan{}, err
					}
				}
				if prior, ok := r.names[m.policy.Name]; ok && prior != b.ID {
					return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
				}
				if prior, ok := names[m.policy.Name]; ok && prior != b.ID {
					return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
				}
				names[m.policy.Name] = b.ID
			}
			stages = append(stages, rollupStage{metric: m, next: m.metadataScalars(m.meta), fresh: fresh})
			i = len(stages) - 1
			stages[i].next.Expired = false
			r.stageKey(&stages[i])
			positions[b.ID] = i
		}
		r.batchPositions[bi] = i
		s := &stages[i]
		if (b.Registered == nil || b.Registered != s.metric.PolicyIdentity) && !s.metric.definitionEqual(b.Policy) {
			return coreSealPlan{}, errors.New("stored rollup policy changed")
		}
		stages[i].batchCount++
	}
	// One allocation per sealed part for samples and application descriptors.
	// Every original boundary/order/clock remains distinct.
	sampleCount := 0
	for _, b := range in.Batches {
		sampleCount += len(b.Points)
	}
	r.samples = slices.Grow(r.samples[:0], sampleCount)[:sampleCount]
	samples := r.samples
	r.batches = slices.Grow(r.batches[:0], len(in.Batches))[:len(in.Batches)]
	batchStorage := r.batches
	offset := 0
	for i := range stages {
		n := stages[i].batchCount
		stages[i].batches = batchStorage[offset : offset : offset+n]
		offset += n
	}
	offset = 0
	for bi, b := range in.Batches {
		s := &stages[r.batchPositions[bi]]
		ps := samples[offset : offset+len(b.Points) : offset+len(b.Points)]
		for j, p := range b.Points {
			ps[j] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		offset += len(b.Points)
		s.batches = append(s.batches, sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: ps})
	}
	named := len(stages)
	// Idle closure uses the existing seal maintenance pass; no separate scheduler.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if r.expire != nil || r.db != nil {
		for id, m := range r.idleCandidates() {
			if _, seen := positions[id]; !seen && !m.meta.Expired && ((r.expire != nil && r.expire(m.rollupPolicy(), m.metadata(), now)) || rollupClosureDue(m, now.Unix())) {
				stages = append(stages, rollupStage{metric: m, next: m.metadata()})
				r.stageKey(&stages[len(stages)-1])
			}
		}
	}
	// Expiry callbacks retain their serial owner contract. Idle plans do not
	// advance order, so the persisted reset frontier is already known here.
	for i := range stages {
		s := &stages[i]
		m := s.metric
		if r.expire != nil && len(s.batches) == 0 && r.expire(m.rollupPolicy(), s.next, now) {
			// Older readers must reject reset authority rather than ignore its
			// fields and resurrect the covered slot history.
			s.next.Version = max(2, s.next.Version)
			s.next.Expired = true
			s.next.ResetOrder = s.next.Frontier.Order
			if s.next.Revision == math.MaxUint64 {
				return coreSealPlan{}, errors.New("metric revision exhausted")
			}
			s.next.Revision++
		}
	}
	var part partsformat.PartID
	if len(stages) > 0 {
		if _, err := rand.Read(part[:]); err != nil {
			return coreSealPlan{}, err
		}
	}
	out, err := r.planStages(stages, part)
	if err != nil {
		return coreSealPlan{}, err
	}
	r.stages = stages
	out.Names = named
	out.Abort = r.release
	out.Commit = func() {
		defer r.release()
		r.commitStages()
		for _, s := range stages {
			m := s.metric
			if s.fresh {
				r.install(m.id, m)
				r.names[m.policy.Name] = m.id
			}
		}
	}
	returned = true
	return out, nil
}

// ScanRollups reads one immutable core snapshot. It resolves each physical slot
// by logical timestamp, then class and source-local order, before time filters.
// The snapshot must include metadata and all archive SSTs from one publication.
func ScanRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	return scanRollups(snapshot, nil, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		if m.Version == 3 && !m.Expired {
			records = sealrollup.ProjectOpen(p.Config, m.Frontier.MaxTime, m.ClosedThrough, records)
		}
		return visit(p, m, records)
	})
}
func scanRollups(snapshot *pebble.Snapshot, identities *PolicyCatalog, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	if copied, err := snapshotHasCopiedParts(snapshot); err != nil {
		return err
	} else if copied {
		return scanCopiedRollups(snapshot, visit)
	}
	type entry struct {
		group     partsformat.Group
		identity  *PolicyIdentity
		meta      metricMetadata
		records   map[[2]int64]sealrollup.Record
		single    [1]sealrollup.Record
		hasSingle bool
	}
	series := map[partsformat.ID]*entry{}
	iter, err := newArchiveIter(context.Background(), snapshot, &pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 3}, UpperBound: partsformat.PrefixEnd([]byte{'G', 'C', 'P', 3})})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		k, err := partsformat.Decode(iter.Key())
		if err != nil {
			return err
		}
		e := series[k.Metric]
		if e == nil {
			e = &entry{group: k.Group}
			series[k.Metric] = e
		}
		if k.Ordinal != 0 || e.group != k.Group {
			return errors.New("unexpected rollup partition")
		}
		if k.Archive == rollupMetadataArchive {
			m, decodeErr := decodeRollupMetadataView(k.Metric, k.Group, iter.Value())
			if identities == nil {
				m.Policy = bytes.Clone(m.Policy)
			}
			if decodeErr != nil {
				err = decodeErr
				return err
			}
			if compactMetadata(m).after(e.meta) {
				var h *PolicyIdentity
				if identities != nil {
					h, err = identities.intern(m.ID, string(k.Metric), m.Policy)
				} else {
					var policy RollupPolicy
					if err = json.Unmarshal(m.Policy, &policy); err == nil {
						var prepared *sealrollup.Prepared
						prepared, err = sealrollup.Prepare(policy.Config)
						if err == nil {
							h = detachedPolicy(m.ID, policy, m.Policy, prepared)
						}
					}
				}
				if err != nil {
					return err
				}
				if h.policy.prepared.Digest() != m.Frontier.Policy {
					return errors.New("rollup policy/frontier mismatch")
				}
				e.identity, e.meta = h, compactMetadata(m)
			}
		} else {
			// Legacy singleton rows need no temporary decoded slice. Dense runs
			// retain their bounded decoder; slot replacement resolves once below.
			install := func(record sealrollup.Record) error {
				slot := [2]int64{int64(record.Archive), record.Slot}
				if e.records == nil {
					if !e.hasSingle {
						e.single[0], e.hasSingle = record, true
						return nil
					}
					old := e.single[0]
					if old.Archive == record.Archive && old.Slot == record.Slot {
						newer, err := newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
						if err != nil {
							return err
						}
						if newer {
							e.single[0] = record
						}
						return nil
					}
					e.records = map[[2]int64]sealrollup.Record{{int64(old.Archive), old.Slot}: old}
				}
				old, ok := e.records[slot]
				newer := !ok
				if ok {
					var err error
					newer, err = newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
					if err != nil {
						return err
					}
				}
				if newer {
					e.records[slot] = record
				}
				return nil
			}
			value := iter.Value()
			if len(value) == 26 && value[0] == 1 {
				record, err := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, value)
				if err != nil {
					return err
				}
				if err := install(record); err != nil {
					return err
				}
			} else {
				decoded, err := partsformat.DecodeSlots(k.Archive, k.BlockStart, value)
				if err != nil {
					return err
				}
				for _, record := range decoded {
					if err := install(record); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	for name, e := range series {
		if e.identity == nil {
			return fmt.Errorf("missing rollup metadata for %s", name)
		}
		policy, meta := e.identity.rollupPolicy(), e.identity.metadata(e.meta)
		if (meta.Version != 1 && meta.Version != 2 && meta.Version != 3) || policy.Group != e.group || policy.Name != name || meta.ID == 0 || meta.Frontier.Order == 0 {
			return fmt.Errorf("missing rollup metadata for %s", name)
		}
		if meta.BaselineOrder > meta.Frontier.Order {
			return errors.New("baseline exceeds rollup frontier")
		}
		if (meta.Version == 1 && (meta.ResetOrder != 0 || meta.Expired)) || meta.ResetOrder > meta.Frontier.Order || (meta.Expired && meta.ResetOrder != meta.Frontier.Order) {
			return errors.New("invalid rollup reset frontier")
		}
		var records []sealrollup.Record
		if e.records == nil {
			records = e.single[:0]
			if e.hasSingle {
				r := e.single[0]
				if r.Archive >= len(policy.Config.Archives) || r.Slot >= policy.Config.Archives[r.Archive].Count || r.Order > meta.Frontier.Order {
					return errors.New("invalid rollup slot/frontier")
				}
				if r.Order > meta.ResetOrder {
					records = append(records, r)
				}
			}
		} else {
			records = make([]sealrollup.Record, 0, len(e.records))
		}
		for slot, record := range e.records {
			if slot[0] >= int64(len(policy.Config.Archives)) || slot[1] >= policy.Config.Archives[slot[0]].Count || record.Order > meta.Frontier.Order {
				return errors.New("invalid rollup slot/frontier")
			}
			if record.Order > meta.ResetOrder {
				records = append(records, record)
			}
		}
		slices.SortFunc(records, func(a, b sealrollup.Record) int {
			if a.Archive < b.Archive {
				return -1
			}
			if a.Archive > b.Archive {
				return 1
			}
			if a.Slot < b.Slot {
				return -1
			}
			if a.Slot > b.Slot {
				return 1
			}
			return 0
		})
		if err = visit(policy, meta, records); err != nil {
			return err
		}
	}
	return nil
}
func scanCopiedRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	index, err := NewRollupSnapshot(context.Background(), snapshot, nil, 0)
	if err != nil {
		return err
	}
	for name, metric := range index.metrics {
		records, err := index.Records(context.Background(), name)
		if err != nil {
			return err
		}
		slices.SortFunc(records, func(a, b sealrollup.Record) int {
			if a.Archive != b.Archive {
				return a.Archive - b.Archive
			}
			if a.Slot < b.Slot {
				return -1
			}
			if a.Slot > b.Slot {
				return 1
			}
			return 0
		})
		if err = visit(metric.latest.rollupPolicy(), metric.latest.metadata(), records); err != nil {
			return err
		}
	}
	return nil
}

func recoverCoreRollups(db *pebble.DB, catalogs ...*PolicyCatalog) (*coreRollups, error) {
	r := newCoreRollups()
	r.db = db
	if len(catalogs) > 0 {
		r.identities = catalogs[0]
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	if found, err := snapshotHasCopiedParts(snapshot); err != nil {
		return nil, err
	} else if found {
		index, err := NewRollupSnapshot(context.Background(), snapshot, nil, 0)
		if err != nil {
			return nil, err
		}
		for _, m := range index.metrics {
			if index.moves[m] != nil && index.moves[m].copied && index.moves[m].authority == nil {
				return nil, errors.New("copied-part catalog requires live reducer and numeric identity adoption before writable open")
			}
		}
	}
	err := scanRollups(snapshot, r.identities, func(policy RollupPolicy, meta RollupMetadata, records []sealrollup.Record) error {
		m, err := r.metric(meta.ID, meta.Policy)
		if err != nil {
			return err
		}
		if r.metrics[meta.ID] != nil {
			return errors.New("duplicate recovered rollup ID")
		}
		meta.Policy = m.definitionBytes()
		m.meta = compactMetadata(meta)
		m.slots.KeepRecent(meta.Frontier.MaxTime)
		for _, record := range records {
			m.slots.InstallRecent(record.Delta)
		}
		m.slots.KeepRecent(meta.Frontier.MaxTime)
		r.install(meta.ID, m)
		// Keep the recovered reverse index on the same immutable name as the
		// metric. ScanRollups' decoded policy is temporary construction state.
		r.names[m.policy.Name] = meta.ID
		return nil
	})
	return r, err
}

// Match GCP3 key ordering without retaining a complete key for every name.
func compareRollupIdentity(a, b *rollupMetric) int {
	if len(a.policy.Group) != len(b.policy.Group) {
		return len(a.policy.Group) - len(b.policy.Group)
	}
	if c := bytes.Compare([]byte(a.policy.Group), []byte(b.policy.Group)); c != 0 {
		return c
	}
	if c := int(partsformat.Route(a.policy.Name)) - int(partsformat.Route(b.policy.Name)); c != 0 {
		return c
	}
	if len(a.policy.Name) < len(b.policy.Name) {
		return -1
	}
	if len(a.policy.Name) > len(b.policy.Name) {
		return 1
	}
	return bytes.Compare([]byte(a.policy.Name), []byte(b.policy.Name))
}
