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
	registered  *ApplicationPolicy
	policy      RollupPolicy
	meta        RollupMetadata
	slots       *sealrollup.Slots
	prepared    *sealrollup.Prepared
	keyTemplate []byte
	prefix      string
	partOffset  int
	vshard      uint16
	ordinal     uint64
}
type rollupCoverage struct {
	spec partsformat.Spec
	seen map[[32]byte]bool
}
type coreRollups struct {
	lanes       []rollupPlannerLane
	parallelism int
	expire      func(RollupPolicy, RollupMetadata, time.Time) bool
	now         func() time.Time

	stage          func(string)
	metrics        map[uint64]*rollupMetric
	names          map[partsformat.ID]uint64
	prepared       map[[32]byte]*sealrollup.Prepared
	stages         []rollupStage
	ordered        []*rollupStage
	positions      map[uint64]int
	batchPositions []int
	ordinalsReady  bool
	pendingNames   map[partsformat.ID]uint64
	stagedPolicies map[[32]byte]*sealrollup.Prepared
	samples        []sealrollup.Sample
	batches        []sealrollup.Batch
	files          map[string][]coreKV
	arena          []byte
	pending        bool
	definitions    map[[32]byte][]partsformat.ArchivePolicy
	coverage       map[string]*rollupCoverage
}

// Archive max-u32 is reserved within the core's GCP3 rollup experiment for the
// per-metric policy/frontier/activity envelope. Other archives contain canonical
// EncodeRecord values; BlockStart is the physical slot, Partition is zero.
// UUID precedes the metric, so each sealed version has a disjoint immutable key.
const rollupMetadataArchive = uint32(math.MaxUint32)

func newCoreRollups() *coreRollups {
	return &coreRollups{metrics: map[uint64]*rollupMetric{}, names: map[partsformat.ID]uint64{}, prepared: map[[32]byte]*sealrollup.Prepared{}}
}
func (r *coreRollups) metric(id uint64, policy []byte, staged map[[32]byte]*sealrollup.Prepared) (*rollupMetric, error) {
	var p RollupPolicy
	if err := json.Unmarshal(policy, &p); err != nil {
		return nil, err
	}
	encoded, err := EncodeRollupPolicy(p)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(encoded, policy) {
		return nil, errors.New("noncanonical rollup policy")
	}
	digest, err := sealrollup.PolicyDigest(p.Config)
	if err != nil {
		return nil, err
	}
	prepared := r.prepared[digest]
	if prepared == nil {
		prepared = staged[digest]
	}
	if prepared == nil {
		prepared, err = sealrollup.Prepare(p.Config)
		if err != nil {
			return nil, err
		}
	}
	staged[digest] = prepared
	// Identity/routing is immutable. Validate and encode it once, then only
	// fill the part/archive/slot fields of each detached output key.
	template, err := partsformat.Encode(partsformat.Key{Group: p.Group, VShard: partsformat.Route(p.Name), Metric: p.Name})
	if err != nil {
		return nil, err
	}
	prefix, err := partsformat.PartitionPrefixForGroup(p.Group, partsformat.Route(p.Name), 0)
	if err != nil {
		return nil, err
	}
	return &rollupMetric{policy: p, meta: RollupMetadata{Version: 1, ID: id, Policy: bytes.Clone(policy)}, slots: sealrollup.NewSlots(p.Config.Archives), prepared: prepared, keyTemplate: template, prefix: string(prefix), partOffset: len(prefix), vshard: partsformat.Route(p.Name)}, nil
}

type rollupStage struct {
	registered *ApplicationPolicy
	metric     *rollupMetric
	next       RollupMetadata
	batches    []sealrollup.Batch
	records    []sealrollup.Record
	fresh      bool
	batchCount int
}

type rollupPlannerLane struct {
	ordered   []*rollupStage
	output    rollupOutput
	workspace sealrollup.Workspace
	records   []sealrollup.Record
	err       error
}

func (r *coreRollups) plan(in coreSealInput) (coreSealPlan, error) {
	if r.pending {
		return coreSealPlan{}, errors.New("rollup plan still owns seal arenas")
	}
	r.pending = true
	returned := false
	defer func() {
		if !returned {
			r.release()
		}
	}()
	stages := r.stages[:0]
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
	stagedPolicies := r.stagedPolicies
	if stagedPolicies == nil {
		stagedPolicies = map[[32]byte]*sealrollup.Prepared{}
		r.stagedPolicies = stagedPolicies
	}
	// At most one stage per input application. Retain capacity between seals.
	stages = slices.Grow(stages, len(in.Batches))
	r.stages = stages
	r.batchPositions = slices.Grow(r.batchPositions[:0], len(in.Batches))[:len(in.Batches)]
	freshCount := 0
	for bi, b := range in.Batches {
		i, ok := positions[b.ID]
		if !ok {
			m := r.metrics[b.ID]
			fresh := m == nil
			if fresh {
				var err error
				m, err = r.metric(b.ID, b.Policy, stagedPolicies)
				if err != nil {
					return coreSealPlan{}, err
				}
				m.registered = b.Registered
				if prior, ok := r.names[m.policy.Name]; ok && prior != b.ID {
					return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
				}
				if prior, ok := names[m.policy.Name]; ok && prior != b.ID {
					return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
				}
				names[m.policy.Name] = b.ID
				freshCount++
			}
			stages = append(stages, rollupStage{metric: m, next: m.meta, fresh: fresh})
			stages[len(stages)-1].next.Expired = false
			i = len(stages) - 1
			positions[b.ID] = i
		}
		r.batchPositions[bi] = i
		s := &stages[i]
		if (b.Registered == nil || b.Registered != s.metric.registered) && !bytes.Equal(b.Policy, s.metric.meta.Policy) {
			return coreSealPlan{}, errors.New("stored rollup policy changed")
		}
		if b.Registered != nil {
			s.registered = b.Registered
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
	// Canonical byte ordering is immutable. Derive its scalar ordinal only
	// when the catalog grows (or after recovery), rather than per seal.
	if freshCount != 0 || !r.ordinalsReady {
		metrics := make([]*rollupMetric, 0, len(r.metrics)+freshCount)
		for _, m := range r.metrics {
			metrics = append(metrics, m)
		}
		for i := range stages {
			if stages[i].fresh {
				metrics = append(metrics, stages[i].metric)
			}
		}
		slices.SortFunc(metrics, compareRollupMetrics)
		for i, m := range metrics {
			m.ordinal = uint64(i)
		}
		r.ordinalsReady = true
	}
	// Idle maintenance visits only live metadata; no point/store reads occur.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if r.expire != nil {
		for id, m := range r.metrics {
			if _, seen := positions[id]; !seen && !m.meta.Expired && r.expire(m.policy, m.meta, now) {
				stages = append(stages, rollupStage{metric: m, next: m.meta})
			}
		}
	}
	// Expiry callbacks retain their serial owner contract. Idle plans do not
	// advance order, so the persisted reset frontier is already known here.
	for i := range stages {
		s := &stages[i]
		m := s.metric
		if r.expire != nil && len(s.batches) == 0 && r.expire(m.policy, s.next, now) {
			// Older readers must reject reset authority rather than ignore its
			// fields and resurrect the covered slot history.
			s.next.Version = 2
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
	out.Abort = r.release
	out.Commit = func() {
		defer r.release()
		r.commitStages()
		for _, s := range stages {
			m := s.metric
			if s.fresh {
				r.metrics[m.meta.ID] = m
				r.names[m.policy.Name] = m.meta.ID
				r.prepared[m.meta.Frontier.Policy] = m.prepared
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
	if copied, err := snapshotHasCopiedParts(snapshot); err != nil {
		return err
	} else if copied {
		return scanCopiedRollups(snapshot, visit)
	}
	type entry struct {
		group   partsformat.Group
		policy  RollupPolicy
		meta    RollupMetadata
		records map[[2]int64]sealrollup.Record
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
			e = &entry{group: k.Group, records: map[[2]int64]sealrollup.Record{}}
			series[k.Metric] = e
		}
		if k.Partition != 0 || k.Ordinal != 0 || e.group != k.Group {
			return errors.New("unexpected rollup partition")
		}
		if k.Archive == rollupMetadataArchive {
			m, decodeErr := DecodeRollupMetadata(iter.Value())
			if decodeErr != nil {
				err = decodeErr
				return err
			}
			if m.Frontier.Order > e.meta.Frontier.Order || (m.Frontier.Order == e.meta.Frontier.Order && m.Revision > e.meta.Revision) {
				e.meta = m
				if err = json.Unmarshal(m.Policy, &e.policy); err != nil {
					return err
				}
			}
		} else {
			decoded, err := partsformat.DecodeSlots(k.Archive, k.BlockStart, iter.Value())
			if err != nil {
				return err
			}
			for _, record := range decoded {
				slot := [2]int64{int64(k.Archive), record.Slot}
				old, ok := e.records[slot]
				newer, err := newerOrigin(partOrigin{Class: LivePart}, record, partOrigin{Class: LivePart}, old)
				if err != nil {
					return err
				}
				if !ok || newer {
					e.records[slot] = record
				}
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	for name, e := range series {
		if (e.meta.Version != 1 && e.meta.Version != 2) || e.policy.Group != e.group || e.policy.Name != name || e.meta.ID == 0 || e.meta.Frontier.Order == 0 {
			return fmt.Errorf("missing rollup metadata for %s", name)
		}
		if e.meta.BaselineOrder > e.meta.Frontier.Order {
			return errors.New("baseline exceeds rollup frontier")
		}
		if (e.meta.Version == 1 && (e.meta.ResetOrder != 0 || e.meta.Expired)) || e.meta.ResetOrder > e.meta.Frontier.Order || (e.meta.Expired && e.meta.ResetOrder != e.meta.Frontier.Order) {
			return errors.New("invalid rollup reset frontier")
		}
		digest, err := sealrollup.PolicyDigest(e.policy.Config)
		if err != nil {
			return err
		}
		if digest != e.meta.Frontier.Policy {
			return errors.New("rollup policy/frontier mismatch")
		}
		records := make([]sealrollup.Record, 0, len(e.records))
		for slot, record := range e.records {
			if slot[0] >= int64(len(e.policy.Config.Archives)) || slot[1] >= e.policy.Config.Archives[slot[0]].Count || record.Order > e.meta.Frontier.Order {
				return errors.New("invalid rollup slot/frontier")
			}
			if record.Order > e.meta.ResetOrder {
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
		if err = visit(e.policy, e.meta, records); err != nil {
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
		if err = visit(metric.latest.policy, metric.latest.meta, records); err != nil {
			return err
		}
	}
	return nil
}

func recoverCoreRollups(db *pebble.DB) (*coreRollups, error) {
	r := newCoreRollups()
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
			if m.copied && m.authority == nil {
				return nil, errors.New("copied-part catalog requires live reducer and numeric identity adoption before writable open")
			}
		}
	}
	err := ScanRollups(snapshot, func(policy RollupPolicy, meta RollupMetadata, records []sealrollup.Record) error {
		m, err := r.metric(meta.ID, meta.Policy, r.prepared)
		if err != nil {
			return err
		}
		if r.metrics[meta.ID] != nil {
			return errors.New("duplicate recovered rollup ID")
		}
		m.meta = meta
		for _, record := range records {
			m.slots.Install(record.Delta)
		}
		r.metrics[meta.ID] = m
		// Keep the recovered reverse index on the same immutable name as the
		// metric. ScanRollups' decoded policy is temporary construction state.
		r.names[m.policy.Name] = meta.ID
		r.prepared[meta.Frontier.Policy] = m.prepared
		return nil
	})
	return r, err
}
