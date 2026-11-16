package v2core

import (
	"bytes"
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
	policy   RollupPolicy
	meta     RollupMetadata
	slots    *sealrollup.Slots
	prepared *sealrollup.Prepared
}
type coreRollups struct {
	dense bool

	expire func(RollupPolicy, RollupMetadata, time.Time) bool
	now    func() time.Time

	stage     func(string)
	metrics   map[uint64]*rollupMetric
	names     map[partsformat.ID]uint64
	prepared  map[[32]byte]*sealrollup.Prepared
	workspace sealrollup.Workspace
	records   []sealrollup.Record
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
	// Planning only populates its private policy cache; failure remains pure.
	return &rollupMetric{policy: p, meta: RollupMetadata{Version: 1, ID: id, Policy: bytes.Clone(policy)}, slots: sealrollup.NewSlots(p.Config.Archives), prepared: prepared}, nil
}

type rollupStage struct {
	metric  *rollupMetric
	next    RollupMetadata
	batches []sealrollup.Batch
	records []sealrollup.Record
	fresh   bool
}

func (r *coreRollups) plan(in coreSealInput) (coreSealPlan, error) {
	var stages []rollupStage
	positions := map[uint64]int{}
	names := map[partsformat.ID]uint64{}
	stagedPolicies := map[[32]byte]*sealrollup.Prepared{}
	for _, b := range in.Batches {
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
			}
			if prior, ok := r.names[m.policy.Name]; ok && prior != b.ID {
				return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
			}
			if prior, ok := names[m.policy.Name]; ok && prior != b.ID {
				return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
			}
			names[m.policy.Name] = b.ID
			stages = append(stages, rollupStage{metric: m, next: m.meta, fresh: fresh})
			stages[len(stages)-1].next.Expired = false
			i = len(stages) - 1
			positions[b.ID] = i
		}
		s := &stages[i]
		if !bytes.Equal(b.Policy, s.metric.meta.Policy) {
			return coreSealPlan{}, errors.New("stored rollup policy changed")
		}
		// Cursor replay filtering belongs to core's owner before PlanSeal. Never
		// silently suppress a repeated order within an otherwise new journal packet.
		batch := sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: make([]sealrollup.Sample, len(b.Points))}
		for j, p := range b.Points {
			batch.Points[j] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		s.batches = append(s.batches, batch)
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
	files := map[string][]coreKV{}
	var part partsformat.PartID
	if len(stages) > 0 {
		if _, err := rand.Read(part[:]); err != nil {
			return coreSealPlan{}, err
		}
	}
	put := func(m *rollupMetric, archive uint32, slot int64, value []byte) error {
		key := partsformat.Key{Group: m.policy.Group, VShard: partsformat.Route(m.policy.Name), Partition: 0, Part: part, Metric: m.policy.Name, Archive: archive, BlockStart: slot}
		encoded, err := partsformat.Encode(key)
		if err != nil {
			return err
		}
		prefix, err := partsformat.PartitionPrefixForGroup(key.Group, key.VShard, 0)
		if err != nil {
			return err
		}
		files[string(prefix)] = append(files[string(prefix)], coreKV{Key: encoded, Value: value})
		return nil
	}
	for i := range stages {
		s := &stages[i]
		m := s.metric
		plan, err := m.prepared.PlanInto(r.records, m.meta.Frontier, m.slots.View, m.slots.Range, s.batches, &r.workspace)
		if err != nil {
			return coreSealPlan{}, err
		}
		if r.stage != nil {
			r.stage("rollup")
		}
		// Detach before the workspace/output arena is reused for the next metric.
		s.records = slices.Clone(plan.Records)
		r.records = plan.Records[:0]
		s.next.Frontier = plan.Next
		for _, b := range s.batches {
			if s.next.Revision == 0 {
				s.next.Revision = 1
				s.next.LastUpdate = b.Now * 1e9
			}
			changed := false
			for _, p := range b.Points {
				if p.Time >= b.Now-m.policy.Config.Archives[len(m.policy.Config.Archives)-1].Step*m.policy.Config.Archives[len(m.policy.Config.Archives)-1].Count {
					changed = true
					break
				}
			}
			if changed {
				if s.next.Revision == math.MaxUint64 {
					return coreSealPlan{}, errors.New("metric revision exhausted")
				}
				s.next.Revision++
				s.next.LastUpdate = max(s.next.LastUpdate, b.Now*1e9)
			}
		}
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
		if !r.dense {
			for _, record := range s.records {
				if err := put(m, uint32(record.Archive), record.Slot, sealrollup.EncodeRecord(record)); err != nil {
					return coreSealPlan{}, err
				}
			}
		} else {
			// Runs keep the immutable metric/archive boundary and original orders.
			slices.SortFunc(s.records, func(a, b sealrollup.Record) int {
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
			for start := 0; start < len(s.records); {
				end := start + 1
				for end < len(s.records) && end-start < partsformat.MaxDenseSlots && s.records[end].Archive == s.records[start].Archive {
					end++
				}
				value, err := partsformat.EncodeSlots(s.records[start:end])
				if err != nil {
					return coreSealPlan{}, err
				}
				record := s.records[start]
				if err := put(m, uint32(record.Archive), record.Slot, value); err != nil {
					return coreSealPlan{}, err
				}
				start = end
			}
		}
		metadata, err := json.Marshal(s.next)
		if err != nil {
			return coreSealPlan{}, err
		}
		if err = put(m, rollupMetadataArchive, 0, metadata); err != nil {
			return coreSealPlan{}, err
		}
	}
	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := coreSealPlan{}
	for _, k := range keys {
		file := files[k]
		slices.SortFunc(file, func(a, b coreKV) int { return bytes.Compare(a.Key, b.Key) })
		first, err := partsformat.Decode(file[0].Key)
		if err != nil {
			return coreSealPlan{}, err
		}
		if err = (partsformat.Scope{Group: first.Group, VShard: first.VShard, Partition: 0, Part: first.Part}).ValidateRange(file[0].Key, file[len(file)-1].Key); err != nil {
			return coreSealPlan{}, err
		}
		out.Files = append(out.Files, file)
	}
	out.Commit = func() {
		for _, s := range stages {
			m := s.metric
			for _, record := range s.records {
				m.slots.Install(record.Delta)
			}
			if s.next.Expired {
				m.slots = sealrollup.NewSlots(m.policy.Config.Archives)
			}
			m.meta = s.next
			if s.fresh {
				r.metrics[m.meta.ID] = m
				r.names[m.policy.Name] = m.meta.ID
				r.prepared[m.meta.Frontier.Policy] = m.prepared
			}
		}
	}
	return out, nil
}

// ScanRollups reads one immutable core snapshot. It resolves the greatest
// replacement order for each physical slot before callers apply time filters.
// The snapshot must include metadata and all archive SSTs from one publication.
func ScanRollups(snapshot *pebble.Snapshot, visit func(RollupPolicy, RollupMetadata, []sealrollup.Record) error) error {
	type entry struct {
		group   partsformat.Group
		policy  RollupPolicy
		meta    RollupMetadata
		records map[[2]int64]sealrollup.Record
	}
	series := map[partsformat.ID]*entry{}
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 3}, UpperBound: partsformat.PrefixEnd([]byte{'G', 'C', 'P', 3})})
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
			var m RollupMetadata
			if err = json.Unmarshal(iter.Value(), &m); err != nil {
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
				if old, ok := e.records[slot]; !ok || record.Order > old.Order {
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
func recoverCoreRollups(db *pebble.DB) (*coreRollups, error) {
	r := newCoreRollups()
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
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
		r.names[policy.Name] = meta.ID
		r.prepared[meta.Frontier.Policy] = m.prepared
		return nil
	})
	return r, err
}
