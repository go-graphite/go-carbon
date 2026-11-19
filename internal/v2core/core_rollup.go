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
	"github.com/go-graphite/go-carbon/internal/namefp"
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

// rollupMetric holds what a name needs beyond its scalars: the shared
// identity and, for coarse archives, KeepRecent residency. The committed
// scalars live in coreRollups.meta, by owner slot. slots is nil while the
// name keeps no residency, which is always for a durable single-archive name,
// so such a name costs one 24-B object.
type rollupMetric struct {
	*PolicyIdentity
	slots  *sealrollup.Slots
	vshard uint16
}

// residency returns m's slots, allocating them on the first install.
func (m *rollupMetric) residency() *sealrollup.Slots {
	if m.slots == nil {
		m.slots = m.policy.prepared.NewSlots()
	}
	return m.slots
}

// keepRecent is Slots.KeepRecent. A single archive keeps nothing, so its
// slots drop to nil.
func (m *rollupMetric) keepRecent(maxTime int64) {
	if len(m.schema().Archives) == 1 {
		m.slots = nil
		return
	}
	m.residency().KeepRecent(maxTime)
}

// installRecent is Slots.InstallRecent, which never keeps the terminal
// archive (a single-archive name's only one).
func (m *rollupMetric) installRecent(d sealrollup.Delta) {
	if d.Archive == len(m.schema().Archives)-1 {
		return
	}
	m.residency().InstallRecent(d)
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
	closable map[uint64]*rollupMetric
	// names holds each installed name's fingerprint; pendingNames those of
	// this plan's fresh names. Both are pointer-free sets that read no name
	// bytes and hold no slot, so renumber moves no entry.
	names        namefp.Table
	stages       []rollupStage
	pendingNames namefp.Table
	// sweepKeys/sweepSpare are the seal's sorted batch keys and radix spare,
	// seen the idle pass's bitset of staged IDs: pointer-free, so retaining
	// them between seals adds nothing for the GC to scan.
	sweepKeys   []uint64
	sweepSpare  []uint64
	seen        []uint64
	samples     []sealrollup.Sample
	batches     []sealrollup.Batch
	files       map[string][][]plannedKV
	arena       []byte
	pending     bool
	definitions map[[32]byte][]partsformat.ArchivePolicy
	coverage    map[string]*rollupCoverage

	// Owner-local slots, in rank order. dense holds every ranked installed
	// metric in canonical (group, vshard, length, name) order, so a seal
	// sorts stages by uint32 rank instead of name bytes; unranked holds those
	// installed since the last renumber, in arrival order. Slot s < len(dense)
	// is dense[s], rank s+1; slot len(dense)+k is unranked[k]. ids[s] is the
	// slot's NumericID (deadSlot once adoption replaced its metric) and
	// meta[s] its committed scalars. install leaves the slot as a hint on the
	// metric's identity; renumber moves ids and meta with their metric and
	// re-stores the hints. ids and meta are pointer-free and sized by this
	// owner's names, not by the engine-wide ID space.
	dense    []*rollupMetric
	unranked []*rollupMetric
	ids      []uint64
	meta     []metricMetadata
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
	return &coreRollups{identities: &PolicyCatalog{}, metrics: map[uint64]*rollupMetric{}, closable: map[uint64]*rollupMetric{}}
}

// install is the only writer of metrics/closable/names, so the closure index
// can never miss a coarse-archive metric and every installed name is indexed.
// meta becomes id's committed scalars.
func (r *coreRollups) install(id uint64, m *rollupMetric, meta metricMetadata) {
	old := r.metrics[id]
	r.metrics[id] = m
	s := uint32(len(r.ids))
	if old != nil {
		s = r.slotOf(id, old)
	}
	switch {
	case old != nil && int(s) >= len(r.dense):
		r.unranked[int(s)-len(r.dense)] = m // an unranked old hands m its slot
	case old == m:
	default:
		if old != nil {
			r.ids[s], s = deadSlot, uint32(len(r.ids)) // the next renumber drops it
		}
		if id == deadSlot {
			panic("v2core: rollup NumericID collides with deadSlot")
		}
		r.ids, r.meta, r.unranked = append(r.ids, id), append(r.meta, metricMetadata{}), append(r.unranked, m)
	}
	r.meta[s] = meta
	m.ownerSlot.Store(s + 1)
	if old != m {
		r.names.Put(m.fingerprint(), 0) // a set: the slot is m's ownerSlot
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
	return &rollupMetric{PolicyIdentity: h, vshard: h.vshard}
}

// freshMetadata is the committed state of a name no seal has installed yet.
var freshMetadata = compactMetadata(RollupMetadata{Version: 1})

// committed returns the scalars s starts from: its slot's, or fresh ones.
func (r *coreRollups) committed(s *rollupStage) metricMetadata {
	if s.fresh {
		return freshMetadata
	}
	return r.meta[s.slot]
}

type rollupStage struct {
	sortKey    uint64
	vshard     uint16
	slot       uint32 // owner slot of an installed metric; unset when fresh
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
	// One stable sort of batch indices by ID makes each name's batches one run
	// in arrival order; the sweep stages a name once per run. The keys and the
	// idle bitset are pointer-free scratch retained between seals.
	keys, spare, maxID := sortSealBatches(in.Batches, r.sweepKeys, r.sweepSpare)
	r.sweepKeys, r.sweepSpare = keys, spare
	runs := 0
	for i := range keys {
		if i == 0 || in.Batches[keys[i]&sealBatchIndexMask].ID != in.Batches[keys[i-1]&sealBatchIndexMask].ID {
			runs++
		}
	}
	stages = slices.Grow(stages, runs)
	r.stages = stages
	idle := r.expire != nil || r.db != nil
	seen := r.seen[:0]
	if words := sealSeenWords(maxID, len(keys)); idle && words != 0 {
		seen = slices.Grow(seen, words)[:words]
	}
	r.seen = seen
	// One allocation per sealed part for samples and application descriptors.
	// Every original boundary/order/clock remains distinct.
	sampleCount := 0
	for i := range in.Batches {
		sampleCount += len(in.Batches[i].Points)
	}
	r.samples = slices.Grow(r.samples[:0], sampleCount)[:sampleCount]
	samples := r.samples
	r.batches = slices.Grow(r.batches[:0], len(in.Batches))[:len(in.Batches)]
	batchStorage := r.batches
	offset := 0
	for i := 0; i < len(keys); {
		first := &in.Batches[keys[i]&sealBatchIndexMask]
		id := first.ID
		j := i + 1
		for j < len(keys) && in.Batches[keys[j]&sealBatchIndexMask].ID == id {
			j++
		}
		// A ranked name whose identity carries this planner's slot reaches
		// its metric through dense. Fresh and unranked names, policy-bytes
		// batches and hints another planner overwrote probe the metrics map.
		var m *rollupMetric
		slot, hinted := r.hintSlot(first.Registered, id)
		rk := uint32(0)
		if hinted && int(slot) < len(r.dense) {
			m, rk = r.dense[slot], slot+1
		} else if m = r.metrics[id]; m != nil && !hinted {
			slot = r.slotOf(id, m)
		}
		fresh := m == nil
		meta := freshMetadata
		if !fresh {
			meta = r.meta[slot]
		}
		if fresh {
			if h := first.Registered; h != nil && h.catalog == r.identities && h.id == id {
				m = newRollupMetric(h)
			} else {
				policy := first.Policy
				if h != nil {
					policy = h.definitionBytes()
				}
				var err error
				m, err = r.metric(id, policy)
				if err != nil {
					return coreSealPlan{}, err
				}
			}
			// A fresh ID is not installed, and the sweep stages each ID once,
			// so any holder of this fingerprint is another ID for the name.
			fp := m.fingerprint()
			if _, ok := r.names.Get(fp); ok {
				return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
			}
			if _, ok := r.pendingNames.Get(fp); ok {
				return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
			}
			r.pendingNames.Put(fp, uint32(len(stages)))
		}
		stages = append(stages, rollupStage{metric: m, slot: slot, next: m.metadataScalars(meta), fresh: fresh, batchCount: j - i})
		s := &stages[len(stages)-1]
		s.next.Expired = false
		if rk != 0 {
			s.vshard, s.sortKey = m.vshard, uint64(rk)<<1
		} else {
			r.stageKey(s)
		}
		if len(seen) != 0 {
			seen[id>>6] |= 1 << (id & 63)
		}
		s.batches = batchStorage[: 0 : j-i]
		batchStorage = batchStorage[j-i:]
		for ; i < j; i++ {
			b := &in.Batches[keys[i]&sealBatchIndexMask]
			if (b.Registered == nil || b.Registered != m.PolicyIdentity) && !m.definitionEqual(b.Policy) {
				return coreSealPlan{}, errors.New("stored rollup policy changed")
			}
			ps := samples[offset : offset+len(b.Points) : offset+len(b.Points)]
			for k, p := range b.Points {
				ps[k] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
			}
			offset += len(b.Points)
			s.batches = append(s.batches, sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: ps})
		}
	}
	named := len(stages)
	// Idle closure uses the existing seal maintenance pass; no separate scheduler.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if r.expire != nil || r.db != nil {
		for id, m := range r.idleCandidates() {
			if sealBatchSeen(in.Batches, keys, seen, maxID, id) {
				continue
			}
			slot := r.slotOf(id, m)
			if meta := r.meta[slot]; !meta.Expired && ((r.expire != nil && r.expire(m.rollupPolicy(), m.metadata(meta), now)) || rollupClosureDue(m, meta, now.Unix())) {
				stages = append(stages, rollupStage{metric: m, slot: slot, next: m.metadata(meta)})
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
				r.install(m.id, m, compactMetadata(s.next))
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
		m.keepRecent(meta.Frontier.MaxTime)
		for _, record := range records {
			m.installRecent(record.Delta)
		}
		m.keepRecent(meta.Frontier.MaxTime)
		r.install(meta.ID, m, compactMetadata(meta))
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
