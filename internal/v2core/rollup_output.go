package v2core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// plannedKV is one planned archive record, or with a nil record the metric's
// envelope, of a rollup part. The part writer formats its key from the name,
// vshard and record; the planner keeps no key bytes. The tuple carries the
// name and vshard so the writer never dereferences the metric's scattered
// planner state.
type plannedKV struct {
	name   partsformat.ID
	record *sealrollup.Record
	value  []byte
	vshard uint16
	// rank is the metric's planner rank (stageKey), or 0 when unranked. The
	// writer checks order by rank instead of comparing names.
	rank uint32
}

// plannedRollupPart is one group/partition part: lane segments in lane order,
// which is key order because lanes own ascending disjoint vshard ranges.
type plannedRollupPart struct {
	Spec     partsformat.Spec
	Segments [][]plannedKV
	// Generation is the dictionary generation of an ID-keyed part (each
	// tuple's rank is then its dictionary ID), 0 for a name-keyed part.
	Generation uint64
}

type rollupOutput struct {
	// byID files each tuple under its dictionary ID (ID-keyed parts).
	byID        bool
	files       map[string]*[]plannedKV
	arena       []byte
	definitions map[[32]byte][]partsformat.ArchivePolicy
	coverage    map[string]*rollupCoverage
	partitions  []int64
	// Cached current month and file name: stages arrive sorted by group, and
	// live samples share one month, so lookups rarely allocate.
	monthLo, monthHi int64
	lastGroup        string
	lastPartition    int64
	lastFileID       string
	// Stages arrive sorted by group and mostly share one month: keep the
	// current file and coverage rather than hashing the file ID per stage.
	curFileID   string
	curFile     *[]plannedKV
	curCoverage *rollupCoverage
}

func (r *rollupOutput) file(fileID string) (*[]plannedKV, *rollupCoverage) {
	if r.curFile != nil && fileID == r.curFileID {
		return r.curFile, r.curCoverage
	}
	file := r.files[fileID]
	if file == nil {
		file = new([]plannedKV)
		r.files[fileID] = file
	}
	coverage := r.coverage[fileID]
	if coverage == nil {
		coverage = &rollupCoverage{seen: map[[32]byte]bool{}}
		r.coverage[fileID] = coverage
	}
	r.curFileID, r.curFile, r.curCoverage = fileID, file, coverage
	return file, coverage
}

func (r *rollupOutput) monthPartition(t int64) int64 {
	if t >= r.monthLo && t < r.monthHi {
		return r.monthLo
	}
	lo := rollupMonthPartition(t)
	y, m, _ := time.Unix(lo, 0).UTC().Date()
	r.monthLo, r.monthHi = lo, time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return lo
}

func (r *rollupOutput) fileID(group string, partition int64) string {
	if r.lastFileID == "" || group != r.lastGroup || partition != r.lastPartition {
		r.lastGroup, r.lastPartition = group, partition
		r.lastFileID = group + "/" + strconv.FormatInt(partition, 10)
	}
	return r.lastFileID
}

// appendStage files records, the winners of their slots, and versions, the
// superseded versions of those slots (sealrollup.SealPlan): each slot's
// versions go just before its winner, in the winner's partition, and count
// toward no coverage, since only Mat and Raw winners are visible to readers.
func (r *rollupOutput) appendStage(s *rollupStage, m *rollupMetric, batches []sealrollup.Batch, records, versions []sealrollup.Record, part partsformat.PartID, stage func(string)) error {
	if r.files == nil {
		r.files = map[string]*[]plannedKV{}
		r.definitions = map[[32]byte][]partsformat.ArchivePolicy{}
		r.coverage = map[string]*rollupCoverage{}
	}
	group := string(m.policy.Group)
	name, vshard := m.policy.Name, m.vshard
	var rank uint32
	if r.byID {
		rank = uint32(s.sortKey) // (vshard, ID): the low half is the ID
	} else if s.sortKey&1 == 0 {
		rank = uint32(s.sortKey >> 1)
	}
	if stage != nil {
		stage("rollup")
	}
	schema := m.schema()
	last := schema.Archives[len(schema.Archives)-1]
	horizon := last.Step * last.Count
	// An idle closure has no admission event; it cannot lower this bound.
	minClock := int64(math.MaxInt64)
	for _, b := range batches {
		minClock = min(minClock, b.Now)
		if s.next.Revision == 0 {
			s.next.Revision = 1
			s.next.LastUpdate = b.Now * 1e9
		}
		changed := false
		for _, p := range b.Points {
			if p.Time >= b.Now-horizon {
				changed = true
				break
			}
		}
		if changed {
			if s.next.Revision == math.MaxUint64 {
				return errors.New("metric revision exhausted")
			}
			s.next.Revision++
			s.next.LastUpdate = max(s.next.LastUpdate, b.Now*1e9)
		}
	}
	// Records go to the calendar month of their sample time; the envelope goes
	// with every partition written and with the month of the newest sample, so
	// the newest envelope survives as long as any data it can mask.
	home := r.monthPartition(s.next.Frontier.MaxTime)
	envelopeExpiry := rollupMetricExpiry(schema, s.next.Frontier.MaxTime)
	partitions := r.partitions[:0]
	partitions = append(partitions, home)
	for i := range records {
		if p := r.monthPartition(records[i].Sample.Time); !slices.Contains(partitions, p) {
			partitions = append(partitions, p)
		}
	}
	r.partitions = partitions
	start := len(r.arena)
	r.arena = m.appendMetadata(r.arena, m.metadataScalars(s.next))
	metadata := r.arena[start:len(r.arena):len(r.arena)]
	for _, partition := range partitions {
		filePtr, coverage := r.file(r.fileID(group, partition))
		physicalPart := part
		binary.BigEndian.PutUint64(physicalPart[:8], binary.BigEndian.Uint64(part[:8])^uint64(partition))
		if len(coverage.seen) == 0 {
			coverage.spec = partsformat.Spec{Scope: partsformat.Scope{Group: m.policy.Group, VShard: partsformat.GroupVShard, Partition: partition, Part: physicalPart}, Encoding: partsformat.DenseEncoding, MinClock: minClock, MinTime: math.MaxInt64, MaxTime: math.MinInt64}
			coverage.spec.Source.MinOrder = math.MaxUint64
		}
		coverage.spec.MinClock = min(coverage.spec.MinClock, minClock)
		digest := m.policy.prepared.Digest()
		if !coverage.seen[digest] {
			policies := r.definitions[digest]
			if policies == nil {
				definition, err := json.Marshal(schema)
				if err != nil {
					return err
				}
				hash := sha256.Sum256(definition)
				for archive := range schema.Archives {
					policies = append(policies, partsformat.ArchivePolicy{Archive: uint32(archive), Digest: hash, Definition: definition})
				}
				partsformat.RegisterPolicies(policies)
				r.definitions[digest] = policies
			}
			coverage.spec.Policies = append(coverage.spec.Policies, policies...)
			coverage.seen[digest] = true
		}
		expiry := expiryBounds{coverage.spec.MinExpiry, coverage.spec.MaxExpiry}
		expiry.envelope(envelopeExpiry)
		coverage.spec.MinTime = min(coverage.spec.MinTime, s.next.Frontier.MaxTime)
		coverage.spec.MaxTime = max(coverage.spec.MaxTime, s.next.Frontier.MaxTime)
		coverage.spec.Source.MinOrder = min(coverage.spec.Source.MinOrder, s.next.Frontier.Order)
		coverage.spec.Source.MaxOrder = max(coverage.spec.Source.MaxOrder, s.next.Frontier.Order)
		// Physical dense packing belongs exclusively to the shared v2part writer.
		file := *filePtr
		v := 0
		for i := range records {
			record := &records[i]
			if len(partitions) > 1 && r.monthPartition(record.Sample.Time) != partition {
				continue
			}
			for ; v < len(versions) && (versions[v].Archive < record.Archive || versions[v].Archive == record.Archive && versions[v].Slot <= record.Slot); v++ {
				if versions[v].Archive == record.Archive && versions[v].Slot == record.Slot {
					file = append(file, plannedKV{name: name, record: &versions[v], vshard: vshard, rank: rank})
				}
			}
			file = append(file, plannedKV{name: name, record: record, vshard: vshard, rank: rank})
			expiry.data(rollupRecordExpiry(schema, *record))
			coverage.spec.MinTime = min(coverage.spec.MinTime, record.Sample.Time)
			coverage.spec.MaxTime = max(coverage.spec.MaxTime, record.Sample.Time)
			coverage.spec.Source.MinOrder = min(coverage.spec.Source.MinOrder, record.Order)
			coverage.spec.Source.MaxOrder = max(coverage.spec.Source.MaxOrder, record.Order)
		}
		coverage.spec.MinExpiry, coverage.spec.MaxExpiry = expiry.min, expiry.max
		file = append(file, plannedKV{name: name, value: metadata, vshard: vshard, rank: rank})
		*filePtr = file
	}

	return nil
}

func (r *rollupOutput) release() {
	for _, file := range r.files {
		clear(*file)
		*file = (*file)[:0]
	}
	r.curFileID, r.curFile, r.curCoverage = "", nil, nil
	for _, c := range r.coverage {
		clear(c.seen)
		c.spec.Policies = nil
	}
	r.arena = r.arena[:0]
}

// Each lane owns a disjoint routed key range, planner workspace and output
// arena until the one owner Commit/Abort. No worker publishes a frontier.
func (r *coreRollups) planStages(part partsformat.PartID) (coreSealPlan, error) {
	stages, metrics := r.stages, r.stageMetrics
	workers := r.parallelism
	if workers == 0 {
		workers = min(8, runtime.GOMAXPROCS(0))
	}
	workers = min(workers, max(1, len(stages)/128))
	if len(r.lanes) < workers {
		r.lanes = slices.Grow(r.lanes, workers-len(r.lanes))[:workers]
	}
	r.takeLaneScratch()
	for i := range r.lanes {
		r.lanes[i].ranked = r.lanes[i].ranked[:0]
		r.lanes[i].output.byID = r.generation != 0
	}
	// plan assigned rank keys while each metric was hot; lanes follow the
	// vshard, and the canonical order within a lane is the key order.
	for i := range stages {
		s := &stages[i]
		lane := int(s.vshard) * workers / 4096
		r.lanes[lane].ranked = append(r.lanes[lane].ranked, rollupRankRef{key: s.sortKey, stage: uint32(i)})
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		work := func() {
			lane := &r.lanes[i]
			lane.records, lane.err = lane.records[:0], nil
			lane.ranked, lane.spare = sortRollupRanks(lane.ranked, lane.spare, &lane.counts, metrics)
			for _, ref := range lane.ranked {
				s, m := &stages[ref.stage], metrics[ref.stage]
				batches := r.batches[s.batch : s.batch+s.batches : s.batch+s.batches]
				start := len(lane.records)
				frontier := sealrollup.Frontier{Order: s.next.Frontier.Order, MaxTime: s.next.Frontier.MaxTime, Policy: m.policy.prepared.Digest()}
				s.next.Version = 3
				plan, err := m.policy.prepared.RouteInto(lane.records[start:], frontier, batches, &lane.workspace)

				if err != nil {
					lane.err = err
					return
				}
				s.next.Frontier.Order, s.next.Frontier.MaxTime = plan.Next.Order, plan.Next.MaxTime
				// A plan that fit was written in place. Otherwise the full
				// chunk stays put (tuples point into it) and a chunk twice as
				// large takes this plan.
				n := len(plan.Records)
				if n > cap(lane.records)-start {
					lane.chunks, lane.bases = append(lane.chunks, lane.records), append(lane.bases, lane.base)
					lane.base += uint32(start)
					lane.records = append(make([]sealrollup.Record, 0, max(2*cap(lane.records), n, 1024)), plan.Records...)
					start = 0
				} else {
					lane.records = lane.records[:start+n]
				}
				if uint64(lane.base)+uint64(start+n) > math.MaxUint32 {
					lane.err = errors.New("too many planned rollup records")
					return
				}
				winners := n - plan.Versions
				s.record, s.records, s.versions = lane.base+uint32(start), uint32(winners), uint32(plan.Versions)
				// Build while this metric's policy, metadata and batches are hot.
				// The lane still retains all arenas until the owner joins ingest.
				if lane.err = lane.output.appendStage(s, m, batches, lane.records[start:start+winners:start+winners], lane.records[start+winners:start+n:start+n], part, r.stage); lane.err != nil {
					return
				}
			}
		}
		if workers == 1 {
			work()
		} else {
			wg.Go(work)
		}
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if r.lanes[i].err != nil {
			return coreSealPlan{}, r.lanes[i].err
		}
	}
	if r.files == nil {
		r.files = map[string][][]plannedKV{}
	}
	if r.coverage == nil {
		r.coverage = map[string]*rollupCoverage{}
	}
	// Concatenation follows deterministic disjoint vshard ranges; each group
	// and partition remains one physical part regardless of planner parallelism.
	for i := 0; i < workers; i++ {
		lane := &r.lanes[i]
		for group, file := range lane.output.files {
			if len(*file) == 0 {
				continue
			}
			// Segments reference lane storage, valid through Commit/Abort.
			r.files[group] = append(r.files[group], *file)
			src := lane.output.coverage[group]
			dst := r.coverage[group]
			if dst == nil {
				dst = &rollupCoverage{seen: map[[32]byte]bool{}}
				r.coverage[group] = dst
			}
			if len(dst.seen) == 0 {
				dst.spec = src.spec
				dst.spec.Policies = nil
			}
			for digest := range src.seen {
				if !dst.seen[digest] {
					dst.spec.Policies = append(dst.spec.Policies, lane.output.definitions[digest]...)
					dst.seen[digest] = true
				}
			}
			dst.spec.MinClock = min(dst.spec.MinClock, src.spec.MinClock)
			dst.spec.MinTime = min(dst.spec.MinTime, src.spec.MinTime)
			dst.spec.MaxTime = max(dst.spec.MaxTime, src.spec.MaxTime)
			dst.spec.Source.MinOrder = min(dst.spec.Source.MinOrder, src.spec.Source.MinOrder)
			dst.spec.Source.MaxOrder = max(dst.spec.Source.MaxOrder, src.spec.Source.MaxOrder)
			expiry := expiryBounds{dst.spec.MinExpiry, dst.spec.MaxExpiry}
			expiry.join(expiryBounds{src.spec.MinExpiry, src.spec.MaxExpiry})
			dst.spec.MinExpiry, dst.spec.MaxExpiry = expiry.min, expiry.max
		}
	}
	keys := make([]string, 0, len(r.files))
	for group, file := range r.files {
		if len(file) > 0 {
			keys = append(keys, group)
		}
	}
	slices.Sort(keys)
	out := coreSealPlan{}
	for _, group := range keys {
		out.Parts = append(out.Parts, plannedRollupPart{Spec: r.coverage[group].spec, Segments: r.files[group], Generation: r.generation})
	}
	return out, nil
}

func compareRollupMetrics(x, y *rollupMetric) int {
	// Most comparisons share one interned definition, including its group.
	if x.policy.policyDefinition != y.policy.policyDefinition {
		if len(x.policy.Group) != len(y.policy.Group) {
			return len(x.policy.Group) - len(y.policy.Group)
		}
		if x.policy.Group < y.policy.Group {
			return -1
		}
		if x.policy.Group > y.policy.Group {
			return 1
		}
	}
	if x.vshard != y.vshard {
		return int(x.vshard) - int(y.vshard)
	}
	if len(x.policy.Name) != len(y.policy.Name) {
		return len(x.policy.Name) - len(y.policy.Name)
	}
	if x.policy.Name < y.policy.Name {
		return -1
	}
	if x.policy.Name > y.policy.Name {
		return 1
	}
	return 0
}

// State installation is independent per metric. The owner joins every lane
// before publishing names, snapshots or the durable frontier to consumers.
func (r *coreRollups) commitStages() {
	var wg sync.WaitGroup
	for i := range r.lanes {
		work := func() {
			lane := &r.lanes[i]
			for _, ref := range lane.ranked {
				s := &r.stages[ref.stage]
				if !s.fresh { // a fresh metric gets its slot, and these scalars, at install
					r.meta[s.slot] = s.next
				}
			}
		}
		if len(r.lanes) == 1 {
			work()
		} else {
			wg.Go(work)
		}
	}
	wg.Wait()
}
