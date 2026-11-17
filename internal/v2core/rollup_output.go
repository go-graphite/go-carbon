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

type rollupOutput struct {
	files       map[string][]coreKV
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

func (r *rollupOutput) appendStage(s *rollupStage, part partsformat.PartID, stage func(string)) error {
	if r.files == nil {
		r.files = map[string][]coreKV{}
		r.definitions = map[[32]byte][]partsformat.ArchivePolicy{}
		r.coverage = map[string]*rollupCoverage{}
	}
	files := r.files
	key := func(m *rollupMetric, partition int64, archive uint32, slot int64) []byte {
		physicalPart := part
		binary.BigEndian.PutUint64(physicalPart[:8], binary.BigEndian.Uint64(part[:8])^uint64(partition))
		start := len(r.arena)
		r.arena = append(r.arena, m.policy.keyPrefix...)
		r.arena = binary.BigEndian.AppendUint16(r.arena, m.vshard)
		r.arena = binary.BigEndian.AppendUint64(r.arena, uint64(partition)^(1<<63))
		r.arena = append(r.arena, physicalPart[:]...)
		r.arena = binary.BigEndian.AppendUint32(r.arena, uint32(len(m.policy.Name)))
		r.arena = append(r.arena, m.policy.Name...)
		r.arena = append(r.arena, make([]byte, 16)...)
		encoded := r.arena[start:len(r.arena):len(r.arena)]
		tail := len(encoded) - 16
		binary.BigEndian.PutUint32(encoded[tail:], archive)
		binary.BigEndian.PutUint64(encoded[tail+4:], uint64(slot)^(1<<63))
		return encoded
	}
	m := s.metric
	group := string(m.policy.Group)
	if stage != nil {
		stage("rollup")
	}
	for _, b := range s.batches {
		if s.next.Revision == 0 {
			s.next.Revision = 1
			s.next.LastUpdate = b.Now * 1e9
		}
		changed := false
		for _, p := range b.Points {
			if p.Time >= b.Now-m.schema().Archives[len(m.schema().Archives)-1].Step*m.schema().Archives[len(m.schema().Archives)-1].Count {
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
	schema := m.schema()
	home := r.monthPartition(s.next.Frontier.MaxTime)
	envelopeExpiry := rollupMetricExpiry(schema, s.next.Frontier.MaxTime)
	partitions := append(r.partitions[:0], home)
	for i := range s.records {
		if p := r.monthPartition(s.records[i].Sample.Time); !slices.Contains(partitions, p) {
			partitions = append(partitions, p)
		}
	}
	r.partitions = partitions
	start := len(r.arena)
	r.arena = m.appendMetadata(r.arena, s.next)
	metadata := r.arena[start:len(r.arena):len(r.arena)]
	for _, partition := range partitions {
		fileID := r.fileID(group, partition)
		physicalPart := part
		binary.BigEndian.PutUint64(physicalPart[:8], binary.BigEndian.Uint64(part[:8])^uint64(partition))
		coverage := r.coverage[fileID]
		if coverage == nil {
			coverage = &rollupCoverage{seen: map[[32]byte]bool{}}
			r.coverage[fileID] = coverage
		}
		if len(coverage.seen) == 0 {
			coverage.spec = partsformat.Spec{Scope: partsformat.Scope{Group: m.policy.Group, VShard: partsformat.GroupVShard, Partition: partition, Part: physicalPart}, Encoding: partsformat.DenseEncoding, MinTime: math.MaxInt64, MaxTime: math.MinInt64}
			coverage.spec.Source.MinOrder = math.MaxUint64
		}
		digest := s.next.Frontier.Policy
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
		file := files[fileID]
		for i := range s.records {
			record := &s.records[i]
			if len(partitions) > 1 && r.monthPartition(record.Sample.Time) != partition {
				continue
			}
			file = append(file, coreKV{Key: key(m, partition, uint32(record.Archive), record.Slot), Record: record})
			expiry.data(rollupRecordExpiry(schema, *record))
			coverage.spec.MinTime = min(coverage.spec.MinTime, record.Sample.Time)
			coverage.spec.MaxTime = max(coverage.spec.MaxTime, record.Sample.Time)
			coverage.spec.Source.MinOrder = min(coverage.spec.Source.MinOrder, record.Order)
			coverage.spec.Source.MaxOrder = max(coverage.spec.Source.MaxOrder, record.Order)
		}
		coverage.spec.MinExpiry, coverage.spec.MaxExpiry = expiry.min, expiry.max
		files[fileID] = append(file, coreKV{Key: key(m, partition, rollupMetadataArchive, 0), Value: metadata})
	}

	return nil
}

func (r *rollupOutput) release() {
	for group, file := range r.files {
		clear(file)
		r.files[group] = file[:0]
	}
	for _, c := range r.coverage {
		clear(c.seen)
		c.spec.Policies = nil
	}
	r.arena = r.arena[:0]
}

// Each lane owns a disjoint routed key range, planner workspace and output
// arena until the one owner Commit/Abort. No worker publishes a frontier.
func (r *coreRollups) planStages(stages []rollupStage, part partsformat.PartID) (coreSealPlan, error) {
	workers := r.parallelism
	if workers == 0 {
		workers = min(8, runtime.GOMAXPROCS(0))
	}
	workers = min(workers, max(1, len(stages)/128))
	if len(r.lanes) < workers {
		r.lanes = slices.Grow(r.lanes, workers-len(r.lanes))[:workers]
	}
	for i := range r.lanes {
		r.lanes[i].ordered = r.lanes[i].ordered[:0]
	}
	prepareRollupStageOrder(stages)
	for i := range stages {
		lane := int(stages[i].metric.vshard) * workers / 4096
		r.lanes[lane].ordered = append(r.lanes[lane].ordered, &stages[i])
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		work := func() {
			lane := &r.lanes[i]
			history := rollupHistoryReader{db: r.db}
			defer history.close()
			lane.records, lane.err = lane.records[:0], nil
			slices.SortFunc(lane.ordered, compareRollupStages)
			for _, s := range lane.ordered {
				m := s.metric
				view, ranges := m.slots.View, m.slots.Range
				var readErr error
				if r.db != nil {
					// Every installed sample is at/before Frontier.MaxTime; the
					// correction bit already covers equal-time replacements.
					view = nil
					ranges = func(a int, start, end int64, dst []sealrollup.Sample) []sealrollup.Sample {
						if readErr != nil {
							return dst
						}
						if result, ok := m.slots.RecentRange(a, start, end, dst); ok {
							return result
						}
						result, err := lane.windows.rangeSamples(&history, m, a, start, end)
						readErr = err
						return append(dst, result...)
					}
				}
				plan, err := m.policy.prepared.PlanInto(lane.records[len(lane.records):], s.next.Frontier, view, ranges, s.batches, &lane.workspace)
				if readErr != nil {
					lane.err = readErr
					return
				}
				if err != nil {
					lane.err = err
					return
				}
				s.records = plan.Records
				s.next.Frontier = plan.Next
				if len(plan.Records) <= cap(lane.records)-len(lane.records) {
					lane.records = lane.records[:len(lane.records)+len(plan.Records)]
				} else {
					lane.records = append(lane.records, plan.Records...)
				}
				// Build while this metric's policy, metadata and batches are hot.
				// The lane still retains all arenas until the owner joins ingest.
				if lane.err = lane.output.appendStage(s, part, r.stage); lane.err != nil {
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
		r.files = map[string][]coreKV{}
	}
	if r.coverage == nil {
		r.coverage = map[string]*rollupCoverage{}
	}
	// Concatenation follows deterministic disjoint vshard ranges; each group
	// and partition remains one physical part regardless of planner parallelism.
	for i := 0; i < workers; i++ {
		lane := &r.lanes[i]
		for group, file := range lane.output.files {
			if len(file) == 0 {
				continue
			}
			r.files[group] = append(r.files[group], file...)
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
		out.Files = append(out.Files, r.files[group])
		out.PartSpecs = append(out.PartSpecs, r.coverage[group].spec)
	}
	return out, nil
}

func prepareRollupStageOrder(stages []rollupStage) {
	// Compute the canonical group and name prefixes once per participating
	// stage, rather than chasing catalog pointers for every sort comparison.
	groups := make(map[partsformat.Group]uint32)
	for i := range stages {
		groups[stages[i].metric.policy.Group] = 0
	}
	orderedGroups := make([]partsformat.Group, 0, len(groups))
	for group := range groups {
		orderedGroups = append(orderedGroups, group)
	}
	slices.SortFunc(orderedGroups, func(a, b partsformat.Group) int {
		if len(a) != len(b) {
			return len(a) - len(b)
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	for i, group := range orderedGroups {
		groups[group] = uint32(i)
	}
	for i := range stages {
		s := &stages[i]
		m := s.metric
		s.sortGroup = groups[m.policy.Group]
		s.sortName = string(m.policy.Name)
		s.sortRouteName = uint64(m.vshard)<<32 | uint64(len(s.sortName))
		var prefix [16]byte
		copy(prefix[:], s.sortName)
		s.sortPrefix = [2]uint64{binary.BigEndian.Uint64(prefix[:8]), binary.BigEndian.Uint64(prefix[8:])}
	}
}

func compareRollupStages(a, b *rollupStage) int {
	if a.sortGroup != b.sortGroup {
		if a.sortGroup < b.sortGroup {
			return -1
		}
		return 1
	}
	if a.sortRouteName != b.sortRouteName {
		if a.sortRouteName < b.sortRouteName {
			return -1
		}
		return 1
	}
	for i := range a.sortPrefix {
		if a.sortPrefix[i] != b.sortPrefix[i] {
			if a.sortPrefix[i] < b.sortPrefix[i] {
				return -1
			}
			return 1
		}
	}
	if len(a.sortName) <= 16 {
		return 0
	}
	if a.sortName[16:] < b.sortName[16:] {
		return -1
	}
	if a.sortName[16:] > b.sortName[16:] {
		return 1
	}
	return 0
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
			for _, s := range r.lanes[i].ordered {
				m := s.metric
				if r.db != nil {
					m.slots.KeepRecent(s.next.Frontier.MaxTime)
				}
				for _, record := range s.records {
					if r.db != nil {
						m.slots.InstallRecent(record.Delta)
					} else {
						m.slots.Install(record.Delta)
					}
				}
				if r.db != nil {
					if len(r.lanes[i].windows.entries) != 0 {
						r.lanes[i].windows.commit(m, s.records, s.next.Expired)
					}
				}
				if s.next.Expired {
					m.slots = *m.policy.prepared.NewSlots()
				}
				m.meta = compactMetadata(s.next)
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
