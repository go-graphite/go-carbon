package v2core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"runtime"
	"slices"
	"sync"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

type rollupOutput struct {
	files       map[string][]coreKV
	arena       []byte
	definitions map[[32]byte][]partsformat.ArchivePolicy
	coverage    map[string]*rollupCoverage
}

func (r *rollupOutput) appendStage(s *rollupStage, part partsformat.PartID, stage func(string)) error {
	if r.files == nil {
		r.files = map[string][]coreKV{}
		r.definitions = map[[32]byte][]partsformat.ArchivePolicy{}
		r.coverage = map[string]*rollupCoverage{}
	}
	files := r.files
	key := func(m *rollupMetric, archive uint32, slot int64) []byte {
		start := len(r.arena)
		r.arena = append(r.arena, m.keyTemplate...)
		encoded := r.arena[start:len(r.arena):len(r.arena)]
		copy(encoded[m.partOffset:m.partOffset+16], part[:])
		tail := len(encoded) - 16
		binary.BigEndian.PutUint32(encoded[tail:], archive)
		binary.BigEndian.PutUint64(encoded[tail+4:], uint64(slot)^(1<<63))
		return encoded
	}
	m := s.metric
	group := string(m.policy.Group)
	file := files[group]
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
	// Physical dense packing belongs exclusively to the shared v2part writer.
	for i := range s.records {
		record := &s.records[i]
		file = append(file, coreKV{Key: key(m, uint32(record.Archive), record.Slot), Record: record})
	}
	coverage := r.coverage[group]
	if coverage == nil {
		coverage = &rollupCoverage{seen: map[[32]byte]bool{}}
		r.coverage[group] = coverage
	}
	if len(coverage.seen) == 0 {
		coverage.spec = partsformat.Spec{Scope: partsformat.Scope{Group: m.policy.Group, VShard: partsformat.GroupVShard, Partition: 0, Part: part}, Encoding: partsformat.DenseEncoding, MinTime: math.MaxInt64, MaxTime: math.MinInt64}
		coverage.spec.Source.MinOrder = math.MaxUint64
	}
	digest := s.next.Frontier.Policy
	if !coverage.seen[digest] {
		policies := r.definitions[digest]
		if policies == nil {
			definition, err := json.Marshal(m.schema())
			if err != nil {
				return err
			}
			hash := sha256.Sum256(definition)
			for archive := range m.schema().Archives {
				policies = append(policies, partsformat.ArchivePolicy{Archive: uint32(archive), Digest: hash, Definition: definition})
			}
			r.definitions[digest] = policies
		}
		coverage.spec.Policies = append(coverage.spec.Policies, policies...)
		coverage.seen[digest] = true
	}
	coverage.spec.MinTime = min(coverage.spec.MinTime, s.next.Frontier.MaxTime)
	coverage.spec.MaxTime = max(coverage.spec.MaxTime, s.next.Frontier.MaxTime)
	coverage.spec.Source.MinOrder = min(coverage.spec.Source.MinOrder, s.next.Frontier.Order)
	coverage.spec.Source.MaxOrder = max(coverage.spec.Source.MaxOrder, s.next.Frontier.Order)
	for _, record := range s.records {
		coverage.spec.MinTime = min(coverage.spec.MinTime, record.Sample.Time)
		coverage.spec.MaxTime = max(coverage.spec.MaxTime, record.Sample.Time)
		coverage.spec.Source.MinOrder = min(coverage.spec.Source.MinOrder, record.Order)
		coverage.spec.Source.MaxOrder = max(coverage.spec.Source.MaxOrder, record.Order)
	}
	start := len(r.arena)
	r.arena = AppendRollupMetadata(r.arena, s.next)
	metadata := r.arena[start:len(r.arena):len(r.arena)]
	files[group] = append(file, coreKV{Key: key(m, rollupMetadataArchive, 0), Value: metadata})

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
		r.lanes = make([]rollupPlannerLane, workers)
	}
	for i := range r.lanes {
		r.lanes[i].ordered = r.lanes[i].ordered[:0]
	}
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
				plan, err := m.prepared.PlanInto(lane.records[len(lane.records):], m.metadata().Frontier, view, ranges, s.batches, &lane.workspace)
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
	// remains one physical part, regardless of planner parallelism.
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

func compareRollupStages(a, b *rollupStage) int {
	// Only metrics participating in this part need ordering. Historical names
	// cannot change their relative canonical order.
	return compareRollupMetrics(a.metric, b.metric)
}

func compareRollupMetrics(x, y *rollupMetric) int {
	if len(x.policy.Group) != len(y.policy.Group) {
		return len(x.policy.Group) - len(y.policy.Group)
	}
	if x.policy.Group < y.policy.Group {
		return -1
	}
	if x.policy.Group > y.policy.Group {
		return 1
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
					m.slots.KeepRecent(s.next.Frontier.MaxTime)
					r.lanes[i].windows.commit(m, s.records, s.next.Expired)
				}
				if s.next.Expired {
					m.slots = *m.prepared.NewSlots()
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
