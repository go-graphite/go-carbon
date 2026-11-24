package v2core

import (
	"slices"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// rollupScratch is one seal's planner working set: keys, fresh names, expiry
// bitset, stages, per-row samples and batches, lane records and part outputs. A planner holds one only
// between planParts and Commit/Abort and returns it to a bounded shared cache, so
// retained capacity follows the seals in flight instead of the high-water
// capacity of every owner. At most eight plans own these buffers at once,
// including while a plan waits for dictionary durability or publication.
//
// Per-owner state (metrics, ranks, history window caches) never moves here.
type rollupScratch struct {
	sweepKeys    []uint64
	sweepSpare   []uint64
	seen         []uint64
	pendingNames namefp.Table
	stages       []rollupStage
	stageMetrics []*PolicyIdentity
	samples      []sealrollup.Sample
	batches      []sealrollup.Batch
	files        map[string][][]plannedKV
	coverage     map[string]*rollupCoverage
	arena        []byte
	lanes        []rollupLaneScratch
}

type rollupLaneScratch struct {
	ranked  []rollupRankRef
	spare   []rollupRankRef
	counts  []int
	records []sealrollup.Record
	output  rollupOutput
}

// Keep a small idle working set across GC cycles. sync.Pool can discard these
// large buffers while owners still need them every seal, forcing allocation
// and another GC cycle. Bound both idle capacity and active working sets:
// dictionary backpressure can otherwise make all staggered owners overlap.
var rollupScratchPool = make(chan *rollupScratch, 8)
var rollupScratchSlots = make(chan struct{}, cap(rollupScratchPool))

// takeScratch adopts a pooled working set for the seal that is starting.
func (r *coreRollups) takeScratch() {
	rollupScratchSlots <- struct{}{}
	var s *rollupScratch
	select {
	case s = <-rollupScratchPool:
	default:
	}
	if s == nil {
		s = &rollupScratch{}
	}
	r.scratch = s
	r.sweepKeys, r.sweepSpare, r.seen, r.pendingNames = s.sweepKeys, s.sweepSpare, s.seen, s.pendingNames
	s.sweepKeys, s.sweepSpare, s.seen, s.pendingNames = nil, nil, nil, namefp.Table{}
	r.stages, r.stageMetrics, r.samples, r.batches = s.stages, s.stageMetrics, s.samples, s.batches
	r.files, r.coverage, r.arena = s.files, s.coverage, s.arena
	s.stages, s.stageMetrics, s.samples, s.batches = nil, nil, nil, nil
	s.files, s.coverage, s.arena = nil, nil, nil
}

// takeLaneScratch moves pooled lane buffers into the planner's sized lanes.
// Lanes beyond the pooled set start empty and join the set at release.
func (r *coreRollups) takeLaneScratch() {
	s := r.scratch
	if s == nil {
		return
	}
	for i := range min(len(r.lanes), len(s.lanes)) {
		lane, pooled := &r.lanes[i], &s.lanes[i]
		lane.ranked, lane.spare, lane.counts, lane.records, lane.output = pooled.ranked, pooled.spare, pooled.counts, pooled.records, pooled.output
		*pooled = rollupLaneScratch{}
	}
}

// returnScratch hands the cleared working set back to the pool. release calls
// it after Commit/Abort, when no plan segment, stage or record is referenced.
func (r *coreRollups) returnScratch() {
	s := r.scratch
	borrowed := s != nil
	if s == nil {
		s = &rollupScratch{}
	}
	r.scratch = nil
	s.sweepKeys, s.sweepSpare, s.seen, s.pendingNames = r.sweepKeys, r.sweepSpare, r.seen, r.pendingNames
	r.sweepKeys, r.sweepSpare, r.seen, r.pendingNames = nil, nil, nil, namefp.Table{}
	s.stages, s.stageMetrics, s.samples, s.batches = r.stages, r.stageMetrics, r.samples, r.batches
	s.files, s.coverage, s.arena = r.files, r.coverage, r.arena
	r.stages, r.stageMetrics, r.samples, r.batches = nil, nil, nil, nil
	r.files, r.coverage, r.arena = nil, nil, nil
	if len(s.lanes) < len(r.lanes) {
		s.lanes = slices.Grow(s.lanes, len(r.lanes)-len(s.lanes))[:len(r.lanes)]
	}
	for i := range r.lanes {
		lane, pooled := &r.lanes[i], &s.lanes[i]
		if lane.ranked == nil && lane.spare == nil && lane.counts == nil && lane.records == nil && lane.output.files == nil && lane.output.arena == nil {
			continue
		}
		*pooled = rollupLaneScratch{ranked: lane.ranked, spare: lane.spare, counts: lane.counts, records: lane.records, output: lane.output}
		lane.ranked, lane.spare, lane.counts, lane.records, lane.output = nil, nil, nil, nil, rollupOutput{}
	}
	select {
	case rollupScratchPool <- s:
	default:
	}
	if borrowed {
		<-rollupScratchSlots
	}
}
