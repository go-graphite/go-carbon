package v2core

import (
	"slices"
	"sync"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// rollupScratch is one seal's planner working set: stages, per-row sample and
// batch storage, lane records and lane part outputs. A planner holds one only
// between planParts and Commit/Abort and returns it to a process-wide pool, so
// retained capacity follows the seals in flight instead of the high-water
// capacity of every owner. Owner seal deadlines are staggered, so few owners
// plan at once. VictoriaMetrics pools its in-memory part buffers the same way.
//
// Per-owner state (metrics, ranks, history window caches) never moves here.
type rollupScratch struct {
	stages         []rollupStage
	batchPositions []int
	samples        []sealrollup.Sample
	batches        []sealrollup.Batch
	files          map[string][][]plannedKV
	coverage       map[string]*rollupCoverage
	arena          []byte
	lanes          []rollupLaneScratch
}

type rollupLaneScratch struct {
	ranked  []rollupRankRef
	spare   []rollupRankRef
	counts  []int
	records []sealrollup.Record
	output  rollupOutput
}

var rollupScratchPool sync.Pool

// takeScratch adopts a pooled working set for the seal that is starting.
func (r *coreRollups) takeScratch() {
	s, _ := rollupScratchPool.Get().(*rollupScratch)
	if s == nil {
		s = &rollupScratch{}
	}
	r.scratch = s
	r.stages, r.batchPositions, r.samples, r.batches = s.stages, s.batchPositions, s.samples, s.batches
	r.files, r.coverage, r.arena = s.files, s.coverage, s.arena
	s.stages, s.batchPositions, s.samples, s.batches = nil, nil, nil, nil
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
	if s == nil {
		s = &rollupScratch{}
	}
	r.scratch = nil
	s.stages, s.batchPositions, s.samples, s.batches = r.stages, r.batchPositions, r.samples, r.batches
	s.files, s.coverage, s.arena = r.files, r.coverage, r.arena
	r.stages, r.batchPositions, r.samples, r.batches = nil, nil, nil, nil
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
	rollupScratchPool.Put(s)
}

// retainedScratchBytes is the planner working-set capacity this planner keeps
// between seals; with pooling it is zero once a seal has committed or aborted.
func (r *coreRollups) retainedScratchBytes() int {
	n := cap(r.stages)*int(unsafe.Sizeof(rollupStage{})) + cap(r.batchPositions)*8 + cap(r.samples)*int(unsafe.Sizeof(sealrollup.Sample{})) + cap(r.batches)*int(unsafe.Sizeof(sealrollup.Batch{})) + cap(r.arena)
	for i := range r.lanes {
		lane := &r.lanes[i]
		n += (cap(lane.ranked)+cap(lane.spare))*int(unsafe.Sizeof(rollupRankRef{})) + cap(lane.counts)*8 + cap(lane.records)*int(unsafe.Sizeof(sealrollup.Record{})) + cap(lane.output.arena)
		for _, file := range lane.output.files {
			n += cap(*file) * int(unsafe.Sizeof(plannedKV{}))
		}
	}
	return n
}
