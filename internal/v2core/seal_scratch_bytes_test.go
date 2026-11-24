package v2core

import (
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// sealScratchBytes is the transient memory a held plan uses, by element
// accounting over the planner's scratch: total bytes and the part in element
// types with pointers, which the GC must scan.
func sealScratchBytes(r *coreRollups) (total, pointerful int) {
	add := func(n int, size uintptr, ptr bool) {
		b := n * int(size)
		total += b
		if ptr {
			pointerful += b
		}
	}
	add(len(r.stages), unsafe.Sizeof(rollupStage{}), false)
	add(len(r.stageMetrics), unsafe.Sizeof((*PolicyIdentity)(nil)), true)
	add(len(r.batches), unsafe.Sizeof(sealrollup.Batch{}), true)
	add(len(r.samples), unsafe.Sizeof(sealrollup.Sample{}), false)
	add(len(r.sweepKeys)+len(r.seen), 8, false)
	for i := range r.lanes {
		l := &r.lanes[i]
		add(len(l.ranked), unsafe.Sizeof(rollupRankRef{}), false)
		add(len(l.records), unsafe.Sizeof(sealrollup.Record{}), false)
		for _, c := range l.chunks {
			add(len(c), unsafe.Sizeof(sealrollup.Record{}), false)
		}
		for _, f := range l.output.files {
			add(len(*f), unsafe.Sizeof(plannedKV{}), true)
		}
		add(len(l.output.arena), 1, false)
	}
	return total, pointerful
}
