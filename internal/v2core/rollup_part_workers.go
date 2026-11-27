package v2core

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

type rollupPartResult struct {
	path       string
	descriptor v2part.Descriptor
	err        error
}
type rollupPartWrites struct {
	done    chan struct{}
	results []rollupPartResult
}

// Output workers only create private immutable parts. The owner joins all of
// them before retaining descriptors and atomically ingesting the one frontier.
// Raw encoding/sorting can run concurrently without changing file fanout.
// seq is the seal's number, stamped on every part it writes (Provenance.Seq).
func (o *coreOwner) startRollupPartWrites(plan coreSealPlan, seq uint64) *rollupPartWrites {
	p := &rollupPartWrites{done: make(chan struct{}), results: make([]rollupPartResult, len(plan.Files)+len(plan.Parts))}
	if o.cfg.BlindBatch && !o.cfg.Rollups {
		close(p.done)
		return p
	}
	jobs := make([]int, 0, len(plan.Files))
	for i, file := range plan.Files {
		if isRollupFile(file) {
			p.results[i].path = filepath.Join(o.dir, fmt.Sprintf("stage-%020d-extra-%03d.sst", o.meta.NextPart, i))
			jobs = append(jobs, i)
		}
	}
	for i := range plan.Parts {
		p.results[len(plan.Files)+i].path = filepath.Join(o.dir, fmt.Sprintf("stage-%020d-extra-%03d.sst", o.meta.NextPart, len(plan.Files)+i))
		jobs = append(jobs, len(plan.Files)+i)
	}
	if len(jobs) == 0 {
		close(p.done)
		return p
	}
	go func() {
		var wg sync.WaitGroup
		for worker := 0; worker < min(8, len(jobs)); worker++ {
			wg.Go(func() {
				for j := worker; j < len(jobs); j += min(8, len(jobs)) {
					i := jobs[j]
					r := &p.results[i]
					switch {
					case i >= len(plan.Files):
						part := plan.Parts[i-len(plan.Files)]
						part.Spec.Source.Incarnation = o.partSource.Incarnation
						part.Spec.Source.Stream = o.partSource.Stream
						part.Spec.Source.Seq = seq
						r.descriptor, r.err = writePlannedRollupTuples(r.path, part)
					case len(plan.PartSpecs) == len(plan.Files):
						spec := plan.PartSpecs[i]
						spec.Source.Incarnation = o.partSource.Incarnation
						spec.Source.Stream = o.partSource.Stream
						spec.Source.Seq = seq
						r.descriptor, r.err = writePlannedRollupPart(r.path, plan.Files[i], spec)
					default:
						source := o.partSource
						source.Seq = seq
						r.descriptor, r.err = WriteDenseRollupPart(r.path, plan.Files[i], source)
					}
				}
			})
		}
		wg.Wait()
		close(p.done)
	}()
	return p
}
func (p *rollupPartWrites) cleanup() {
	<-p.done
	for _, r := range p.results {
		if r.path != "" {
			_ = os.Remove(r.path)
		}
	}
}
