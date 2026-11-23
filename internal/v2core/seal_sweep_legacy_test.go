package v2core

import (
	"crypto/rand"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// legacySealPlanner is the planner before the seal sweep, kept test-only as
// the differential oracle: a per-seal positions map from batch ID to stage
// and a second pass placing each batch by position. The sweep must match it
// exactly on every seal.
func legacySealPlanner(r *coreRollups, in coreSealInput) (coreSealPlan, error) {
	return r.planPartsLegacy(in)
}

func (r *coreRollups) planPartsLegacy(in coreSealInput) (coreSealPlan, error) {
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
	stages, metrics := r.stages[:0], r.stageMetrics[:0]
	if len(r.unranked) > 0 && len(r.unranked)*rankSlack >= len(r.dense) {
		r.renumber()
	}
	positions := map[uint64]int{}
	// At most one stage per input application. Retain capacity between seals.
	stages = slices.Grow(stages, len(in.Batches))
	r.stages = stages
	batchPositions := make([]int, len(in.Batches))
	batchCounts := make([]int, 0, len(in.Batches))
	for bi, b := range in.Batches {
		i, ok := positions[b.ID]
		if !ok {
			m := r.metrics[b.ID]
			fresh := m == nil
			if fresh {
				if h := b.Registered; h != nil && h.catalog == r.identities && h.id == b.ID {
					m = newRollupMetric(h)
				} else {
					policy := b.Policy
					if h != nil {
						policy = h.definitionBytes()
					}
					var err error
					m, err = r.metric(b.ID, policy)
					if err != nil {
						return coreSealPlan{}, err
					}
				}
				fp := m.fingerprint()
				if _, ok := r.names.Get(fp); ok {
					return coreSealPlan{}, errors.New("canonical name has multiple producer IDs")
				}
				if _, ok := r.pendingNames.Get(fp); ok {
					return coreSealPlan{}, errors.New("canonical name has multiple pending IDs")
				}
				r.pendingNames.Put(fp, uint32(len(stages)))
			}
			slot, meta := uint32(0), freshMetadata
			if !fresh {
				slot = r.slotOf(b.ID, m)
				meta = r.meta[slot]
			}
			stages, metrics = append(stages, rollupStage{next: meta, slot: slot, fresh: fresh}), append(metrics, m)
			batchCounts = append(batchCounts, 0)
			i = len(stages) - 1
			stages[i].next.Expired = false
			r.stageKey(&stages[i], m, nil)
			positions[b.ID] = i
		}
		batchPositions[bi] = i
		m := metrics[i]
		if (b.Registered == nil || b.Registered != m.PolicyIdentity) && !m.definitionEqual(b.Policy) {
			return coreSealPlan{}, errors.New("stored rollup policy changed")
		}
		batchCounts[i]++
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
		stages[i].batch = uint32(offset)
		offset += batchCounts[i]
	}
	offset = 0
	for bi, b := range in.Batches {
		s := &stages[batchPositions[bi]]
		ps := samples[offset : offset+len(b.Points) : offset+len(b.Points)]
		for j, p := range b.Points {
			ps[j] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		offset += len(b.Points)
		batchStorage[s.batch+s.batches] = sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: ps}
		s.batches++
	}
	named := len(stages)
	// Idle closure uses the existing seal maintenance pass; no separate scheduler.
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if r.expire != nil {
		for id, m := range r.metrics { // the full walk: single-archive names are never due
			if _, seen := positions[id]; seen {
				continue
			}
			slot := r.slotOf(id, m)
			if meta := r.meta[slot]; !meta.Expired && (r.expire != nil && r.expire(m.rollupPolicy(), m.metadata(meta), now)) {
				stages, metrics = append(stages, rollupStage{next: meta, slot: slot}), append(metrics, m)
				r.stageKey(&stages[len(stages)-1], m, nil)
			}
		}
	}
	// Expiry callbacks retain their serial owner contract. Idle plans do not
	// advance order, so the persisted reset frontier is already known here.
	for i := range stages {
		s, m := &stages[i], metrics[i]
		if r.expire != nil && s.batches == 0 && r.expire(m.rollupPolicy(), m.PolicyIdentity.metadata(s.next), now) {
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
	r.stages, r.stageMetrics = stages, metrics
	out, err := r.planStages(part)
	if err != nil {
		return coreSealPlan{}, err
	}
	out.Names = named
	out.Abort = r.release
	out.Commit = r.commitPlan
	returned = true
	return out, nil
}
