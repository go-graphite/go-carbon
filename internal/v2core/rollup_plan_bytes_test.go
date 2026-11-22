package v2core

import (
	"encoding/binary"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// plan is the test view of the production planner: planParts with every
// planned part expanded into the encoded Files/PartSpecs form that the planner
// returned before the tuple handoff. Existing plan tests and goldens read it.
func (r *coreRollups) plan(in coreSealInput) (coreSealPlan, error) {
	out, err := r.planParts(in)
	if err != nil {
		return out, err
	}
	orderProofPlanned(r, &out)
	out.Files, out.PartSpecs = expandPlannedParts(out.Parts, plannedOwners(r))
	out.Parts = nil
	return out, nil
}

// plannedOwners maps each planned tuple back to its metric through the
// planner's stages: records by address, envelopes by name. The reference key
// builder takes the name and vshard from that metric, not from the tuple, so a
// tuple carrying the wrong name or vshard changes the reference bytes.
func plannedOwners(r *coreRollups) func(plannedKV) *rollupMetric {
	byRecord := map[*sealrollup.Record]*rollupMetric{}
	byName := map[v2part.ID]*rollupMetric{}
	for i := range r.lanes {
		lane := &r.lanes[i]
		for _, ref := range lane.ranked {
			m, records := r.stageMetrics[ref.stage], lane.stageRecords(&r.stages[ref.stage])
			records = records[:cap(records)] // with the superseded versions
			for j := range records {
				byRecord[&records[j]] = m
			}
			byName[m.policy.Name] = m
		}
	}
	return func(kv plannedKV) *rollupMetric {
		if kv.record != nil {
			return byRecord[kv.record]
		}
		return byName[kv.name]
	}
}

// expandPlannedParts is the pre-handoff key builder, kept as the reference
// byte path: one full GCP3 key per record and per envelope.
func expandPlannedParts(parts []plannedRollupPart, owner func(plannedKV) *rollupMetric) ([][]coreKV, []v2part.Spec) {
	var files [][]coreKV
	var specs []v2part.Spec
	for _, part := range parts {
		s := part.Spec.Scope
		prefix, _, err := v2part.GroupRange(s.Group)
		if err != nil {
			panic(err)
		}
		var file []coreKV
		for _, segment := range part.Segments {
			for _, kv := range segment {
				m := owner(kv)
				if m == nil {
					panic("planned tuple without a planner stage")
				}
				archive, slot := rollupMetadataArchive, int64(0)
				if kv.record != nil {
					archive, slot = uint32(kv.record.Archive), kv.record.Slot
				}
				key := append([]byte(nil), prefix...)
				key = binary.BigEndian.AppendUint16(key, m.vshard)
				key = binary.BigEndian.AppendUint64(key, uint64(s.Partition)^(1<<63))
				key = append(key, s.Part[:]...)
				key = binary.BigEndian.AppendUint32(key, uint32(len(m.policy.Name)))
				key = append(key, m.policy.Name...)
				key = binary.BigEndian.AppendUint32(key, archive)
				key = binary.BigEndian.AppendUint64(key, uint64(slot)^(1<<63))
				key = binary.BigEndian.AppendUint32(key, 0)
				file = append(file, coreKV{Key: key, Value: kv.value, Record: kv.record, Name: m.policy.Name})
			}
		}
		files = append(files, file)
		specs = append(specs, part.Spec)
	}
	return files, specs
}
