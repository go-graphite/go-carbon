package v2core

import (
	"encoding/binary"

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
	out.Files, out.PartSpecs = expandPlannedParts(out.Parts)
	out.Parts = nil
	return out, nil
}

// expandPlannedParts is the pre-handoff key builder, kept as the reference
// byte path: one full GCP3 key per record and per envelope.
func expandPlannedParts(parts []plannedRollupPart) ([][]coreKV, []v2part.Spec) {
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
				m := kv.metric
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
