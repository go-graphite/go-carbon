package downsize

import "sort"

// Report only the terminal read's surviving holes after an accepted fill.
// Failed transports, an interrupted read, and successful repairs are not proof
// of an ineffective repair. Group donor/archive observations by metric so the
// fixed-cardinality progress gauge counts distinct names, not point fan-out.
func (s *syncer) reportRepairFailures() int {
	byName := map[string][]syncResidual{}
	for _, r := range s.sum.residual {
		if r.Reason == "persisted_after_fill" {
			byName[r.Name] = append(byName[r.Name], r)
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		event("sync_repair_failed", map[string]any{"level": "ERROR", "metric": name, "reason": "persisted_after_fill", "round": s.sum.Rounds + s.verificationBase, "target": s.o.to, "holes": byName[name]})
	}
	return len(names)
}
