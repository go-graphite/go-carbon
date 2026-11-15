package downsize

import "strings"

// These are observations of the rejecting response, not an inference from a
// later storage-status sample. Host cardinality is bounded by the copy peers.
type copyBackpressure struct{ Slots, Storage, Unknown int64 }

func (p copyBackpressure) total() int64 { return p.Slots + p.Storage + p.Unknown }
func (p copyBackpressure) since(old copyBackpressure) copyBackpressure {
	return copyBackpressure{p.Slots - old.Slots, p.Storage - old.Storage, p.Unknown - old.Unknown}
}
func (p copyBackpressure) reasons() map[string]int64 {
	out := map[string]int64{}
	for reason, n := range map[string]int64{"transfer_slots": p.Slots, "storage_pressure": p.Storage, "unknown": p.Unknown} {
		if n > 0 {
			out[reason] = n
		}
	}
	return out
}
func (p copyBackpressure) limiter() string {
	var reasons []string
	if p.Slots > 0 {
		reasons = append(reasons, "transfer slots")
	}
	if p.Storage > 0 {
		reasons = append(reasons, "storage pressure")
	}
	if p.Unknown > 0 {
		reasons = append(reasons, "unclassified")
	}
	return "target HTTP 429 backpressure (" + strings.Join(reasons, ", ") + ")"
}
func (m *mover) recordBackpressure(host string, api *apiError) string {
	reason := "unknown"
	message := strings.TrimSpace(api.Message)
	switch {
	case strings.HasSuffix(message, ": HTTP 429 too many concurrent transfers"):
		reason = "transfer_slots"
	case strings.HasSuffix(message, ": HTTP 429 target storage pressure; import paused"), strings.HasSuffix(message, ": HTTP 429 source storage pressure; deletion paused"):
		reason = "storage_pressure"
	}
	m.backpressureMu.Lock()
	defer m.backpressureMu.Unlock()
	if m.backpressureHosts == nil {
		m.backpressureHosts = map[string]copyBackpressure{}
	}
	p := m.backpressureHosts[host]
	switch reason {
	case "transfer_slots":
		p.Slots++
	case "storage_pressure":
		p.Storage++
	default:
		p.Unknown++
	}
	m.backpressureHosts[host] = p
	m.backpressure.Add(1)
	return reason
}
func (m *mover) backpressureSnapshot(target string) (int64, copyBackpressure) {
	m.backpressureMu.Lock()
	defer m.backpressureMu.Unlock()
	return m.backpressure.Load(), m.backpressureHosts[target]
}
