package carbonserver

import "github.com/go-graphite/go-carbon/internal/hostcapacity"

// SetCapacityReadLatency connects application-owned read pressure before Start.
// The persister can start before this listener; both share the same histogram.
func (listener *CarbonserverListener) SetCapacityReadLatency(reads *hostcapacity.Latency) {
	listener.capacityReadLatency = reads
}
